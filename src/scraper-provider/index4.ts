import { connect } from "puppeteer-real-browser";
import { Browser, Page, BrowserContext } from "puppeteer";
import PreprocessService from "./preprocessService";

export enum SearchEngine {
  GOOGLE = "https://google.com",
  BING = "https://bing.com",
  DUCKDUCKGO = "https://duckduckgo.com",
  YAHOO = "https://search.yahoo.com",
}

export interface SearchResult {
  domain: string;
  title: string;
  link: string;
  description: string;
  rank: number;
}

interface SearchTask {
  query: string;
  searchEngine: SearchEngine;
  resolve: (results: SearchResult[]) => void;
  reject: (error: Error) => void;
  id: string;
  abortController: AbortController;
  cancelled: boolean;
  retries: number;
  priority: number;
  createdAt: number;
}

interface TabPool {
  page: Page;
  busy: boolean;
  lastUsed: number;
  contextType: "default" | "google";
}

interface BandwidthMetrics {
  totalBytes: number;
  requestCount: number;
  responseCount: number;
  blockedRequests: number;
  startTime: number;
  endTime?: number;
}

// Priority levels for task scheduling
enum TaskPriority {
  NORMAL = 0,
  GOOGLE_TASK = 1, // Priority for Google tasks after captcha
}

const isTracker =
  /(beacon|track|analytics|pixel|gtm|tagmanager|doubleclick|gstatic|xjs|ads|collect|log|telemetry|stats|metrics|cdn)/i;
const isNonEssential = /(image|media|font|stylesheet)/i;
const MAX_RETRIES = 2;
const CAPTCHA_COOLDOWN = 3000; // 3 second cooldown after captcha detection

export default class SERPScraper extends PreprocessService {
  private browser: Browser | null = null;
  private tabPool: TabPool[] = [];
  private taskQueue: SearchTask[] = [];
  private maxTabs: number;
  private maxQueueSize: number;
  private processingQueue: boolean = false;
  private tabIdleTimeout: number = 60000;
  public ready: boolean = false;
  private googleContext: BrowserContext | null = null;
  private googleContextResetting: boolean = false;
  private browserInitializing: boolean = false; // Lock to prevent multiple browser initializations
  private initializing: Promise<void> | null = null; // Promise for initialization
  private cleanUpIntervalId: NodeJS.Timeout | null = null;
  private pendingGoogleTasks: SearchTask[] = [];
  private lastCaptchaDetection: number = 0;
  private browserHealthy: boolean = false;

  constructor(maxTabs: number = 1000, maxQueueSize: number = 1000) {
    super();
    this.maxTabs = Math.max(1, maxTabs);
    this.maxQueueSize = maxQueueSize;
    this.initializing = this.launchBrowser();
  }

  async launchBrowser(): Promise<void> {
    // Critical section - prevent multiple browser launches
    if (this.browserInitializing) {
      console.log(
        "Browser initialization already in progress, returning existing promise",
      );
      return this.initializing || Promise.resolve();
    }

    this.browserInitializing = true;
    this.ready = false;

    try {
      // First, make sure to clean up any existing browser
      if (this.browser) {
        try {
          console.log("Closing existing browser before creating a new one");
          await this.closeBrowser();
        } catch (error) {
          console.error("Error closing existing browser:", error);
        }
      }

      console.log("Launching new browser...");
      const { browser, page } = await connect({
        headless: false,
        args: [
          "--no-sandbox",
          "--disable-setuid-sandbox",
          "--disable-blink-features=AutomationControlled",
          "--disable-dev-shm-usage",
          "--no-zygote",
          "--disable-software-rasterizer",
          "--disable-extensions",
          "--disable-background-networking",
          "--disable-renderer-backgrounding",
          "--memory-pressure-off",
          "--disable-notifications",
        ],
        turnstile: true,
        proxy: {
          host: process.env.PROXY_HOST || "",
          port: parseInt(process.env.PROXY_PORT || "0", 10),
        },
        disableXvfb: process.env.NODE_ENV === "development",
        ignoreAllFlags: false,
        plugins: [],
        connectOption: {
          defaultViewport: {
            height: 667,
            width: 375,
          },
        },
      });

      this.browser = browser as unknown as Browser;
      this.browserHealthy = true;
      this.tabPool = []; // Reset tab pool when creating a new browser

      try {
        await page.goto("https://www.google.com");
        await this.setupAggressiveRequestBlocking(page);

        // Setup initial tab
        this.tabPool.push({
          page,
          busy: false,
          lastUsed: Date.now(),
          contextType: "default",
        });

        // Create Google context and run the test
        await this.createGoogleContext();

        // Run the initial Minecraft search test
        await this.runGoogleContextTest();

        this.startIdleTabCleanup();
        console.log("Browser fully initialized and ready");
      } catch (error) {
        console.error("Error during browser initialization:", error);
        this.browserHealthy = false;

        // Clean up and retry
        try {
          await this.closeBrowser();
        } catch (closeError) {
          console.error("Error closing browser after failed init:", closeError);
        }

        // Schedule a retry
        setTimeout(() => this.launchBrowser(), 5000);
      }
    } catch (error) {
      console.error("Error launching browser:", error);
      this.browserHealthy = false;
    } finally {
      this.browserInitializing = false;
      this.initializing = null;
    }
  }

  private async createGoogleContext(): Promise<BrowserContext | null> {
    if (!this.browser || !this.browserHealthy) {
      console.log("Browser not healthy, cannot create Google context");
      return null;
    }

    try {
      // Safely create a new browser context
      console.log("Creating new Google context...");
      const context = await this.browser.createBrowserContext();
      this.googleContext = context;
      console.log("Created new Google context");

      // Create an initial page in this context to keep it alive
      const page = await this.googleContext.newPage();
      await this.setupAggressiveRequestBlocking(page);
      await page.goto("about:blank");

      // Add it to the tab pool
      this.tabPool.push({
        page,
        busy: false,
        lastUsed: Date.now(),
        contextType: "google",
      });

      return context;
    } catch (error) {
      console.error("Failed to create Google context:", error);
      return null;
    }
  }

  private async ensureGoogleContext(
    forceCreate: boolean = false,
  ): Promise<BrowserContext | null> {
    // If resetting, wait for it to complete
    if (this.googleContextResetting) {
      console.log("Google context is being reset, waiting...");
      await new Promise<void>((resolve) => {
        const checkReset = () => {
          if (!this.googleContextResetting) {
            resolve();
          } else {
            setTimeout(checkReset, 200);
          }
        };
        checkReset();
      });
    }

    // Check if existing context is valid
    if (this.googleContext && !forceCreate) {
      try {
        // Test if context is still valid
        await this.googleContext.pages();
        return this.googleContext;
      } catch (error) {
        console.log("Google context is no longer valid, recreating...");
        this.googleContext = null;
      }
    }

    // Create a new context if needed
    if (!this.googleContext || forceCreate) {
      return this.createGoogleContext();
    }

    return this.googleContext;
  }

  private async runGoogleContextTest() {
    try {
      console.log("Running Google context test (Minecraft search)...");

      // Create a test page in the Google context
      if (!this.googleContext) {
        throw new Error("Google context not available for test");
      }

      const testPage = await this.googleContext.newPage();
      await this.setupAggressiveRequestBlocking(testPage);

      // Generate search URL
      const query = "Minecraft";
      const url = await this.urlQueryProvider(query, SearchEngine.GOOGLE);

      // Navigate directly without using search method to avoid circular dependency
      await testPage.goto(url, { waitUntil: "domcontentloaded" });

      // Process the results manually
      const results = await this.preprocessPageResult(
        testPage,
        SearchEngine.GOOGLE,
      );
      console.log(
        `Google context test successful, found ${results.length} results`,
      );

      // Clean up test page
      await testPage.close();

      this.ready = true;
      return results;
    } catch (error) {
      console.error("Google context test failed:", error);
      this.ready = false;
      throw error;
    }
  }

  private async resetGoogleContext(): Promise<void> {
    // Prevent multiple simultaneous resets
    if (this.googleContextResetting) {
      console.log("Google context reset already in progress");
      return;
    }

    this.googleContextResetting = true;
    console.log("Starting Google context reset due to captcha detection");
    this.lastCaptchaDetection = Date.now();
    this.ready = false; // Mark as not ready during reset

    try {
      // Move Google tasks to pending queue
      const googleTasksInQueue = this.taskQueue.filter(
        (task) => task.searchEngine === SearchEngine.GOOGLE && !task.cancelled,
      );

      // Remove Google tasks from main queue
      this.taskQueue = this.taskQueue.filter(
        (task) => task.searchEngine !== SearchEngine.GOOGLE || task.cancelled,
      );

      // Store pending tasks with higher priority
      this.pendingGoogleTasks.push(
        ...googleTasksInQueue.map((task) => ({
          ...task,
          priority: TaskPriority.GOOGLE_TASK,
          retries: 0, // Reset retry count
        })),
      );

      // Carefully close Google tabs
      const googleTabs = this.tabPool.filter(
        (tab) => tab.contextType === "google",
      );

      for (const tab of googleTabs) {
        try {
          if (!tab.page.isClosed()) {
            await tab.page.close();
          }
        } catch (error) {
          console.error("Error closing Google tab:", error);
        }
      }

      // Remove all Google tabs from pool
      this.tabPool = this.tabPool.filter((tab) => tab.contextType !== "google");

      // Close the Google context
      if (this.googleContext) {
        try {
          await this.googleContext.close();
        } catch (error) {
          console.error("Error closing Google context:", error);
        }
        this.googleContext = null;
      }

      // Wait for cooldown period
      await new Promise((resolve) => setTimeout(resolve, CAPTCHA_COOLDOWN));

      // Create a new Google context
      await this.createGoogleContext();

      // Run the test search on the new context
      await this.runGoogleContextTest();

      // Move pending tasks back to main queue
      const tasksToRequeue = [...this.pendingGoogleTasks];
      this.pendingGoogleTasks = [];

      console.log(
        `Google context reset complete. Requeuing ${tasksToRequeue.length} Google tasks.`,
      );

      // Add tasks back to queue - at the beginning for priority
      this.taskQueue.unshift(
        ...tasksToRequeue.filter((task) => !task.cancelled),
      );

      if (tasksToRequeue.length > 0) {
        setImmediate(() => this.processQueue());
      }
    } catch (error) {
      console.error("Error during Google context reset:", error);
      this.browserHealthy = false;

      // Keep pending tasks for later
      if (this.pendingGoogleTasks.length > 0) {
        console.log(
          `Keeping ${this.pendingGoogleTasks.length} tasks pending due to reset failure`,
        );
      }

      // Try to recover the browser
      await this.recoveryCheck();
    } finally {
      this.googleContextResetting = false;
    }
  }

  private async recoveryCheck(): Promise<boolean> {
    if (!this.browserHealthy) {
      console.log("Browser appears unhealthy, attempting recovery");
      try {
        // Try to create a simple page as a test
        if (this.browser) {
          const testPage = await this.browser.newPage();
          await testPage.goto("about:blank");
          await testPage.evaluate(() => document.title);
          await testPage.close();

          // If we get here, browser is responsive
          this.browserHealthy = true;
          console.log("Browser recovery successful");

          // Try to recreate Google context
          await this.createGoogleContext();
          await this.runGoogleContextTest();
          return true;
        } else {
          // Browser is null, we need a full restart
          console.log("Browser is null, attempting full restart");
          this.initializing = this.launchBrowser();
          await this.initializing;
          return this.browserHealthy;
        }
      } catch (error) {
        console.error("Browser recovery failed:", error);
        // Schedule a full restart
        console.log("Scheduling browser restart in 5 seconds");
        setTimeout(() => {
          this.closeBrowser().then(() => {
            this.initializing = this.launchBrowser();
          });
        }, 5000);
        return false;
      }
    }
    return true;
  }

  private async getAvailableTab(searchEngine: SearchEngine): Promise<TabPool> {
    const useGoogleContext = searchEngine === SearchEngine.GOOGLE;
    const contextType = useGoogleContext ? "google" : "default";

    // Make sure browser is ready
    if (!this.browserHealthy || this.browserInitializing) {
      console.log("Waiting for browser to be healthy...");
      if (this.initializing) {
        await this.initializing;
      } else {
        await new Promise<void>((resolve) => {
          const checkHealth = () => {
            if (this.browserHealthy && !this.browserInitializing) {
              resolve();
            } else {
              setTimeout(checkHealth, 500);
            }
          };
          checkHealth();
        });
      }
    }

    // If Google context is being reset and this is a Google search, wait
    if (useGoogleContext && this.googleContextResetting) {
      console.log("Waiting for Google context reset to complete");
      await new Promise<void>((resolve) => {
        const checkReset = () => {
          if (!this.googleContextResetting) {
            resolve();
          } else {
            setTimeout(checkReset, 200);
          }
        };
        checkReset();
      });
    }

    // For Google searches, ensure context is available
    if (useGoogleContext) {
      const context = await this.ensureGoogleContext();
      if (!context) {
        if (this.googleContextResetting) {
          throw new Error("Google context is being reset");
        } else {
          throw new Error("Google context not available");
        }
      }
    }

    // Find tabs of the right context type
    const currentTabsOfType = this.tabPool.filter(
      (tab) => tab.contextType === contextType,
    );

    // Find available tab
    const availableTab = currentTabsOfType.find((tab) => !tab.busy);
    if (availableTab) {
      return availableTab;
    }

    // Calculate max tabs per context to avoid overloading one context
    const maxTabsForContext = useGoogleContext
      ? Math.max(1, Math.floor(this.maxTabs / 2))
      : Math.max(1, this.maxTabs - Math.floor(this.maxTabs / 2));

    // If we've reached the max tabs for this context, wait for one to become available
    if (currentTabsOfType.length >= maxTabsForContext) {
      console.log(
        `Max tabs (${maxTabsForContext}) for ${contextType} reached, waiting for available tab`,
      );
      return new Promise((resolve) => {
        const checkAvailability = () => {
          const tab = this.tabPool.find(
            (t) => !t.busy && t.contextType === contextType,
          );
          if (tab) {
            resolve(tab);
          } else {
            setTimeout(checkAvailability, 100);
          }
        };
        checkAvailability();
      });
    }

    // Create a new tab
    try {
      let page: Page;

      if (useGoogleContext) {
        const context = await this.ensureGoogleContext();
        if (!context) {
          throw new Error("Failed to get Google context");
        }
        page = await context.newPage();
      } else {
        if (!this.browser) {
          throw new Error("Browser not initialized");
        }
        page = await this.browser.newPage();
      }

      // Set up aggressive request blocking to save bandwidth
      await this.setupAggressiveRequestBlocking(page);

      // Add the new tab to the pool
      const newTab: TabPool = {
        page,
        busy: false,
        lastUsed: Date.now(),
        contextType,
      };

      this.tabPool.push(newTab);
      console.log(
        `Created new ${contextType} tab. Total tabs: ${this.tabPool.length}`,
      );

      return newTab;
    } catch (error) {
      console.error(`Error creating new ${contextType} tab:`, error);

      // If creating a Google tab fails, it might be a context issue
      if (useGoogleContext) {
        this.browserHealthy = false;
        await this.recoveryCheck();
      }

      throw error;
    }
  }

  private startIdleTabCleanup() {
    // Clear any existing interval
    if (this.cleanUpIntervalId) {
      clearInterval(this.cleanUpIntervalId);
    }

    this.cleanUpIntervalId = setInterval(async () => {
      if (!this.browserHealthy || this.browserInitializing) return;

      const now = Date.now();

      // Get tabs by context type
      const defaultTabs = this.tabPool.filter(
        (tab) => tab.contextType === "default",
      );
      const idleDefaultTabs = defaultTabs.filter(
        (tab) => !tab.busy && now - tab.lastUsed > this.tabIdleTimeout,
      );

      const googleTabs = this.tabPool.filter(
        (tab) => tab.contextType === "google",
      );
      const idleGoogleTabs = googleTabs.filter(
        (tab) => !tab.busy && now - tab.lastUsed > this.tabIdleTimeout,
      );

      // For default tabs, keep at least one
      if (defaultTabs.length > 1 && idleDefaultTabs.length > 0) {
        // Always leave at least one tab
        const tabsToClose = idleDefaultTabs.slice(
          0,
          idleDefaultTabs.length - 1,
        );
        await this.closeTabs(tabsToClose);
      }

      // For Google tabs, always keep at least one to maintain context
      if (googleTabs.length > 1 && idleGoogleTabs.length > 0) {
        // Always leave at least one Google tab
        const tabsToClose = idleGoogleTabs.slice(0, idleGoogleTabs.length - 1);
        await this.closeTabs(tabsToClose);
      }
    }, 60000); // Run every minute
  }

  private async closeTabs(tabs: TabPool[]) {
    for (const tab of tabs) {
      try {
        if (!tab.page.isClosed()) {
          await tab.page.close();
        }
        this.tabPool = this.tabPool.filter((t) => t !== tab);
        console.log(`Closed tab. Pool size: ${this.tabPool.length}`);
      } catch (error) {
        console.error("Error closing tab:", error);
        // Remove from pool even if close fails
        this.tabPool = this.tabPool.filter((t) => t !== tab);
      }
    }
  }

  // Improved aggressive request blocking to save bandwidth
  private async setupAggressiveRequestBlocking(page: Page): Promise<void> {
    try {
      await page.setRequestInterception(true);

      page.on("request", (request) => {
        const resourceType = request.resourceType();
        const url = request.url();

        // Always allow the main document
        if (resourceType === "document") {
          request.continue();
          return;
        }

        // Block known trackers and ad-related resources
        if (isTracker.test(url)) {
          request.abort();
          return;
        }

        // Block non-essential resources to save bandwidth
        if (isNonEssential.test(resourceType)) {
          request.abort();
          return;
        }

        // Allow essential scripts for the page to function
        if (resourceType === "script") {
          // Block large script files from Google that aren't essential
          if (
            url.includes("google") &&
            (url.includes("gstatic") ||
              url.includes("apis.") ||
              url.includes("adsbygoogle"))
          ) {
            request.abort();
            return;
          }

          request.continue();
          return;
        }

        // Allow XHR/fetch requests for search results
        if (resourceType === "xhr" || resourceType === "fetch") {
          request.continue();
          return;
        }

        // Block everything else
        request.abort();
      });
    } catch (error) {
      console.error("Error setting up request interception:", error);
      // If setting up interception fails, continue without it
    }
  }

  private setupBandwidthTracking(page: Page): BandwidthMetrics {
    const metrics: BandwidthMetrics = {
      totalBytes: 0,
      requestCount: 0,
      responseCount: 0,
      blockedRequests: 0,
      startTime: Date.now(),
    };

    page.on("requestfailed", (request) => {
      const resourceType = request.resourceType();
      if (isNonEssential.test(resourceType) || isTracker.test(request.url())) {
        metrics.blockedRequests++;
      }
    });

    page.on("request", (request) => {
      const resourceType = request.resourceType();
      if (!isNonEssential.test(resourceType)) {
        metrics.requestCount++;
        const postData = request.postData();
        if (postData) {
          metrics.totalBytes += Buffer.byteLength(postData, "utf8");
        }
      }
    });

    page.on("response", async (response) => {
      const request = response.request();
      const resourceType = request.resourceType();
      if (!isNonEssential.test(resourceType)) {
        metrics.responseCount++;
        try {
          const contentLength = response.headers()["content-length"];
          if (contentLength) {
            metrics.totalBytes += parseInt(contentLength, 10);
          } else {
            try {
              const buffer = await response.buffer();
              metrics.totalBytes += buffer.length;
            } catch {
              if (response.ok()) {
                metrics.totalBytes += 1024;
              }
            }
          }
        } catch (error) {}
      }
    });

    return metrics;
  }

  private logBandwidthMetrics(
    taskId: string,
    query: string,
    searchEngine: SearchEngine,
    metrics: BandwidthMetrics,
  ) {
    metrics.endTime = Date.now();
    const duration = metrics.endTime - metrics.startTime;
    const totalKB = (metrics.totalBytes / 1024).toFixed(2);
    const totalMB = (metrics.totalBytes / (1024 * 1024)).toFixed(2);

    console.log(`Search Bandwidth Report - Task ${taskId}`);
    console.log(`   Query: "${query}"`);
    console.log(`   Engine: ${searchEngine}`);
    console.log(`   Duration: ${duration}ms`);
    console.log(`   Total Bandwidth: ${totalKB} KB (${totalMB} MB)`);
    console.log(`   Total Bytes: ${metrics.totalBytes.toLocaleString()} bytes`);
    console.log(`   Requests: ${metrics.requestCount}`);
    console.log(`   Responses: ${metrics.responseCount}`);
    console.log(`   Blocked Requests: ${metrics.blockedRequests}`);
    console.log(
      `   Avg per request: ${(metrics.totalBytes / Math.max(metrics.responseCount, 1) / 1024).toFixed(2)} KB`,
    );
    console.log(`   ────────────────────────────────────────────────────`);
  }

  private async processQueue() {
    if (this.processingQueue || this.taskQueue.length === 0) return;
    this.processingQueue = true;

    try {
      while (this.taskQueue.length > 0) {
        // If browser is initializing, wait
        if (this.browserInitializing || !this.browserHealthy) {
          console.log(
            "Waiting for browser to be ready before processing queue",
          );
          break;
        }

        // Sort by priority (simple - higher number = higher priority)
        this.taskQueue.sort((a, b) => b.priority - a.priority);

        // Remove cancelled tasks
        const validTasks = this.taskQueue.filter((task) => !task.cancelled);
        const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

        cancelledTasks.forEach((task) => {
          task.reject(new Error("Request cancelled"));
        });

        this.taskQueue = validTasks;
        if (this.taskQueue.length === 0) break;

        // Get next task
        const task = this.taskQueue.shift()!;
        if (task.cancelled) {
          task.reject(new Error("Request cancelled"));
          continue;
        }

        try {
          // Skip Google tasks if context is resetting
          if (
            task.searchEngine === SearchEngine.GOOGLE &&
            this.googleContextResetting
          ) {
            // Put task back and wait for context reset
            this.taskQueue.unshift(task);
            break;
          }

          // Get tab for task
          const tab = await this.getAvailableTab(task.searchEngine);
          if (task.cancelled) {
            task.reject(new Error("Request cancelled"));
            continue;
          }

          // Mark tab as busy
          tab.busy = true;
          tab.lastUsed = Date.now();

          console.log(
            `Processing task ${task.id}. Queue: ${this.taskQueue.length}`,
          );

          // Execute search
          this.executeSearch(tab, task).finally(() => {
            tab.busy = false;
            tab.lastUsed = Date.now();
          });
        } catch (error) {
          console.error(`Error processing task ${task.id}:`, error);

          if (!task.cancelled) {
            if (
              task.searchEngine === SearchEngine.GOOGLE &&
              (error.message?.includes("context") ||
                error.message?.includes("Protocol error"))
            ) {
              // Context/connection error - add to pending for reset
              console.log(
                `Adding task ${task.id} back to queue due to connection error`,
              );

              // Add back to queue with retries
              if (task.retries < MAX_RETRIES) {
                task.retries++;
                this.taskQueue.push(task);
              } else {
                task.reject(new Error(`Max retries reached: ${error.message}`));
              }

              // Check browser health
              this.browserHealthy = false;
              await this.recoveryCheck();
            } else {
              // Other error - just reject the task
              task.reject(error as Error);
            }
          }
        }
      }
    } catch (error) {
      console.error("Error in queue processing:", error);
      this.browserHealthy = false;
      await this.recoveryCheck();
    } finally {
      this.processingQueue = false;

      // If there are still tasks and the browser is healthy, continue processing
      if (
        this.taskQueue.length > 0 &&
        this.browserHealthy &&
        !this.browserInitializing
      ) {
        setImmediate(() => this.processQueue());
      }
    }
  }

  private async executeSearch(tab: TabPool, task: SearchTask) {
    let bandwidthMetrics: BandwidthMetrics | null = null;
    let captchaDetected = false;

    try {
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      // Track bandwidth if enabled
      if (process.env.ENABLE_BANDWIDTH_LOGGING === "true") {
        bandwidthMetrics = this.setupBandwidthTracking(tab.page);
      }

      // Get search URL
      const url = await this.urlQueryProvider(task.query, task.searchEngine);

      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      // Set page timeout
      tab.page.setDefaultNavigationTimeout(
        task.searchEngine === SearchEngine.GOOGLE ? 60000 : 30000,
      );

      // Navigate to search URL
      await Promise.race([
        tab.page.goto(url, { waitUntil: "domcontentloaded" }), // Changed to domcontentloaded to reduce bandwidth
        new Promise((_, reject) => {
          task.abortController.signal.addEventListener("abort", () => {
            reject(new Error("Request cancelled"));
          });
        }),
      ]);

      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      // Check for captcha
      captchaDetected = await this.detectCaptcha(tab.page);
      if (captchaDetected && task.searchEngine === SearchEngine.GOOGLE) {
        console.log(
          `Captcha detected in task ${task.id} - handling context reset`,
        );

        // Add this task to pending for reset
        if (!task.cancelled) {
          this.pendingGoogleTasks.push({
            ...task,
            priority: TaskPriority.GOOGLE_TASK,
            retries: 0, // Reset retries for captcha-induced reset
          });
        }

        // Trigger reset if not already resetting
        if (!this.googleContextResetting) {
          setImmediate(() => this.resetGoogleContext());
        }

        // Return early - task will be reprocessed after reset
        return;
      }

      // Process search results
      const results = await this.preprocessPageResult(
        tab.page,
        task.searchEngine,
      );

      // Clear page to reduce memory usage
      try {
        await tab.page.goto("about:blank");
      } catch (error) {
        console.warn("Error clearing page:", error);
      }

      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      // Log bandwidth metrics
      if (bandwidthMetrics) {
        this.logBandwidthMetrics(
          task.id,
          task.query,
          task.searchEngine,
          bandwidthMetrics,
        );
      }

      // Resolve task with results
      task.resolve(results);
      console.log(`Task ${task.id} completed successfully`);
    } catch (error) {
      // Log bandwidth metrics even on error
      if (bandwidthMetrics) {
        this.logBandwidthMetrics(
          task.id,
          task.query,
          task.searchEngine,
          bandwidthMetrics,
        );
      }

      // Don't handle captcha errors here - already handled above
      if (captchaDetected && task.searchEngine === SearchEngine.GOOGLE) {
        return; // Task already added to pending queue
      }

      // For connection errors, add back to the queue
      if (
        error.message?.includes("Protocol error") ||
        error.message?.includes("Target closed") ||
        error.message?.includes("Connection closed")
      ) {
        console.log(
          `Connection error in task ${task.id}, adding back to queue`,
        );

        if (!task.cancelled && task.retries < MAX_RETRIES) {
          task.retries++;
          this.taskQueue.push(task);

          // Signal possible browser issue
          this.browserHealthy = false;
          setTimeout(() => this.recoveryCheck(), 1000);
          return;
        }
      }

      // For other errors, reject the task
      if (!task.cancelled) {
        console.error(`Task ${task.id} failed:`, error.message);
        task.reject(new Error(`Search failed: ${error.message}`));
      }
    }
  }

  private async detectCaptcha(page: Page): Promise<boolean> {
    try {
      const captchaSelectors = [
        "#captcha",
        ".g-recaptcha",
        'iframe[src*="recaptcha"]',
        'div[class*="captcha"]',
        'div[class*="Captcha"]',
        'form[action*="sorry"]',
      ];

      for (const selector of captchaSelectors) {
        if (await page.$(selector)) return true;
      }

      return await page.evaluate(() => {
        const text = document.body.innerText.toLowerCase();
        return /captcha|verify you|not a robot|security check|unusual traffic/i.test(
          text,
        );
      });
    } catch (error) {
      console.error("Captcha detection error:", error);
      return false;
    }
  }

  async search(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
    // If browser not ready, wait for it
    if (!this.browser || !this.browserHealthy || this.browserInitializing) {
      console.log("Browser not ready, waiting for initialization...");
      if (this.initializing) {
        await this.initializing;
      } else {
        await this.launchBrowser();
      }
    }

    if (this.taskQueue.length >= this.maxQueueSize) {
      throw new Error(
        `Queue is full. Maximum queue size: ${this.maxQueueSize}`,
      );
    }

    const abortController = new AbortController();
    const taskId = `task_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const priority =
      searchEngine === SearchEngine.GOOGLE
        ? TaskPriority.NORMAL
        : TaskPriority.NORMAL;

    const searchPromise = new Promise<SearchResult[]>((resolve, reject) => {
      const task: SearchTask = {
        query,
        searchEngine,
        resolve,
        reject,
        id: taskId,
        abortController,
        cancelled: false,
        retries: 0,
        priority,
        createdAt: Date.now(),
      };

      this.taskQueue.push(task);
      console.log(
        `Added task ${taskId} to queue. Queue: ${this.taskQueue.length}`,
      );
      this.processQueue();
    });

    const cancel = () => {
      const task = this.taskQueue.find((t) => t.id === taskId);
      if (task) {
        task.cancelled = true;
        task.abortController.abort();
        this.taskQueue = this.taskQueue.filter((t) => t !== task);
        console.log(`Cancelled task ${taskId}`);
      }
    };

    return { promise: searchPromise, cancel };
  }

  async searchSimple(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<SearchResult[]> {
    const { promise } = await this.search(query, searchEngine);
    return promise;
  }

  cancelAllRequests() {
    const cancelledCount =
      this.taskQueue.length + this.pendingGoogleTasks.length;

    this.taskQueue.forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error("All requests cancelled"));
    });

    this.pendingGoogleTasks.forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error("All requests cancelled"));
    });

    this.taskQueue = [];
    this.pendingGoogleTasks = [];

    console.log(`Cancelled ${cancelledCount} pending requests`);
    return cancelledCount;
  }

  cancelRequestsByEngine(searchEngine: SearchEngine) {
    const tasksToCancel = this.taskQueue.filter(
      (task) => task.searchEngine === searchEngine,
    );

    const pendingTasksToCancel = this.pendingGoogleTasks.filter(
      (task) => task.searchEngine === searchEngine,
    );

    [...tasksToCancel, ...pendingTasksToCancel].forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error(`Requests for ${searchEngine} cancelled`));
    });

    this.taskQueue = this.taskQueue.filter(
      (task) => task.searchEngine !== searchEngine,
    );

    if (searchEngine === SearchEngine.GOOGLE) {
      this.pendingGoogleTasks = [];
    }

    const totalCancelled = tasksToCancel.length + pendingTasksToCancel.length;
    console.log(`Cancelled ${totalCancelled} requests for ${searchEngine}`);
    return totalCancelled;
  }

  getStatus() {
    const activeTasks = this.taskQueue.filter((task) => !task.cancelled);
    const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

    // Count tasks by search engine
    const tasksByEngine = activeTasks.reduce(
      (acc, task) => {
        acc[task.searchEngine] = (acc[task.searchEngine] || 0) + 1;
        return acc;
      },
      {} as Record<string, number>,
    );

    return {
      queueLength: activeTasks.length,
      cancelledInQueue: cancelledTasks.length,
      tasksByEngine,
      totalTabs: this.tabPool.length,
      googleTabs: this.tabPool.filter((tab) => tab.contextType === "google")
        .length,
      defaultTabs: this.tabPool.filter((tab) => tab.contextType === "default")
        .length,
      busyTabs: this.tabPool.filter((tab) => tab.busy).length,
      maxTabs: this.maxTabs,
      maxQueueSize: this.maxQueueSize,
      googleContextActive: !!this.googleContext,
      pendingGoogleTasks: this.pendingGoogleTasks.length,
      googleContextResetting: this.googleContextResetting,
      browserInitializing: this.browserInitializing,
      lastCaptchaDetection: this.lastCaptchaDetection
        ? new Date(this.lastCaptchaDetection).toISOString()
        : null,
      timeSinceLastCaptcha: this.lastCaptchaDetection
        ? Date.now() - this.lastCaptchaDetection
        : null,
      browserHealthy: this.browserHealthy,
      ready: this.ready,
    };
  }

  async closeBrowser() {
    if (this.cleanUpIntervalId) {
      clearInterval(this.cleanUpIntervalId);
      this.cleanUpIntervalId = null;
    }

    try {
      const cancelledCount = this.cancelAllRequests();
      console.log(`Cancelled ${cancelledCount} requests during close`);

      // Carefully close tabs
      for (const tab of this.tabPool) {
        try {
          if (!tab.page.isClosed()) {
            await tab.page.close();
          }
        } catch (error) {
          console.error("Error closing tab:", error);
        }
      }
      this.tabPool = [];

      // Safely handle Google context
      if (this.googleContext) {
        try {
          this.googleContext = null; // Just remove the reference, don't try to close explicitly
        } catch (error) {
          console.error("Error clearing Google context:", error);
        }
      }

      // Close the browser if available
      if (this.browser) {
        try {
          await this.browser.close();
        } catch (error) {
          console.error("Error closing browser:", error);
        }
        this.browser = null;
      }

      this.browserHealthy = false;
      this.ready = false;
      console.log("Browser closed successfully");
    } catch (error) {
      console.error("Error closing browser:", error);

      // Force cleanup
      this.browser = null;
      this.googleContext = null;
      this.tabPool = [];
      this.browserHealthy = false;
      this.ready = false;
    }
  }
}

// Export TaskPriority for use in other modules
export { TaskPriority };
