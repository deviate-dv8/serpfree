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
  private browserInitializing: boolean = false;
  private initializing: Promise<void> | null = null;
  private cleanUpIntervalId: NodeJS.Timeout | null = null;
  private pendingGoogleTasks: SearchTask[] = [];
  private lastCaptchaDetection: number = 0;
  private browserHealthy: boolean = false;
  private googleHealthy: boolean = false;
  private concurrentSearches: number = 0;
  private maxConcurrentSearches: number = 20; // Limit concurrent searches

  constructor(maxTabs: number = 1000, maxQueueSize: number = 1000) {
    super();
    this.maxTabs = Math.max(1, maxTabs);
    this.maxQueueSize = maxQueueSize;
    this.initializing = this.launchBrowser();
  }

  async launchBrowser(): Promise<void> {
    if (this.browserInitializing) {
      console.log("Browser initialization already in progress");
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
        // Prepare initial page
        await page.goto("about:blank");
        await this.setupAggressiveRequestBlocking(page);

        // Setup initial tab
        this.tabPool.push({
          page,
          busy: false,
          lastUsed: Date.now(),
          contextType: "default",
        });

        // Create Google context and run test without blocking
        this.createGoogleContext().then(() => {
          this.runGoogleContextTest()
            .then(() => {
              console.log("Google context test completed successfully");
              this.googleHealthy = true;
            })
            .catch((error) => {
              console.error("Google context test failed:", error);
              this.googleHealthy = false;
            });
        });

        this.startIdleTabCleanup();
        this.ready = true;
        console.log("Browser initialized and ready for non-Google searches");
      } catch (error) {
        console.error("Error during browser initialization:", error);
        this.browserHealthy = false;
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
    // Don't create if already resetting
    if (this.googleContextResetting) {
      console.log("Google context is being reset, waiting to create");
      return null;
    }

    try {
      if (!this.browser || !this.browserHealthy) {
        console.log("Browser not healthy, cannot create Google context");
        return null;
      }

      // Check if we already have a valid context
      if (this.googleContext) {
        try {
          await this.googleContext.pages();
          console.log("Using existing Google context");
          return this.googleContext;
        } catch (error) {
          console.log("Existing Google context is invalid, recreating");
          this.googleContext = null;
        }
      }

      // Create new context
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

  private async runGoogleContextTest(): Promise<SearchResult[]> {
    // Make sure we're not already resetting
    if (this.googleContextResetting) {
      throw new Error("Cannot run test while context is being reset");
    }

    console.log("Running Google context test (Minecraft search)...");

    // Create a test page in the Google context
    if (!this.googleContext) {
      throw new Error("Google context not available for test");
    }

    const testPage = await this.googleContext.newPage();
    await this.setupAggressiveRequestBlocking(testPage);

    try {
      // Generate search URL
      const query = "Minecraft";
      const url = await this.urlQueryProvider(query, SearchEngine.GOOGLE);

      // Navigate directly
      await testPage.goto(url, { waitUntil: "domcontentloaded" });

      // Check for captcha
      const captchaDetected = await this.detectCaptcha(testPage);
      if (captchaDetected) {
        throw new Error("Captcha detected during test");
      }

      // Process the results manually
      const results = await this.preprocessPageResult(
        testPage,
        SearchEngine.GOOGLE,
      );
      console.log(
        `Google context test successful, found ${results.length} results`,
      );

      return results;
    } finally {
      // Always clean up test page
      try {
        await testPage.goto("about:blank");
        await testPage.close();
      } catch (err) {
        console.error("Error closing test page:", err);
      }
    }
  }

  private async resetGoogleContext(): Promise<void> {
    // Check if already resetting
    if (this.googleContextResetting) {
      console.log("Google context reset already in progress");
      return;
    }

    this.googleContextResetting = true;
    this.googleHealthy = false;
    console.log("Starting Google context reset due to captcha detection");
    this.lastCaptchaDetection = Date.now();

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

      // Close all Google tabs
      for (const tab of googleTabs) {
        try {
          if (!tab.page.isClosed()) {
            await tab.page.goto("about:blank"); // Navigate to blank page first
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
      const newContext = await this.createGoogleContext();
      if (!newContext) {
        throw new Error("Failed to create new Google context");
      }

      // Run the test search on the new context
      await this.runGoogleContextTest();

      // Mark Google as healthy
      this.googleHealthy = true;

      // Move pending tasks back to main queue
      const tasksToRequeue = [...this.pendingGoogleTasks];
      this.pendingGoogleTasks = [];

      console.log(
        `Google context reset complete. Requeuing ${tasksToRequeue.length} Google tasks.`,
      );

      // Add tasks back to queue
      this.taskQueue.unshift(
        ...tasksToRequeue.filter((task) => !task.cancelled),
      );

      // Process queue in the next tick
      if (this.taskQueue.length > 0) {
        setImmediate(() => this.processQueue());
      }
    } catch (error) {
      console.error("Error during Google context reset:", error);
      this.googleHealthy = false;

      // Keep pending tasks for later retry
      console.log(
        `Keeping ${this.pendingGoogleTasks.length} tasks pending due to reset failure`,
      );

      // Schedule another attempt in 10 seconds
      setTimeout(() => {
        this.googleContextResetting = false;
        this.resetGoogleContext();
      }, 10000);
    } finally {
      this.googleContextResetting = false;
    }
  }

  private async getAvailableTab(searchEngine: SearchEngine): Promise<TabPool> {
    const useGoogleContext = searchEngine === SearchEngine.GOOGLE;
    const contextType = useGoogleContext ? "google" : "default";

    // For Google searches, check if Google is healthy
    if (useGoogleContext && !this.googleHealthy) {
      throw new Error("Google context is not healthy");
    }

    // For all searches, check if browser is healthy
    if (!this.browserHealthy) {
      throw new Error("Browser is not healthy");
    }

    // If Google context is being reset and this is a Google search, fail fast
    if (useGoogleContext && this.googleContextResetting) {
      throw new Error("Google context is being reset");
    }

    // Find tabs of the right context type - filter out closed tabs
    const currentTabsOfType = this.tabPool.filter((tab) => {
      try {
        return tab.contextType === contextType && !tab.page.isClosed();
      } catch (e) {
        return false;
      }
    });

    // Find available tab
    const availableTab = currentTabsOfType.find((tab) => !tab.busy);
    if (availableTab) {
      return availableTab;
    }

    // Calculate max tabs per context
    const maxTabsForContext = useGoogleContext
      ? Math.max(1, Math.floor(this.maxTabs / 3)) // Limit Google tabs to 1/3
      : Math.max(1, Math.floor((this.maxTabs * 2) / 3)); // Allow more default tabs

    // If we've reached the max tabs for this context, wait for one to become available
    if (currentTabsOfType.length >= maxTabsForContext) {
      return new Promise((resolve, reject) => {
        let attempts = 0;
        const maxAttempts = 100; // Prevent infinite waiting

        const checkAvailability = () => {
          // Check if context is still valid
          if (
            useGoogleContext &&
            (!this.googleHealthy || this.googleContextResetting)
          ) {
            return reject(
              new Error("Google context became unavailable while waiting"),
            );
          }

          if (!this.browserHealthy) {
            return reject(new Error("Browser became unhealthy while waiting"));
          }

          const tab = this.tabPool.find((t) => {
            try {
              return (
                !t.busy && t.contextType === contextType && !t.page.isClosed()
              );
            } catch (e) {
              return false;
            }
          });

          if (tab) {
            resolve(tab);
          } else if (++attempts >= maxAttempts) {
            reject(
              new Error(`Timeout waiting for available ${contextType} tab`),
            );
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
        if (!this.googleContext) {
          throw new Error("Google context not available");
        }
        page = await this.googleContext.newPage();
      } else {
        if (!this.browser) {
          throw new Error("Browser not initialized");
        }
        page = await this.browser.newPage();
      }

      // Initialize with a blank page
      await page.goto("about:blank");

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

      // Filter out closed pages first
      this.tabPool = this.tabPool.filter((tab) => {
        try {
          return !tab.page.isClosed();
        } catch (e) {
          return false;
        }
      });

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
          await tab.page.goto("about:blank"); // Navigate to blank page first
          await tab.page.close();
        }
      } catch (error) {
        console.error("Error closing tab:", error);
      } finally {
        // Always remove from pool
        this.tabPool = this.tabPool.filter((t) => t !== tab);
      }
    }
    console.log(`Closed tabs. Pool size: ${this.tabPool.length}`);
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

  private processQueue() {
    if (this.processingQueue || this.taskQueue.length === 0) return;
    this.processingQueue = true;

    // Function to process a single task
    const processTask = async (task: SearchTask): Promise<void> => {
      // Don't process cancelled tasks
      if (task.cancelled) {
        task.reject(new Error("Request cancelled"));
        return;
      }

      // Don't process Google tasks if Google context is not healthy or resetting
      if (
        task.searchEngine === SearchEngine.GOOGLE &&
        (!this.googleHealthy || this.googleContextResetting)
      ) {
        // Put task back in queue
        this.taskQueue.push(task);
        return;
      }

      try {
        // Wait if we're at max concurrent searches
        if (this.concurrentSearches >= this.maxConcurrentSearches) {
          // Put task back in queue
          this.taskQueue.push(task);
          return;
        }

        // Get tab for task
        const tab = await this.getAvailableTab(task.searchEngine);

        // Check if task was cancelled during tab acquisition
        if (task.cancelled) {
          task.reject(new Error("Request cancelled"));
          return;
        }

        // Mark tab as busy
        tab.busy = true;
        tab.lastUsed = Date.now();

        // Increment concurrent searches
        this.concurrentSearches++;

        console.log(
          `Processing task ${task.id}. Queue: ${this.taskQueue.length}`,
        );

        // Execute search in background
        this.executeSearch(tab, task).finally(() => {
          tab.busy = false;
          tab.lastUsed = Date.now();
          this.concurrentSearches--;

          // Process next tasks if available
          setImmediate(() => this.processQueue());
        });
      } catch (error) {
        console.error(`Error processing task ${task.id}:`, error);

        if (!task.cancelled) {
          if (
            task.searchEngine === SearchEngine.GOOGLE &&
            (error.message?.includes("Google context") ||
              error.message?.includes("captcha"))
          ) {
            console.log(`Adding Google task ${task.id} back to queue`);
            this.taskQueue.push(task);

            // If captcha, trigger context reset
            if (
              error.message?.includes("captcha") &&
              !this.googleContextResetting
            ) {
              setImmediate(() => this.resetGoogleContext());
            }
          } else if (task.retries < MAX_RETRIES) {
            task.retries++;
            console.log(
              `Retrying task ${task.id} (${task.retries}/${MAX_RETRIES})`,
            );
            this.taskQueue.push(task);
          } else {
            task.reject(error as Error);
          }
        }
      }
    };

    try {
      // Sort by priority
      this.taskQueue.sort((a, b) => b.priority - a.priority);

      // Filter out cancelled tasks
      const validTasks = this.taskQueue.filter((task) => !task.cancelled);
      const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

      // Reject all cancelled tasks
      cancelledTasks.forEach((task) => {
        task.reject(new Error("Request cancelled"));
      });

      this.taskQueue = [];

      // Process tasks in batches for better throughput
      // Take up to maxConcurrentSearches - concurrentSearches tasks
      const availableSlots =
        this.maxConcurrentSearches - this.concurrentSearches;
      const tasksToProcess = validTasks.slice(0, Math.max(0, availableSlots));

      // Keep remaining tasks in queue
      this.taskQueue = [...validTasks.slice(Math.max(0, availableSlots))];

      // Process the batch
      const promises = tasksToProcess.map(processTask);

      // Don't await - we want this to run in background
      Promise.all(promises);
    } catch (error) {
      console.error("Error in queue processing:", error);
    } finally {
      this.processingQueue = false;

      // Schedule next queue processing if there are remaining tasks
      if (this.taskQueue.length > 0) {
        // Use random delay for better load distribution
        setTimeout(() => this.processQueue(), Math.random() * 100);
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

      // Navigate to about:blank first to ensure a clean state
      await tab.page.goto("about:blank");

      // Navigate to search URL
      await Promise.race([
        tab.page.goto(url, {
          waitUntil: "domcontentloaded",
          timeout: 60000,
        }),
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

        // Trigger reset if not already resetting
        if (!this.googleContextResetting) {
          // Use non-blocking reset
          setImmediate(() => this.resetGoogleContext());
        }

        throw new Error("Captcha detected, task will be retried after reset");
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

      // For Google captcha, let the caller handle it
      if (captchaDetected && task.searchEngine === SearchEngine.GOOGLE) {
        throw new Error("Captcha detected in Google search");
      }

      // For connection errors, bubble up the error
      if (
        error.message?.includes("Protocol error") ||
        error.message?.includes("Target closed") ||
        error.message?.includes("Connection closed") ||
        error.message?.includes("main frame too early")
      ) {
        console.log(`Connection error in task ${task.id}: ${error.message}`);
        throw error;
      }

      // For other errors, reject the task
      console.error(`Task ${task.id} failed:`, error.message);
      throw error;
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
    // For Google searches, make sure Google is healthy
    if (
      searchEngine === SearchEngine.GOOGLE &&
      !this.googleHealthy &&
      !this.googleContextResetting
    ) {
      // Try to initialize Google context if needed
      if (!this.googleContext) {
        await this.createGoogleContext();
        try {
          await this.runGoogleContextTest();
          this.googleHealthy = true;
        } catch (error) {
          console.error("Google context test failed during search:", error);
          // Continue anyway - will fail gracefully later
        }
      }
    }

    // Make sure browser is initialized
    if (!this.browser || !this.browserHealthy) {
      if (this.initializing) {
        await this.initializing;
      } else {
        await this.launchBrowser();
      }
    }

    // Check queue size
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

      // Immediately start processing queue
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
      concurrentSearches: this.concurrentSearches,
      maxConcurrentSearches: this.maxConcurrentSearches,
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
      googleHealthy: this.googleHealthy,
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
            await tab.page.goto("about:blank"); // Navigate to blank page first
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
      this.googleHealthy = false;
      this.ready = false;
      console.log("Browser closed successfully");
    } catch (error) {
      console.error("Error closing browser:", error);

      // Force cleanup
      this.browser = null;
      this.googleContext = null;
      this.tabPool = [];
      this.browserHealthy = false;
      this.googleHealthy = false;
      this.ready = false;
    }
  }
}

// Export TaskPriority for use in other modules
export { TaskPriority };
