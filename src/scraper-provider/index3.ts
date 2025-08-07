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

const isTracker =
  /(beacon|track|analytics|pixel|gtm|tagmanager|doubleclick|gstatic|xjs)/i;
const MAX_RETRIES = 2;

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
  private googleContextResetPromise: Promise<void> | null = null;
  private cleanUpIntervalId: NodeJS.Timeout | null = null;
  private pendingGoogleTasks: SearchTask[] = [];

  constructor(maxTabs: number = 1000, maxQueueSize: number = 1000) {
    super();
    this.maxTabs = Math.max(1, maxTabs);
    this.maxQueueSize = maxQueueSize;
    this.launchBrowser();
  }

  async launchBrowser() {
    try {
      console.log("Launching browser...");
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
      await this.ensureGoogleContext();

      await page.goto("https://www.google.com");

      try {
        await this.chromeContextSearchTest();
        await page.close();
        this.startIdleTabCleanup();
      } catch (error) {
        console.error("Error during initial search:", error);
        this.ready = false;
        await this.closeBrowser();
        await this.launchBrowser();
      }
    } catch (error) {
      console.error("Error launching browser:", error);
    }
  }

  private async ensureGoogleContext(): Promise<BrowserContext> {
    if (this.googleContext) {
      try {
        await this.googleContext.pages();
      } catch (error) {
        console.log("Google context is no longer valid, recreating...");
        this.googleContext = null;
      }
    }

    if (!this.googleContext) {
      if (!this.browser) {
        throw new Error("Browser not initialized");
      }
      this.googleContext = await this.browser.createBrowserContext();
      console.log("Created new Google context");
    }

    return this.googleContext;
  }

  private async chromeContextSearchTest() {
    const { cancel, promise } = await this.search(
      "Minecraft",
      SearchEngine.GOOGLE,
    );
    await promise;

    this.ready = true;
    console.log("Chrome Search Engine is ready for use.");
  }

  private async resetGoogleContext(): Promise<void> {
    if (this.googleContextResetPromise) {
      return this.googleContextResetPromise;
    }

    this.googleContextResetPromise = this._performGoogleContextReset();

    try {
      await this.googleContextResetPromise;
    } finally {
      this.googleContextResetPromise = null;
    }
  }

  private async _performGoogleContextReset(): Promise<void> {
    try {
      console.log("Starting Google context reset due to captcha detection...");

      // Move all Google tasks from main queue to pending
      const googleTasksInQueue = this.taskQueue.filter(
        (task) => task.searchEngine === SearchEngine.GOOGLE && !task.cancelled,
      );

      this.taskQueue = this.taskQueue.filter(
        (task) => task.searchEngine !== SearchEngine.GOOGLE || task.cancelled,
      );

      // Add to pending list
      this.pendingGoogleTasks.push(...googleTasksInQueue);

      const googleTabs = this.tabPool.filter(
        (tab) => tab.contextType === "google",
      );

      // Close all Google tabs
      for (const tab of googleTabs) {
        try {
          if (!tab.page.isClosed()) {
            await tab.page.close();
          }
        } catch (error) {
          console.error("Error closing Google tab:", error);
        }
      }

      // Remove Google tabs from pool
      this.tabPool = this.tabPool.filter((tab) => tab.contextType !== "google");

      // Close and recreate Google context
      if (this.googleContext) {
        try {
          await this.googleContext.close();
        } catch (error) {
          console.error("Error closing Google context:", error);
        }
        this.googleContext = null;
      }

      await this.ensureGoogleContext();

      console.log(
        `Google context reset complete. Requeuing ${this.pendingGoogleTasks.length} Google tasks.`,
      );

      // Move pending tasks back to main queue
      const tasksToRequeue = [...this.pendingGoogleTasks];
      this.pendingGoogleTasks = [];

      for (const task of tasksToRequeue) {
        if (!task.cancelled) {
          // Reset retry count for context reset
          task.retries = 0;
          this.taskQueue.unshift(task);
        }
      }

      if (tasksToRequeue.length > 0) {
        setImmediate(() => this.processQueue());
      }
    } catch (error) {
      console.error("Error resetting Google context:", error);
      throw error;
    }
  }

  private async getAvailableTab(searchEngine: SearchEngine): Promise<TabPool> {
    const useGoogleContext = searchEngine === SearchEngine.GOOGLE;
    const contextType = useGoogleContext ? "google" : "default";

    if (useGoogleContext) {
      await this.ensureGoogleContext();
    }

    const currentTabsOfType = this.tabPool.filter(
      (tab) => tab.contextType === contextType,
    );

    // Find available tab
    const availableTab = currentTabsOfType.find((tab) => !tab.busy);
    if (availableTab) {
      return availableTab;
    }

    const maxTabsForContext = useGoogleContext
      ? Math.max(1, Math.floor(this.maxTabs / 2))
      : Math.max(1, this.maxTabs - Math.floor(this.maxTabs / 2));

    if (currentTabsOfType.length >= maxTabsForContext) {
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

    try {
      let page: Page;
      if (useGoogleContext) {
        const context = await this.ensureGoogleContext();
        page = await context.newPage();
      } else {
        if (!this.browser) throw new Error("Browser not initialized");
        page = await this.browser.newPage();
      }

      if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
        await this.setupRequestBlocking(page);
      }

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
      console.error("Error creating new tab:", error);
      throw error;
    }
  }

  private startIdleTabCleanup() {
    this.cleanUpIntervalId = setInterval(async () => {
      const now = Date.now();

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

      if (defaultTabs.length > 1 && idleDefaultTabs.length > 0) {
        const tabsToClose = idleDefaultTabs.slice(
          0,
          idleDefaultTabs.length - 1,
        );
        await this.closeTabs(tabsToClose);
      }

      if (googleTabs.length > 1 && idleGoogleTabs.length > 0) {
        const tabsToClose = idleGoogleTabs.slice(0, idleGoogleTabs.length - 1);
        await this.closeTabs(tabsToClose);
      }
    }, 60000);
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
      }
    }
  }

  private async setupRequestBlocking(page: Page): Promise<void> {
    await page.setRequestInterception(true);
    page.on("request", (request) => {
      const t = request.resourceType();
      if (t === "document") {
        request.continue();
      } else if (t === "script" && !isTracker.test(request.url())) {
        request.continue();
      } else {
        request.abort();
      }
    });
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
      if (["image", "font", "media"].includes(resourceType)) {
        metrics.blockedRequests++;
      }
    });

    page.on("request", (request) => {
      const resourceType = request.resourceType();
      if (!["image", "font", "media"].includes(resourceType)) {
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
      if (!["image", "font", "media"].includes(resourceType)) {
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

    console.log(`🔍 Search Bandwidth Report - Task ${taskId}`);
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

    while (this.taskQueue.length > 0) {
      const validTasks = this.taskQueue.filter((task) => !task.cancelled);
      const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

      cancelledTasks.forEach((task) => {
        task.reject(new Error("Request cancelled"));
      });

      this.taskQueue = validTasks;
      if (this.taskQueue.length === 0) break;

      const task = this.taskQueue.shift()!;
      if (task.cancelled) {
        task.reject(new Error("Request cancelled"));
        continue;
      }

      try {
        const tab = await this.getAvailableTab(task.searchEngine);
        if (task.cancelled) {
          task.reject(new Error("Request cancelled"));
          continue;
        }

        tab.busy = true;
        tab.lastUsed = Date.now();

        console.log(
          `Processing task ${task.id}. Queue: ${this.taskQueue.length}`,
        );

        this.executeSearch(tab, task).finally(() => {
          tab.busy = false;
          tab.lastUsed = Date.now();
        });
      } catch (error) {
        if (!task.cancelled) {
          task.reject(error as Error);
        }
      }
    }

    this.processingQueue = false;
  }

  private async executeSearch(tab: TabPool, task: SearchTask) {
    let bandwidthMetrics: BandwidthMetrics | null = null;
    let captchaDetected = false;

    try {
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      if (process.env.ENABLE_BANDWIDTH_LOGGING === "true") {
        bandwidthMetrics = this.setupBandwidthTracking(tab.page);
      }

      const url = await this.urlQueryProvider(task.query, task.searchEngine);

      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      tab.page.setDefaultNavigationTimeout(
        task.searchEngine === SearchEngine.GOOGLE ? 60000 : 30000,
      );

      await Promise.race([
        tab.page.goto(url, { waitUntil: "load" }),
        new Promise((_, reject) => {
          task.abortController.signal.addEventListener("abort", () => {
            reject(new Error("Request cancelled"));
          });
        }),
      ]);

      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      captchaDetected = await this.detectCaptcha(tab.page);
      if (captchaDetected && task.searchEngine === SearchEngine.GOOGLE) {
        console.log(
          `Captcha detected in task ${task.id} - triggering Google context reset`,
        );

        // Add this task to pending Google tasks for requeue after reset
        if (!task.cancelled) {
          this.pendingGoogleTasks.push(task);
        }

        // Trigger context reset (but don't await it here to avoid blocking)
        setImmediate(() => this.resetGoogleContext());

        // Return early - task will be reprocessed after context reset
        return;
      }

      const results = await this.preprocessPageResult(
        tab.page,
        task.searchEngine,
      );
      await tab.page.goto("about:blank");

      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      if (bandwidthMetrics) {
        this.logBandwidthMetrics(
          task.id,
          task.query,
          task.searchEngine,
          bandwidthMetrics,
        );
      }

      task.resolve(results);
    } catch (error) {
      if (bandwidthMetrics) {
        this.logBandwidthMetrics(
          task.id,
          task.query,
          task.searchEngine,
          bandwidthMetrics,
        );
      }

      // Don't handle captcha errors here - they're handled above
      if (captchaDetected && task.searchEngine === SearchEngine.GOOGLE) {
        return; // Task already added to pending queue
      }

      if (!task.cancelled) {
        // Handle other errors with regular retry logic
        if (task.retries < MAX_RETRIES) {
          task.retries++;
          this.taskQueue.unshift(task); // Add to front of queue for immediate retry
          console.log(
            `Retrying task ${task.id} (attempt ${task.retries}/${MAX_RETRIES})`,
          );
          setImmediate(() => this.processQueue());
        } else {
          task.reject(
            new Error(
              `Max retries (${MAX_RETRIES}) reached: ${(error as Error).message}`,
            ),
          );
        }
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
      ];

      for (const selector of captchaSelectors) {
        if (await page.$(selector)) return true;
      }

      return await page.evaluate(() => {
        const text = document.body.innerText;
        return /captcha|verify you|not a robot/i.test(text);
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
    if (!this.browser) {
      await this.launchBrowser();
      return this.search(query, searchEngine);
    }

    if (this.taskQueue.length >= this.maxQueueSize) {
      throw new Error(
        `Queue is full. Maximum queue size: ${this.maxQueueSize}`,
      );
    }

    const abortController = new AbortController();
    const taskId = `task_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

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
    const cancelledCount = this.taskQueue.length;
    this.taskQueue.forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error("All requests cancelled"));
    });
    this.taskQueue = [];
    console.log(`Cancelled ${cancelledCount} pending requests`);
    return cancelledCount;
  }

  cancelRequestsByEngine(searchEngine: SearchEngine) {
    const tasksToCancel = this.taskQueue.filter(
      (task) => task.searchEngine === searchEngine,
    );
    tasksToCancel.forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error(`Requests for ${searchEngine} cancelled`));
    });
    this.taskQueue = this.taskQueue.filter(
      (task) => task.searchEngine !== searchEngine,
    );
    console.log(
      `Cancelled ${tasksToCancel.length} requests for ${searchEngine}`,
    );
    return tasksToCancel.length;
  }

  getStatus() {
    const activeTasks = this.taskQueue.filter((task) => !task.cancelled);
    const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

    return {
      queueLength: activeTasks.length,
      cancelledInQueue: cancelledTasks.length,
      totalTabs: this.tabPool.length,
      googleTabs: this.tabPool.filter((tab) => tab.contextType === "google")
        .length,
      defaultTabs: this.tabPool.filter((tab) => tab.contextType === "default")
        .length,
      busyTabs: this.tabPool.filter((tab) => tab.busy).length,
      maxTabs: this.maxTabs,
      maxQueueSize: this.maxQueueSize,
      googleContextActive: !!this.googleContext,
      pendingGoogleRetries: this.pendingGoogleTasks.length,
      googleContextResetInProgress: !!this.googleContextResetPromise,
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

      for (const tab of this.tabPool) {
        try {
          if (!tab.page.isClosed()) await tab.page.close();
        } catch (error) {
          console.error("Error closing tab:", error);
        }
      }
      this.tabPool = [];

      if (this.googleContext) {
        await this.googleContext.close();
        this.googleContext = null;
      }

      await this.browser?.close();
      this.browser = null;
      console.log("Browser closed successfully");
    } catch (error) {
      console.error("Error closing browser:", error);
    }
  }
}
