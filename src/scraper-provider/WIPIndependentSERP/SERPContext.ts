import { Browser, Page } from "puppeteer";
import {
  BandwidthMetrics,
  SearchEngine,
  SearchResult,
  SearchTask,
  TabPool,
} from ".";
import PreprocessService from "./services/preprocessService";

// Cached regular expressions for performance
const isTracker =
  /(beacon|track|analytics|pixel|gtm|tagmanager|doubleclick|gstatic|xjs)/i;

export class SERPContextBase extends PreprocessService {
  private browser: Browser | null = null;
  private tabPool: TabPool[] = [];
  private taskQueue: SearchTask[] = [];
  private maxTabs: number;
  private maxQueueSize: number;
  private processingQueue: boolean = false;
  private tabIdleTimeout: number = 60000;
  private cleanUpIntervalId: NodeJS.Timeout | null = null;
  // private preprocessService: PreprocessService;

  constructor(
    // preprocessService: PreprocessService,
    maxTabs: number = 1000,
    maxQueueSize: number = 1000,
  ) {
    super();
    this.maxTabs = maxTabs;
    this.maxQueueSize = maxQueueSize;
    // this.preprocessService = preprocessService;

    this.startIdleTabCleanup();
  }
  async setContext(browser: Browser) {
    this.browser = browser;
  }
  private startIdleTabCleanup() {
    this.cleanUpIntervalId = setInterval(async () => {
      const now = Date.now();
      const idleTabs = this.tabPool.filter(
        (tab) => !tab.busy && now - tab.lastUsed > this.tabIdleTimeout,
      );

      // Keep at least 1 tab, close excess idle tabs
      if (this.tabPool.length > 1 && idleTabs.length > 0) {
        const tabsToClose = idleTabs.slice(0, idleTabs.length - 1);

        for (const tab of tabsToClose) {
          try {
            await tab.page.close();
            this.tabPool = this.tabPool.filter((t) => t !== tab);
            console.log(`Closed idle tab. Pool size: ${this.tabPool.length}`);
          } catch (error) {
            console.error("Error closing idle tab:", error);
          }
        }
      }
    }, 60000); // Check every minute
  }

  private async getAvailableTab(): Promise<TabPool> {
    // Try to find an available tab
    let availableTab = this.tabPool.find((tab) => !tab.busy);

    if (!availableTab && this.tabPool.length < this.maxTabs) {
      // Create new tab if under limit
      try {
        const page = await this.browser!.newPage();
        // await page.setViewport({ width: 1280, height: 800 });

        if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
          await this.setupRequestBlocking(page);
        }

        const newTab: TabPool = {
          page,
          busy: false,
          lastUsed: Date.now(),
        };

        this.tabPool.push(newTab);
        availableTab = newTab;
        console.log(`Created new tab. Pool size: ${this.tabPool.length}`);
      } catch (error) {
        console.error("Error creating new tab:", error);
        throw error;
      }
    }

    if (!availableTab) {
      // Wait for a tab to become available
      return new Promise((resolve) => {
        const checkAvailability = () => {
          const tab = this.tabPool.find((t) => !t.busy);
          if (tab) {
            resolve(tab);
          } else {
            setTimeout(checkAvailability, 100);
          }
        };
        checkAvailability();
      });
    }

    return availableTab;
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

    // Track blocked requests
    page.on("requestfailed", (request) => {
      const resourceType = request.resourceType();
      if (
        resourceType === "image" ||
        resourceType === "font" ||
        resourceType === "media"
      ) {
        metrics.blockedRequests++;
      }
    });

    // Track requests
    page.on("request", (request) => {
      const resourceType = request.resourceType();
      // Only count requests that we're not blocking
      if (
        resourceType !== "image" &&
        resourceType !== "font" &&
        resourceType !== "media"
      ) {
        metrics.requestCount++;
        // Log request size if available (usually not available for outgoing requests)
        const postData = request.postData();
        if (postData) {
          metrics.totalBytes += Buffer.byteLength(postData, "utf8");
        }
      }
    });

    // Track responses
    page.on("response", async (response) => {
      const request = response.request();
      const resourceType = request.resourceType();

      // Only count responses for requests we're not blocking
      if (
        resourceType !== "image" &&
        resourceType !== "font" &&
        resourceType !== "media"
      ) {
        metrics.responseCount++;
        try {
          // Get response size from headers
          const contentLength = response.headers()["content-length"];
          if (contentLength) {
            metrics.totalBytes += parseInt(contentLength, 10);
          } else {
            // If no content-length header, try to get the actual response size
            try {
              const buffer = await response.buffer();
              metrics.totalBytes += buffer.length;
            } catch (bufferError) {
              // If we can't get the buffer, estimate based on response status
              // This is a fallback for cases where the response body can't be accessed
              if (response.ok()) {
                metrics.totalBytes += 1024; // Estimate 1KB for successful responses without size
              }
            }
          }
        } catch (error) {
          // Silently handle errors in response size tracking
          // Don't let bandwidth tracking interfere with the main functionality
        }
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
    console.log(
      `   Blocked Requests: ${metrics.blockedRequests} (images/fonts/media/trackers)`,
    );
    console.log(
      `   Avg per request: ${(metrics.totalBytes / Math.max(metrics.responseCount, 1) / 1024).toFixed(2)} KB`,
    );
    console.log(`   ────────────────────────────────────────────────────`);
  }

  private async processQueue() {
    if (this.processingQueue || this.taskQueue.length === 0) {
      return;
    }

    this.processingQueue = true;

    while (this.taskQueue.length > 0) {
      // Remove cancelled tasks from queue
      const validTasks = this.taskQueue.filter((task) => !task.cancelled);
      const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

      // Clean up cancelled tasks
      cancelledTasks.forEach((task) => {
        task.reject(new Error("Request cancelled"));
      });

      this.taskQueue = validTasks;

      if (this.taskQueue.length === 0) {
        break;
      }

      const task = this.taskQueue.shift()!;

      // Double-check if task was cancelled while waiting
      if (task.cancelled) {
        task.reject(new Error("Request cancelled"));
        continue;
      }

      try {
        const tab = await this.getAvailableTab();

        // Check again after getting tab (in case cancelled while waiting)
        if (task.cancelled) {
          task.reject(new Error("Request cancelled"));
          continue;
        }

        tab.busy = true;
        tab.lastUsed = Date.now();

        console.log(
          `Processing task ${task.id}. Queue length: ${this.taskQueue.length}`,
        );

        // Process the search in background
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

    try {
      // Check if cancelled before starting
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      // Setup bandwidth tracking before navigation
      if (process.env.ENABLE_BANDWIDTH_LOGGING === "true") {
        bandwidthMetrics = this.setupBandwidthTracking(tab.page);
      }

      const url = await this.urlQueryProvider(task.query, task.searchEngine);

      // Check if cancelled before navigation
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      // Updated this since sometimes Google Search can take longer
      if (task.searchEngine === SearchEngine.GOOGLE) {
        tab.page.setDefaultNavigationTimeout(60000);
      } else {
        tab.page.setDefaultNavigationTimeout(30000);
      }

      // Navigate with timeout and abort signal awareness
      await Promise.race([
        tab.page.goto(url, { waitUntil: "load" }),
        new Promise((_, reject) => {
          task.abortController.signal.addEventListener("abort", () => {
            reject(new Error("Request cancelled"));
          });
        }),
      ]);

      // Check if cancelled after navigation
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      const results = await this.preprocessPageResult(
        tab.page,
        task.searchEngine,
      );

      await tab.page.goto("about:blank");
      // Final check before resolving
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      // Log bandwidth metrics before resolving
      if (bandwidthMetrics && process.env.ENABLE_BANDWIDTH_LOGGING === "true") {
        this.logBandwidthMetrics(
          task.id,
          task.query,
          task.searchEngine,
          bandwidthMetrics,
        );
      }

      task.resolve(results);
    } catch (error) {
      // Log bandwidth metrics even on error
      if (bandwidthMetrics && process.env.ENABLE_BANDWIDTH_LOGGING === "true") {
        this.logBandwidthMetrics(
          task.id,
          task.query,
          task.searchEngine,
          bandwidthMetrics,
        );
      }

      if (!task.cancelled) {
        task.reject(error as Error);
      }
    }
  }

  // Public search method - adds to queue and returns cancellable promise
  async search(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
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
      };

      this.taskQueue.push(task);
      console.log(
        `Added task ${taskId} to queue. Queue length: ${this.taskQueue.length}`,
      );

      // Start processing if not already processing
      this.processQueue();
    });

    const cancel = () => {
      // Find and mark task as cancelled
      const task = this.taskQueue.find((t) => t.id === taskId);
      if (task) {
        task.cancelled = true;
        task.abortController.abort();
        console.log(`Cancelled task ${taskId}`);
      }
    };

    return {
      promise: searchPromise,
      cancel,
    };
  }

  // Convenience method for backward compatibility
  async searchSimple(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<SearchResult[]> {
    const { promise } = await this.search(query, searchEngine);
    return promise;
  }

  // Cancel all pending requests
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

  // Cancel requests by search engine
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
  // Get queue and pool status
  getStatus() {
    const activeTasks = this.taskQueue.filter((task) => !task.cancelled);
    const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

    return {
      queueLength: activeTasks.length,
      cancelledInQueue: cancelledTasks.length,
      totalTabs: this.tabPool.length,
      busyTabs: this.tabPool.filter((tab) => tab.busy).length,
      availableTabs: this.tabPool.filter((tab) => !tab.busy).length,
      maxTabs: this.maxTabs,
      maxQueueSize: this.maxQueueSize,
    };
  }

  async closeBrowser() {
    if (this.cleanUpIntervalId) {
      clearInterval(this.cleanUpIntervalId);
      this.cleanUpIntervalId = null;
    }
    try {
      // Cancel and clear the queue
      const cancelledCount = this.cancelAllRequests();
      console.log(
        `Cancelled ${cancelledCount} pending requests during browser close`,
      );

      // Close all tabs
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

      await this.browser?.close();
      this.browser = null;
      console.log("Browser closed successfully!");
    } catch (error) {
      console.error("Error closing browser:", error);
    }
  }

  private async preprocessPageResult(
    page: Page,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<SearchResult[]> {
    try {
      await page.bringToFront();

      switch (searchEngine) {
        case SearchEngine.GOOGLE:
          return await this.processGoogleResults(page);
        case SearchEngine.BING:
          return await this.processBingResults(page);
        case SearchEngine.DUCKDUCKGO:
          return await this.processDuckDuckGoResults(page);
        case SearchEngine.YAHOO:
          return await this.processYahooResults(page);
        default:
          throw new Error("Unsupported search engine");
      }
    } catch (error) {
      console.error("Error preprocessing page result:", error);
      throw error;
    }
  }

  private async urlQueryProvider(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<string> {
    switch (searchEngine) {
      case SearchEngine.GOOGLE:
        return `https://www.google.com/search?q=${encodeURIComponent(query)}`;
      case SearchEngine.BING:
        return `https://www.bing.com/search?q=${encodeURIComponent(query)}`;
      case SearchEngine.DUCKDUCKGO:
        return `https://duckduckgo.com/?q=${encodeURIComponent(query)}`;
      case SearchEngine.YAHOO:
        return `https://search.yahoo.com/search?p=${encodeURIComponent(query)}`;
      default:
        throw new Error("Unsupported search engine");
    }
  }
}

export const BaseSerpContext = new SERPContextBase();
