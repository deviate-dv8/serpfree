import { Browser, Page } from "puppeteer";
import {
  BandwidthMetrics,
  SearchEngine,
  SearchResult,
  SearchTask,
  TabPool,
} from ".";
import PreprocessService from "./services/preprocessService";

export class BaseSERPContext extends PreprocessService {
  private browser: Browser;
  private tabPool: TabPool[] = [];
  private taskQueue: SearchTask[] = [];
  private maxTabs: number;
  private maxQueueSize: number;
  private processingQueue: boolean = false;
  private initComplete: boolean = false;
  private tabIdleTimeout: number = 60000;

  constructor(browser: Browser, maxTabs: number, maxQueueSize: number) {
    super();
    this.browser = browser;
    this.maxTabs = maxTabs;
    this.maxQueueSize = maxQueueSize;
  }

  async initialize(): Promise<boolean> {
    try {
      // Create initial tab for the base context
      const page = await this.browser.newPage();

      // Set up resource blocking if enabled
      if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
        await this.setupRequestBlocking(page);
      }

      // Add to tab pool
      this.tabPool.push({
        page,
        busy: false,
        lastUsed: Date.now(),
        contextType: "default",
      });

      console.log("Base SERP context initialized successfully");
      this.initComplete = true;
      return true;
    } catch (error) {
      console.error("Failed to initialize Base context:", error);
      return false;
    }
  }

  private async getAvailableTab(): Promise<TabPool> {
    // Look for an available tab first
    const availableTab = this.tabPool.find((tab) => !tab.busy);

    if (availableTab) {
      return availableTab;
    }

    // Create a new tab if under the limit
    if (this.tabPool.length < this.maxTabs) {
      try {
        const page = await this.browser.newPage();

        if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
          await this.setupRequestBlocking(page);
        }

        const newTab: TabPool = {
          page,
          busy: false,
          lastUsed: Date.now(),
          contextType: "default",
        };

        this.tabPool.push(newTab);
        console.log(`Created new Base tab. Total: ${this.tabPool.length}`);
        return newTab;
      } catch (error) {
        console.error("Error creating new Base tab:", error);
        throw error;
      }
    }

    // If we reach here, we need to wait for a tab
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

  private async setupRequestBlocking(page: Page): Promise<void> {
    const isTracker =
      /(beacon|track|analytics|pixel|gtm|tagmanager|doubleclick|gstatic|xjs)/i;
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

  cleanupIdleTabs(): void {
    const now = Date.now();
    const idleTabs = this.tabPool.filter(
      (tab) => !tab.busy && now - tab.lastUsed > this.tabIdleTimeout,
    );

    // Keep at least one tab
    if (this.tabPool.length > 1 && idleTabs.length > 0) {
      const tabsToClose = idleTabs.slice(0, idleTabs.length - 1);

      tabsToClose.forEach(async (tab) => {
        try {
          if (!tab.page.isClosed()) {
            await tab.page.close();
          }
          this.tabPool = this.tabPool.filter((t) => t !== tab);
        } catch (error) {
          console.error("Error closing idle Base tab:", error);
        }
      });

      console.log(
        `Closed ${tabsToClose.length} idle Base tabs. Remaining: ${this.tabPool.length}`,
      );
    }
  }

  async search(
    query: string,
    searchEngine: SearchEngine,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
    if (!this.initComplete) {
      throw new Error("Base context not fully initialized");
    }

    if (this.taskQueue.length >= this.maxQueueSize) {
      throw new Error(`Base queue is full. Maximum size: ${this.maxQueueSize}`);
    }

    const abortController = new AbortController();
    const taskId = `base_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

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
        `Added Base task ${taskId} to queue. Queue: ${this.taskQueue.length}`,
      );
      this.processQueue();
    });

    const cancel = () => {
      const task = this.taskQueue.find((t) => t.id === taskId);
      if (task) {
        task.cancelled = true;
        task.abortController.abort();
        this.taskQueue = this.taskQueue.filter((t) => t !== task);
        console.log(`Cancelled Base task ${taskId}`);
      }
    };

    return { promise: searchPromise, cancel };
  }

  async searchSimple(
    query: string,
    searchEngine: SearchEngine,
  ): Promise<SearchResult[]> {
    const { promise } = await this.search(query, searchEngine);
    return promise;
  }

  private async processQueue(): Promise<void> {
    if (this.processingQueue || this.taskQueue.length === 0) {
      return;
    }

    this.processingQueue = true;

    while (this.taskQueue.length > 0) {
      // Remove cancelled tasks
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
        const tab = await this.getAvailableTab();
        if (task.cancelled) {
          task.reject(new Error("Request cancelled"));
          continue;
        }

        tab.busy = true;
        tab.lastUsed = Date.now();

        console.log(
          `Processing Base task ${task.id}. Queue: ${this.taskQueue.length}`,
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

  private async executeSearch(tab: TabPool, task: SearchTask): Promise<void> {
    let bandwidthMetrics: BandwidthMetrics | null = null;

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

      tab.page.setDefaultNavigationTimeout(30000);

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

      if (!task.cancelled) {
        task.reject(error as Error);
      }
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
  ): void {
    metrics.endTime = Date.now();
    const duration = metrics.endTime - metrics.startTime;
    const totalKB = (metrics.totalBytes / 1024).toFixed(2);
    const totalMB = (metrics.totalBytes / (1024 * 1024)).toFixed(2);

    console.log(`🔍 Base Search Bandwidth Report - Task ${taskId}`);
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

  cancelAllRequests(): number {
    const cancelledCount = this.taskQueue.length;
    this.taskQueue.forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error("All Base requests cancelled"));
    });
    this.taskQueue = [];
    console.log(`Cancelled ${cancelledCount} pending Base requests`);
    return cancelledCount;
  }

  cancelRequestsByEngine(searchEngine: SearchEngine): number {
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
      busyTabs: this.tabPool.filter((tab) => tab.busy).length,
      maxTabs: this.maxTabs,
      maxQueueSize: this.maxQueueSize,
    };
  }

  async close(): Promise<void> {
    // Cancel all requests
    this.cancelAllRequests();

    // Close all tabs
    for (const tab of this.tabPool) {
      try {
        if (!tab.page.isClosed()) {
          await tab.page.close();
        }
      } catch (error) {
        console.error("Error closing Base tab during shutdown:", error);
      }
    }
    this.tabPool = [];

    console.log("Base context closed successfully");
  }
}
