import { Browser, BrowserContext, Page } from "puppeteer";
import {
  BandwidthMetrics,
  SearchEngine,
  SearchResult,
  SearchTask,
  TabPool,
} from ".";
import PreprocessService from "./services/preprocessService";

export class ChromeSERPContext extends PreprocessService {
  private browser: Browser;
  private incognitoContext: BrowserContext | null = null;
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
    this.maxTabs = Math.max(1, maxTabs);
    this.maxQueueSize = maxQueueSize;
  }

  async initialize(): Promise<boolean> {
    try {
      // Isolated incognito context for Google
      this.incognitoContext = await this.browser.createBrowserContext();

      const page = await this.incognitoContext.newPage();

      if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
        await this.setupRequestBlocking(page);
      }

      // Optionally warm up Google (can be commented out if undesired)
      try {
        await page.goto("https://www.google.com", {
          waitUntil: "load",
          timeout: 20000,
        });
      } catch {
        // Best-effort warmup; do not fail initialization
      }

      this.tabPool.push({
        page,
        busy: false,
        lastUsed: Date.now(),
        contextType: "google",
      });

      console.log("Chrome SERP context initialized successfully");
      this.initComplete = true;
      return true;
    } catch (error) {
      console.error("Failed to initialize Chrome context:", error);
      return false;
    }
  }

  private async getAvailableTab(): Promise<TabPool> {
    const availableTab = this.tabPool.find((tab) => !tab.busy);
    if (availableTab) return availableTab;

    if (this.tabPool.length < this.maxTabs) {
      try {
        if (!this.incognitoContext)
          throw new Error("Incognito context not initialized");
        const page = await this.incognitoContext.newPage();

        if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
          await this.setupRequestBlocking(page);
        }

        const newTab: TabPool = {
          page,
          busy: false,
          lastUsed: Date.now(),
          contextType: "google",
        };

        this.tabPool.push(newTab);
        console.log(`Created new Chrome tab. Total: ${this.tabPool.length}`);
        return newTab;
      } catch (error) {
        console.error("Error creating new Chrome tab:", error);
        throw error;
      }
    }

    return new Promise((resolve) => {
      const checkAvailability = () => {
        const tab = this.tabPool.find((t) => !t.busy);
        if (tab) resolve(tab);
        else setTimeout(checkAvailability, 100);
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
      if (t === "document") request.continue();
      else if (t === "script" && !isTracker.test(request.url()))
        request.continue();
      else request.abort();
    });
  }

  cleanupIdleTabs(): void {
    const now = Date.now();
    const idleTabs = this.tabPool.filter(
      (tab) => !tab.busy && now - tab.lastUsed > this.tabIdleTimeout,
    );

    if (this.tabPool.length > 1 && idleTabs.length > 0) {
      const tabsToClose = idleTabs.slice(0, idleTabs.length - 1);

      tabsToClose.forEach(async (tab) => {
        try {
          if (!tab.page.isClosed()) await tab.page.close();
          this.tabPool = this.tabPool.filter((t) => t !== tab);
        } catch (error) {
          console.error("Error closing idle Chrome tab:", error);
        }
      });

      console.log(
        `Closed ${tabsToClose.length} idle Chrome tabs. Remaining: ${this.tabPool.length}`,
      );
    }
  }

  async search(
    query: string,
    searchEngine: SearchEngine,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
    if (!this.initComplete) {
      throw new Error("Chrome context not fully initialized");
    }

    if (searchEngine !== SearchEngine.GOOGLE) {
      throw new Error("ChromeSERPContext only handles Google searches");
    }

    if (this.taskQueue.length >= this.maxQueueSize) {
      throw new Error(
        `Chrome queue is full. Maximum size: ${this.maxQueueSize}`,
      );
    }

    const abortController = new AbortController();
    const taskId = `chrome_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

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
        `Added Chrome task ${taskId} to queue. Queue: ${this.taskQueue.length}`,
      );
      this.processQueue();
    });

    const cancel = () => {
      const task = this.taskQueue.find((t) => t.id === taskId);
      if (task) {
        task.cancelled = true;
        task.abortController.abort();
        this.taskQueue = this.taskQueue.filter((t) => t !== task);
        console.log(`Cancelled Chrome task ${taskId}`);
      }
    };

    return { promise: searchPromise, cancel };
  }

  async searchSimple(query: string): Promise<SearchResult[]> {
    const { promise } = await this.search(query, SearchEngine.GOOGLE);
    return promise;
  }

  private async processQueue(): Promise<void> {
    if (this.processingQueue || this.taskQueue.length === 0) return;
    this.processingQueue = true;

    while (this.taskQueue.length > 0) {
      const validTasks = this.taskQueue.filter((task) => !task.cancelled);
      const cancelledTasks = this.taskQueue.filter((task) => task.cancelled);

      cancelledTasks.forEach((task) =>
        task.reject(new Error("Request cancelled")),
      );
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
          `Processing Chrome task ${task.id}. Queue: ${this.taskQueue.length}`,
        );

        this.executeSearch(tab, task).finally(() => {
          tab.busy = false;
          tab.lastUsed = Date.now();
        });
      } catch (error) {
        if (!task.cancelled) task.reject(error as Error);
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

      // Longer timeout for Google
      tab.page.setDefaultNavigationTimeout(60000);

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
      if (!task.cancelled) task.reject(error as Error);
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
        if (postData) metrics.totalBytes += Buffer.byteLength(postData, "utf8");
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
              if (response.ok()) metrics.totalBytes += 1024;
            }
          }
        } catch {}
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

    console.log(`🔍 Chrome Search Bandwidth Report - Task ${taskId}`);
    console.log(`   Query: "${query}"`);
    console.log(`   Engine: ${searchEngine}`);
    console.log(`   Duration: ${duration}ms`);
    console.log(`   Total Bandwidth: ${totalKB} KB (${totalMB} MB)`);
    console.log(`   Total Bytes: ${metrics.totalBytes.toLocaleString()} bytes`);
    console.log(`   Requests: ${metrics.requestCount}`);
    console.log(`   Responses: ${metrics.responseCount}`);
    console.log(`   Blocked Requests: ${metrics.blockedRequests}`);
    console.log(`   ────────────────────────────────────────────────────`);
  }

  cancelAllRequests(): number {
    const cancelledCount = this.taskQueue.length;
    this.taskQueue.forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error("All Chrome requests cancelled"));
    });
    this.taskQueue = [];
    console.log(`Cancelled ${cancelledCount} pending Chrome requests`);
    return cancelledCount;
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
        if (!tab.page.isClosed()) await tab.page.close();
      } catch (error) {
        console.error("Error closing Chrome tab during shutdown:", error);
      }
    }
    this.tabPool = [];

    // Close incognito context
    try {
      await this.incognitoContext?.close();
    } catch (error) {
      console.error("Error closing Chrome incognito context:", error);
    } finally {
      this.incognitoContext = null;
    }

    console.log("Chrome context closed successfully");
  }
}
