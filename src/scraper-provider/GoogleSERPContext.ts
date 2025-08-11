import { Browser, BrowserContext, Page } from "puppeteer";
import {
  BandwidthMetrics,
  SearchEngine,
  SearchResult,
  SearchTask,
  TabPool,
} from ".";
import PreprocessService from "./services/preprocessService";

const MAX_RETRIES = 2;

export class GoogleSERPContext extends PreprocessService {
  private browser: Browser;
  private context: BrowserContext | null = null;
  private tabPool: TabPool[] = [];
  private taskQueue: SearchTask[] = [];
  private maxTabs: number;
  private maxQueueSize: number;
  private processingQueue: boolean = false;
  private contextResetInProgress: boolean = false;
  private pendingRetryTasks: SearchTask[] = [];
  private initComplete: boolean = false;

  constructor(browser: Browser, maxTabs: number, maxQueueSize: number) {
    super();
    this.browser = browser;
    this.maxTabs = maxTabs;
    this.maxQueueSize = maxQueueSize;
  }

  async initialize() {
    try {
      // Create a browser context specifically for Google searches
      this.context = await this.browser.createBrowserContext();

      // Create an initial tab that we'll keep around for the context lifetime
      const page = await this.context.newPage();

      // Navigate to Google homepage to set up cookies
      await page.goto("https://www.google.com", {
        waitUntil: "networkidle2",
        timeout: 30000,
      });

      // Set up resource blocking if enabled
      if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
        await this.setupRequestBlocking(page);
      }

      // Add to tab pool
      this.tabPool.push({
        page,
        busy: false,
        lastUsed: Date.now(),
        contextType: "google",
      });

      console.log("Google SERP context initialized successfully");
      this.initComplete = true;
      return true;
    } catch (error) {
      console.error("Failed to initialize Google context:", error);
      return false;
    }
  }

  async resetContext() {
    if (this.contextResetInProgress) return;
    this.contextResetInProgress = true;

    try {
      console.log("Resetting Google context...");

      // Close all Google tabs
      for (const tab of this.tabPool) {
        try {
          if (!tab.page.isClosed()) {
            await tab.page.close();
          }
        } catch (error) {
          console.error("Error closing Google tab:", error);
        }
      }
      this.tabPool = [];

      // Close and recreate context
      if (this.context) {
        await this.context.close();
      }
      this.context = await this.browser.createBrowserContext();

      // Create new initial tab
      const page = await this.context.newPage();
      await page.goto("https://www.google.com", {
        waitUntil: "networkidle2",
        timeout: 30000,
      });

      if (process.env.ENABLE_RESOURCE_BLOCKING === "true") {
        await this.setupRequestBlocking(page);
      }

      this.tabPool.push({
        page,
        busy: false,
        lastUsed: Date.now(),
        contextType: "google",
      });

      // Queue pending tasks for retry
      const pendingTasks = [...this.pendingRetryTasks];
      this.pendingRetryTasks = [];

      for (const task of pendingTasks) {
        if (!task.cancelled) {
          this.taskQueue.unshift(task); // Add to front of queue for priority
        }
      }

      console.log("Google context reset complete");
    } catch (error) {
      console.error("Error resetting Google context:", error);
    } finally {
      this.contextResetInProgress = false;
      this.processQueue(); // Resume processing
    }
  }

  async getAvailableTab(): Promise<TabPool> {
    if (!this.initComplete) {
      throw new Error("Google context not initialized");
    }

    // Look for an available tab first
    const availableTab = this.tabPool.find((tab) => !tab.busy);

    if (availableTab) {
      return availableTab;
    }

    // Create a new tab if under the limit
    if (this.tabPool.length < this.maxTabs) {
      if (!this.context) {
        throw new Error("Google context not initialized");
      }

      try {
        const page = await this.context.newPage();

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
        console.log(`Created new Google tab. Total: ${this.tabPool.length}`);
        return newTab;
      } catch (error) {
        console.error("Error creating new Google tab:", error);
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

  private async setupRequestBlocking(page: Page) {
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

  cleanupIdleTabs() {
    const now = Date.now();
    const idleTabs = this.tabPool.filter(
      (tab) => !tab.busy && now - tab.lastUsed > 60000,
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
          console.error("Error closing idle Google tab:", error);
        }
      });

      console.log(
        `Closed ${tabsToClose.length} idle Google tabs. Remaining: ${this.tabPool.length}`,
      );
    }
  }

  async search(
    query: string,
    searchEngine: SearchEngine,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
    if (!this.initComplete) {
      throw new Error("Google context not fully initialized");
    }

    if (this.taskQueue.length >= this.maxQueueSize) {
      throw new Error(
        `Google queue is full. Maximum size: ${this.maxQueueSize}`,
      );
    }

    const abortController = new AbortController();
    const taskId = `google_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

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
        `Added Google task ${taskId} to queue. Queue: ${this.taskQueue.length}`,
      );
      this.processQueue();
    });

    const cancel = () => {
      const task = this.taskQueue.find((t) => t.id === taskId);
      if (task) {
        task.cancelled = true;
        task.abortController.abort();
        console.log(`Cancelled Google task ${taskId}`);
      }
    };

    return { promise: searchPromise, cancel };
  }

  private async processQueue() {
    if (
      this.processingQueue ||
      this.taskQueue.length === 0 ||
      this.contextResetInProgress
    ) {
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
          `Processing Google task ${task.id}. Queue: ${this.taskQueue.length}`,
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

      tab.page.setDefaultNavigationTimeout(60000); // Google search can take longer

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
      if (captchaDetected) {
        console.log(`Captcha detected in Google task ${task.id}`);
        throw new Error("Google captcha detected");
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

      if (captchaDetected) {
        // Handle captcha by retrying after context reset
        if (task.retries < MAX_RETRIES) {
          task.retries++;
          this.pendingRetryTasks.push(task);
          console.log(
            `Queued Google task ${task.id} for retry (attempt ${task.retries})`,
          );

          // Trigger context reset if not already in progress
          if (!this.contextResetInProgress) {
            this.resetContext();
          }
        } else {
          task.reject(new Error("Max retries reached for Google search"));
        }
      } else if (!task.cancelled) {
        task.reject(error as Error);
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

    console.log(`🔍 Google Search Bandwidth Report - Task ${taskId}`);
    console.log(`   Query: "${query}"`);
    console.log(`   Duration: ${duration}ms`);
    console.log(`   Total Bandwidth: ${totalKB} KB (${totalMB} MB)`);
    console.log(`   Total Bytes: ${metrics.totalBytes.toLocaleString()} bytes`);
    console.log(`   Requests: ${metrics.requestCount}`);
    console.log(`   Responses: ${metrics.responseCount}`);
    console.log(`   Blocked Requests: ${metrics.blockedRequests}`);
    console.log(`   ────────────────────────────────────────────────────`);
  }

  cancelAllRequests() {
    const cancelledCount = this.taskQueue.length;
    this.taskQueue.forEach((task) => {
      task.cancelled = true;
      task.abortController.abort();
      task.reject(new Error("All Google requests cancelled"));
    });
    this.taskQueue = [];
    this.pendingRetryTasks = [];
    console.log(`Cancelled ${cancelledCount} pending Google requests`);
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
      pendingRetries: this.pendingRetryTasks.length,
      contextActive: !!this.context,
      contextResetInProgress: this.contextResetInProgress,
    };
  }

  async close() {
    // Cancel all requests
    this.cancelAllRequests();

    // Close all tabs
    for (const tab of this.tabPool) {
      try {
        if (!tab.page.isClosed()) {
          await tab.page.close();
        }
      } catch (error) {
        console.error("Error closing Google tab during shutdown:", error);
      }
    }
    this.tabPool = [];

    // Close the context
    if (this.context) {
      try {
        await this.context.close();
        this.context = null;
      } catch (error) {
        console.error("Error closing Google context:", error);
      }
    }

    console.log("Google context closed successfully");
  }
}
