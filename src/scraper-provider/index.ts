import { connect } from "puppeteer-real-browser";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import AnonymizeUAPlugin from "puppeteer-extra-plugin-anonymize-ua";
import { Browser, Page } from "puppeteer";

const stealth = StealthPlugin();

export enum SearchEngine {
  GOOGLE = "https://google.com",
  BING = "https://bing.com",
  DUCKDUCKGO = "https://duckduckgo.com",
  YAHOO = "https://search.yahoo.com",
}

interface SearchResult {
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
}

interface TabPool {
  page: Page;
  busy: boolean;
  lastUsed: number;
}

export default class SERPScraper {
  private browser: Browser | null = null;
  private tabPool: TabPool[] = [];
  private taskQueue: SearchTask[] = [];
  private maxTabs: number;
  private maxQueueSize: number;
  private processingQueue: boolean = false;
  private tabIdleTimeout: number = 5000;

  constructor(maxTabs: number = 1000, maxQueueSize: number = 1000) {
    this.maxTabs = maxTabs;
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
        ],
        disableXvfb: process.env.NODE_ENV === "development",
        plugins: [],
      });

      this.browser = browser as unknown as Browser;
      await page.setViewport({ width: 1280, height: 800 });

      // Initialize the first tab in the pool
      this.tabPool.push({
        page: page as unknown as Page,
        busy: false,
        lastUsed: Date.now(),
      });

      // Start idle tab cleanup
      this.startIdleTabCleanup();
    } catch (error) {
      console.error("Error launching browser:", error);
    }
  }

  private startIdleTabCleanup() {
    setInterval(async () => {
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
        await page.setViewport({ width: 1280, height: 800 });

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
    try {
      // Check if cancelled before starting
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
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

      // Final check before resolving
      if (task.cancelled || task.abortController.signal.aborted) {
        throw new Error("Request cancelled");
      }

      task.resolve(results);
    } catch (error) {
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

  private async processGoogleResults(page: Page): Promise<SearchResult[]> {
    const rsoDiv = await page.$("#rso");
    if (!rsoDiv) {
      const captcha = await page.$("form#captcha-form");
      if (captcha) {
        throw new Error("Captcha detected");
      }

      const searchDiv = await page.$("div#search");
      if (searchDiv) {
        throw new Error("No results found in Google search.");
      } else {
        await page.screenshot({ path: "google_search_error.png" });
        throw new Error(
          "Might be blocked by Google, try using a different search engine.",
        );
      }
    }

    return await page.evaluate(() => {
      const results: SearchResult[] = [];
      const resultElements = document.querySelectorAll("#rso div[data-rpos]");
      let rank = 1;

      resultElements.forEach((element) => {
        const titleElement = element.querySelector("h3");
        const linkElement = element.querySelector("a");
        const descriptionElement = element.querySelector(
          "div[style='-webkit-line-clamp:2']",
        );

        if (titleElement && linkElement && descriptionElement) {
          const title = titleElement.textContent?.trim() || "";
          const link = linkElement.getAttribute("href") || "";
          const description = descriptionElement.textContent?.trim() || "";

          if (title && link && description) {
            try {
              const domain = new URL(link).hostname;
              results.push({
                title,
                link,
                description,
                rank: rank++,
                domain,
              });
            } catch (e) {
              // Skip invalid URLs but don't increment rank
            }
          }
        }
      });

      return results;
    });
  }

  private async processBingResults(page: Page): Promise<SearchResult[]> {
    const bResults = await page.$("ol#b_results");
    if (!bResults) {
      await page.screenshot({ path: "bing_search_error.png" });
      throw new Error(
        "Might be blocked by Bing, try using a different search engine.",
      );
    }

    const noResults = await page.$("ol#b_results > li.b_no");
    if (noResults) {
      throw new Error("No results found in Bing search.");
    }

    return await page.evaluate(() => {
      const results: SearchResult[] = [];
      const resultElements = document.querySelectorAll("li.b_algo");
      let rank = 1;

      resultElements.forEach((element) => {
        const titleElement = element.querySelector("h2");
        const linkElement = element.querySelector("a.tilk");
        const descriptionElement = element.querySelector("p");

        if (titleElement && linkElement && descriptionElement) {
          const title = titleElement.textContent?.trim() || "";
          const link = linkElement.getAttribute("href") || "";
          const description = descriptionElement.textContent?.trim() || "";

          if (title && link && description) {
            try {
              const domain = new URL(link).hostname;
              results.push({
                title,
                link,
                description,
                rank: rank++,
                domain,
              });
            } catch (e) {
              // Skip invalid URLs but don't increment rank
            }
          }
        }
      });

      return results;
    });
  }

  private async processDuckDuckGoResults(page: Page): Promise<SearchResult[]> {
    const olElement = await page.$("ol.react-results--main");
    if (!olElement) {
      const boldElements = await page.$$("b");
      if (boldElements.length === 1) {
        throw new Error("No results found in DuckDuckGo search.");
      } else {
        await page.screenshot({ path: "duckduckgo_search_error.png" });
        throw new Error(
          "Might be blocked by DuckDuckGo, try using a different search engine.",
        );
      }
    }

    return await page.evaluate(() => {
      const results: SearchResult[] = [];
      const resultElements = document.querySelectorAll(
        "ol.react-results--main li[data-layout='organic']",
      );
      let rank = 1;

      resultElements.forEach((element) => {
        const titleElement = element.querySelector("h2");
        const linkElement = element.querySelector(
          "a[data-testid='result-extras-url-link']",
        );
        const descriptionElement = element.querySelector(
          "div[data-result='snippet'] span",
        );

        if (titleElement && linkElement && descriptionElement) {
          const title = titleElement.textContent?.trim() || "";
          const link = linkElement.getAttribute("href") || "";
          const description = descriptionElement.textContent?.trim() || "";

          if (title && link && description) {
            try {
              const domain = new URL(link).hostname;
              results.push({
                title,
                link,
                description,
                rank: rank++,
                domain,
              });
            } catch (e) {
              // Skip invalid URLs but don't increment rank
            }
          }
        }
      });

      return results;
    });
  }

  private async processYahooResults(page: Page): Promise<SearchResult[]> {
    const regSearchCenterMiddle = await page.$("ol.searchCenterMiddle");
    if (!regSearchCenterMiddle) {
      const noResults = await page.$("ol.adultRegion");
      if (noResults) {
        throw new Error("No results found in Yahoo search.");
      } else {
        await page.screenshot({ path: "yahoo_search_error.png" });
        throw new Error(
          "Might be blocked by Yahoo, try using a different search engine.",
        );
      }
    }

    return await page.evaluate(() => {
      const results: SearchResult[] = [];
      const resultElements = document.querySelectorAll(
        "ol.searchCenterMiddle li",
      );
      let rank = 1;

      resultElements.forEach((element) => {
        const titleElement = element.querySelector("h3");
        const linkElement = element.querySelector(
          "div.compTitle > a:first-child",
        );
        const descriptionElement = element.querySelector("div.compText");

        if (titleElement && linkElement && descriptionElement) {
          const title = titleElement.textContent?.trim() || "";
          const link = linkElement.getAttribute("href") || "";
          const description = descriptionElement.textContent?.trim() || "";

          if (title && link && description) {
            try {
              const domain = new URL(link).hostname;
              results.push({
                title,
                link,
                description,
                rank: rank++,
                domain,
              });
            } catch (e) {
              // Skip invalid URLs but don't increment rank
            }
          }
        }
      });

      return results;
    });
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
