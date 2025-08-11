import { Page } from "puppeteer";
import { connect } from "puppeteer-real-browser";
import { Browser } from "puppeteer";
import { BaseSERPContext } from "./BaseSERPContext";

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

export interface SearchTask {
  query: string;
  searchEngine: SearchEngine;
  resolve: (results: SearchResult[]) => void;
  reject: (error: Error) => void;
  id: string;
  abortController: AbortController;
  cancelled: boolean;
  retries: number;
}

export interface TabPool {
  page: Page;
  busy: boolean;
  lastUsed: number;
  contextType: "default" | "google";
}

export interface BandwidthMetrics {
  totalBytes: number;
  requestCount: number;
  responseCount: number;
  blockedRequests: number;
  startTime: number;
  endTime?: number;
}

export default class SERPScraper {
  private browser: Browser | null = null;
  private baseContext: BaseSERPContext | null = null;
  private maxTabs: number;
  private maxQueueSize: number;
  public ready: boolean = false;
  private cleanUpIntervalId: NodeJS.Timeout | null = null;

  constructor(maxTabs: number = 1000, maxQueueSize: number = 1000) {
    this.maxTabs = Math.max(1, maxTabs);
    this.maxQueueSize = maxQueueSize;
    this.launchBrowser();
  }

  private async launchBrowser(): Promise<void> {
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

      // Initialize base context
      this.baseContext = new BaseSERPContext(
        this.browser,
        this.maxTabs,
        this.maxQueueSize,
      );

      const baseInitialized = await this.baseContext.initialize();

      if (!baseInitialized) {
        throw new Error("Failed to initialize base context");
      }

      // Close the initial page
      await page.close();

      // Test search functionality
      await this.testSearchFunctionality();

      this.startIdleTabCleanup();
      this.ready = true;
      console.log("SERP Scraper is ready for use.");
    } catch (error) {
      console.error("Error launching browser:", error);
      this.ready = false;
      await this.closeBrowser();
      // Retry launching
      setTimeout(() => this.launchBrowser(), 5000);
    }
  }

  private async testSearchFunctionality(): Promise<void> {
    try {
      if (!this.baseContext) {
        throw new Error("Base context not initialized");
      }

      const { promise } = await this.baseContext.search(
        "test search",
        SearchEngine.BING, // Use Bing for testing to avoid Google complications
      );
      await promise;
      console.log("Search functionality test passed");
    } catch (error) {
      console.error("Search functionality test failed:", error);
      throw error;
    }
  }

  private startIdleTabCleanup(): void {
    this.cleanUpIntervalId = setInterval(() => {
      if (this.baseContext) {
        this.baseContext.cleanupIdleTabs();
      }
    }, 60000); // Run cleanup every minute
  }

  async search(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
    if (!this.browser || !this.baseContext) {
      await this.launchBrowser();
      return this.search(query, searchEngine);
    }

    if (!this.ready) {
      throw new Error("SERP Scraper is not ready yet");
    }

    return this.baseContext.search(query, searchEngine);
  }

  async searchSimple(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<SearchResult[]> {
    const { promise } = await this.search(query, searchEngine);
    return promise;
  }

  cancelAllRequests(): number {
    if (!this.baseContext) return 0;
    return this.baseContext.cancelAllRequests();
  }

  cancelRequestsByEngine(searchEngine: SearchEngine): number {
    if (!this.baseContext) return 0;
    return this.baseContext.cancelRequestsByEngine(searchEngine);
  }

  getStatus() {
    if (!this.baseContext) {
      return {
        ready: this.ready,
        browserActive: !!this.browser,
        baseContextActive: false,
      };
    }

    return {
      ready: this.ready,
      browserActive: !!this.browser,
      baseContextActive: true,
      ...this.baseContext.getStatus(),
    };
  }

  async closeBrowser(): Promise<void> {
    if (this.cleanUpIntervalId) {
      clearInterval(this.cleanUpIntervalId);
      this.cleanUpIntervalId = null;
    }

    try {
      let cancelledCount = 0;

      if (this.baseContext) {
        cancelledCount += this.baseContext.cancelAllRequests();
        await this.baseContext.close();
        this.baseContext = null;
      }

      console.log(`Cancelled ${cancelledCount} requests during browser close`);

      if (this.browser) {
        await this.browser.close();
        this.browser = null;
      }

      this.ready = false;
      console.log("Browser closed successfully");
    } catch (error) {
      console.error("Error closing browser:", error);
    }
  }
}
