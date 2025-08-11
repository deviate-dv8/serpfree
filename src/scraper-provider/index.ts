import { Page } from "puppeteer";
import { connect } from "puppeteer-real-browser";
import { Browser } from "puppeteer";
import { BaseSERPContext } from "./BaseSERPContext";
import { ChromeSERPContext } from "./GoogleSERPContext";

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
  private chromeContext: ChromeSERPContext | null = null;

  private maxTabs: number;
  private maxQueueSize: number;
  public ready: boolean = false;
  private cleanUpIntervalId: NodeJS.Timeout | null = null;

  // Env switches
  private enableChromeContext: boolean =
    process.env.ENABLE_CHROME_CONTEXT === "true";
  private onlyGoogle: boolean = process.env.SERP_ONLY_GOOGLE === "true";
  private onlyBase: boolean = process.env.SERP_ONLY_BASE === "true";

  constructor(maxTabs: number = 1000, maxQueueSize: number = 1000) {
    this.maxTabs = Math.max(1, maxTabs);
    this.maxQueueSize = maxQueueSize;
    this.validateEnv();
    this.launchBrowser();
  }

  private validateEnv() {
    if (this.onlyGoogle && this.onlyBase) {
      throw new Error(
        "Configuration error: both SERP_ONLY_GOOGLE and SERP_ONLY_BASE are set. Use only one.",
      );
    }

    // If onlyGoogle is requested, force enable chrome context
    if (this.onlyGoogle && !this.enableChromeContext) {
      console.warn(
        "SERP_ONLY_GOOGLE is true but ENABLE_CHROME_CONTEXT is not. Enabling Chrome context.",
      );
      this.enableChromeContext = true;
    }

    // If onlyBase is requested, ensure chrome context disabled
    if (this.onlyBase && this.enableChromeContext) {
      console.warn(
        "SERP_ONLY_BASE is true; Chrome context will not be initialized despite ENABLE_CHROME_CONTEXT.",
      );
    }
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

      // Initialize contexts based on env
      await this.initializeContexts();

      // Close the initial page opened by connect()
      try {
        await page.close();
      } catch {}

      // Test enabled contexts
      await this.testEnabledContexts();

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

  private async initializeContexts() {
    if (!this.browser) throw new Error("Browser not initialized");

    // Base context
    if (!this.onlyGoogle) {
      this.baseContext = new BaseSERPContext(
        this.browser,
        this.maxTabs,
        this.maxQueueSize,
      );
      const baseOk = await this.baseContext.initialize();
      if (!baseOk) throw new Error("Failed to initialize base context");
    } else {
      this.baseContext = null;
    }

    // Chrome (Google) context
    if (this.enableChromeContext && !this.onlyBase) {
      this.chromeContext = new ChromeSERPContext(
        this.browser,
        Math.max(1, Math.floor(this.maxTabs / 2)), // split tabs if both contexts on
        this.maxQueueSize,
      );
      const chromeOk = await this.chromeContext.initialize();
      if (!chromeOk) throw new Error("Failed to initialize chrome context");
    } else {
      this.chromeContext = null;
    }
  }

  private async testEnabledContexts(): Promise<void> {
    // Keep tests simple and fast; do not block readiness for long
    const tests: Promise<any>[] = [];

    if (this.baseContext) {
      const { promise } = await this.baseContext.search(
        "ping",
        SearchEngine.BING,
      );
      tests.push(
        promise.catch((e) => console.warn("Base test failed:", e.message)),
      );
    }

    if (this.chromeContext) {
      const { promise } = await this.chromeContext.search(
        "ping",
        SearchEngine.GOOGLE,
      );
      tests.push(
        promise.catch((e) => console.warn("Chrome test failed:", e.message)),
      );
    }

    await Promise.allSettled(tests);
  }

  private startIdleTabCleanup(): void {
    this.cleanUpIntervalId = setInterval(() => {
      if (this.baseContext) this.baseContext.cleanupIdleTabs();
      if (this.chromeContext) this.chromeContext.cleanupIdleTabs();
    }, 60000); // Run cleanup every minute
  }

  async search(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
    if (!this.browser) {
      await this.launchBrowser();
      return this.search(query, searchEngine);
    }
    if (!this.ready) {
      throw new Error("SERP Scraper is not ready yet");
    }

    // Enforce "only" modes
    if (this.onlyGoogle && searchEngine !== SearchEngine.GOOGLE) {
      throw new Error("Only Google searches are enabled by configuration");
    }
    if (this.onlyBase && searchEngine === SearchEngine.GOOGLE) {
      // If only base is enabled and someone requests Google, run via base if available
      if (!this.baseContext) {
        throw new Error("Base context is not available");
      }
      return this.baseContext.search(query, searchEngine);
    }

    // Route: Google -> Chrome context if enabled, otherwise base
    if (searchEngine === SearchEngine.GOOGLE && this.chromeContext) {
      return this.chromeContext.search(query, searchEngine);
    }

    if (!this.baseContext) {
      throw new Error("Base context is not available");
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
    let cancelled = 0;
    if (this.baseContext) cancelled += this.baseContext.cancelAllRequests();
    if (this.chromeContext) cancelled += this.chromeContext.cancelAllRequests();
    return cancelled;
  }

  getStatus() {
    const baseStatus = this.baseContext ? this.baseContext.getStatus() : null;
    const chromeStatus = this.chromeContext
      ? this.chromeContext.getStatus()
      : null;

    return {
      ready: this.ready,
      browserActive: !!this.browser,
      enableChromeContext: this.enableChromeContext,
      onlyGoogle: this.onlyGoogle,
      onlyBase: this.onlyBase,
      baseContextActive: !!this.baseContext,
      chromeContextActive: !!this.chromeContext,
      base: baseStatus,
      chrome: chromeStatus,
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

      if (this.chromeContext) {
        cancelledCount += this.chromeContext.cancelAllRequests();
        await this.chromeContext.close();
        this.chromeContext = null;
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
