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

  private baseReady = false;
  private chromeReady = false;
  private baseReadyPromise: Promise<void> | null = null;
  private chromeReadyPromise: Promise<void> | null = null;

  private maxTabs: number;
  private maxQueueSize: number;
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
    if (this.onlyGoogle && !this.enableChromeContext) {
      console.warn("SERP_ONLY_GOOGLE=true implies enabling Chrome context.");
      this.enableChromeContext = true;
    }
    if (this.onlyBase && this.enableChromeContext) {
      console.warn(
        "SERP_ONLY_BASE=true: Chrome context will not be initialized even if ENABLE_CHROME_CONTEXT is true.",
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

      await this.initializeContexts();

      try {
        await page.close();
      } catch {}

      // Start readiness test loops independently
      if (this.baseContext) {
        this.baseReadyPromise = this.runBaseTestLoop();
        // Do not await chrome readiness here; base can be usable earlier
      }
      if (this.chromeContext) {
        this.chromeReadyPromise = this.runChromeTestLoop();
      }

      this.startIdleTabCleanup();
      console.log("SERP Scraper launched.");
    } catch (error) {
      console.error("Error launching browser:", error);
      await this.closeBrowser();
      setTimeout(() => this.launchBrowser(), 5000);
    }
  }

  private async initializeContexts() {
    if (!this.browser) throw new Error("Browser not initialized");

    // Base context
    if (!this.onlyGoogle) {
      this.baseContext = new BaseSERPContext(
        this.browser,
        this.enableChromeContext
          ? Math.max(1, Math.floor(this.maxTabs / 2))
          : this.maxTabs,
        this.maxQueueSize,
      );
      const baseOk = await this.baseContext.initialize();
      if (!baseOk) throw new Error("Failed to initialize base context");
    } else {
      this.baseContext = null;
      this.baseReady = false;
      this.baseReadyPromise = null;
    }

    // Chrome (Google) context
    if (this.enableChromeContext && !this.onlyBase) {
      this.chromeContext = new ChromeSERPContext(
        this.browser,
        this.baseContext
          ? Math.max(1, Math.floor(this.maxTabs / 2))
          : this.maxTabs,
        this.maxQueueSize,
      );
      const chromeOk = await this.chromeContext.initialize();
      if (!chromeOk) {
        console.warn(
          "Failed to initialize Chrome context. Readiness loop will recreate it.",
        );
      }
    } else {
      this.chromeContext = null;
      this.chromeReady = false;
      this.chromeReadyPromise = null;
    }
  }

  // Recreate and test Base context until test search succeeds
  private async runBaseTestLoop(): Promise<void> {
    while (true) {
      try {
        if (!this.baseContext) {
          // Create new base context if missing (shouldn't happen unless set to onlyGoogle, but keep safe)
          if (!this.browser) throw new Error("Browser not available");
          this.baseContext = new BaseSERPContext(
            this.browser,
            this.maxTabs,
            this.maxQueueSize,
          );
          await this.baseContext.initialize();
        }

        // Use Bing for base test to avoid Google-specific flows
        const { promise } = await this.baseContext.search(
          "ping",
          SearchEngine.BING,
        );
        await promise;

        this.baseReady = true;
        console.log("Base context search test passed.");
        return;
      } catch (err) {
        this.baseReady = false;
        console.warn(
          "Base context search test failed. Recreating base context...",
        );
        try {
          await this.baseContext?.close();
        } catch {}
        this.baseContext = null;

        if (!this.browser) throw new Error("Browser not available");
        this.baseContext = new BaseSERPContext(
          this.browser,
          this.enableChromeContext
            ? Math.max(1, Math.floor(this.maxTabs / 2))
            : this.maxTabs,
          this.maxQueueSize,
        );
        await this.baseContext.initialize();
        // Loop continues to retry immediately
      }
    }
  }

  // Recreate and test Chrome (Google) context until test search succeeds
  private async runChromeTestLoop(): Promise<void> {
    while (true) {
      try {
        if (!this.chromeContext) {
          if (!this.browser) throw new Error("Browser not available");
          this.chromeContext = new ChromeSERPContext(
            this.browser,
            this.baseContext
              ? Math.max(1, Math.floor(this.maxTabs / 2))
              : this.maxTabs,
            this.maxQueueSize,
          );
          await this.chromeContext.initialize();
        }

        const { promise } = await this.chromeContext.search(
          "ping",
          SearchEngine.GOOGLE,
        );
        await promise;

        this.chromeReady = true;
        console.log("Chrome context search test passed.");
        return;
      } catch (err) {
        this.chromeReady = false;
        console.warn(
          "Chrome context search test failed. Recreating Chrome context...",
        );
        try {
          await this.chromeContext?.close();
        } catch {}
        this.chromeContext = null;

        if (!this.browser) throw new Error("Browser not available");
        this.chromeContext = new ChromeSERPContext(
          this.browser,
          this.baseContext
            ? Math.max(1, Math.floor(this.maxTabs / 2))
            : this.maxTabs,
          this.maxQueueSize,
        );
        await this.chromeContext.initialize();
        // Loop continues to retry immediately
      }
    }
  }

  private startIdleTabCleanup(): void {
    this.cleanUpIntervalId = setInterval(() => {
      if (this.baseContext) this.baseContext.cleanupIdleTabs();
      if (this.chromeContext) this.chromeContext.cleanupIdleTabs();
    }, 60000);
  }

  async search(
    query: string,
    searchEngine: SearchEngine = SearchEngine.GOOGLE,
  ): Promise<{ promise: Promise<SearchResult[]>; cancel: () => void }> {
    if (!this.browser) {
      await this.launchBrowser();
      return this.search(query, searchEngine);
    }

    // Enforce "only" modes
    if (this.onlyGoogle && searchEngine !== SearchEngine.GOOGLE) {
      throw new Error("Only Google searches are enabled by configuration");
    }
    if (this.onlyBase && searchEngine === SearchEngine.GOOGLE) {
      if (!this.baseContext) throw new Error("Base context is not available");
      // Await base readiness if needed
      if (!this.baseReady && this.baseReadyPromise) {
        await this.baseReadyPromise;
      }
      return this.baseContext.search(query, searchEngine);
    }

    // Route: Google -> Chrome context, wait for Chrome readiness
    if (searchEngine === SearchEngine.GOOGLE && this.chromeContext) {
      if (!this.chromeReady && this.chromeReadyPromise) {
        await this.chromeReadyPromise;
      }
      return this.chromeContext.search(query, searchEngine);
    }

    // Route others to Base, wait for Base readiness
    if (this.baseContext) {
      if (!this.baseReady && this.baseReadyPromise) {
        await this.baseReadyPromise;
      }
      return this.baseContext.search(query, searchEngine);
    }

    throw new Error("No suitable context available for the requested search");
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
      browserActive: !!this.browser,
      enableChromeContext: this.enableChromeContext,
      onlyGoogle: this.onlyGoogle,
      onlyBase: this.onlyBase,
      baseReady: this.baseReady,
      chromeReady: this.chromeReady,
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

      this.baseReady = false;
      this.chromeReady = false;
      this.baseReadyPromise = null;
      this.chromeReadyPromise = null;

      console.log("Browser closed successfully");
    } catch (error) {
      console.error("Error closing browser:", error);
    }
  }
}
