import { connect, PageWithCursor } from "puppeteer-real-browser";
import { Browser, BrowserContext, Page } from "puppeteer";
import PreprocessService from "./services/preprocessService";
import { SERPContextBase } from "./SERPContext";

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
}

export interface TabPool {
  page: Page;
  busy: boolean;
  lastUsed: number;
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
  private googleContext: BrowserContext | null = null;
  private baseContext: SERPContextBase | null = null;
  constructor(baseContext: SERPContextBase) {
    this.baseContext = baseContext;
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
          "--disable-dev-shm-usage", // Avoid /dev/shm usage
          "--no-zygote", // Disable zygote process
          "--disable-software-rasterizer",
          "--disable-extensions",
          "--disable-background-networking",
          "--disable-renderer-backgrounding",
          "--memory-pressure-off",
          "--disable-notifications",
          // "--disable-background-timer-throttling",
          // "--disable-backgrounding-occluded-windows",
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

      try {
      } catch (error) {
        console.error("Error during initial search:", error);
        await this.closeBrowser();
        await this.launchBrowser();
      }
    } catch (error) {
      console.error("Error launching browser:", error);
    }
  }
  async initializeContext(serpContext: SERPContextBase) {
    try {
      if (!this.browser) {
        throw new Error("Browser is not initialized");
      }
      console.log("SERP context initialized successfully!");
    } catch (error) {
      console.error("Error initializing SERP context:", error);
    }
  }
  async closeBrowser() {
    try {
      // Cancel and clear the queue
      // const cancelledCount = this.cancelAllRequests();
      // console.log(
      //   `Cancelled ${cancelledCount} pending requests during browser close`,
      // );

      // Close all tabs
      // for (const tab of this.tabPool) {
      //   try {
      //     if (!tab.page.isClosed()) {
      //       await tab.page.close();
      //     }
      //   } catch (error) {
      //     console.error("Error closing tab:", error);
      //   }
      // }
      // this.tabPool = [];

      await this.browser?.close();
      this.browser = null;
      console.log("Browser closed successfully!");
    } catch (error) {
      console.error("Error closing browser:", error);
    }
  }
}
