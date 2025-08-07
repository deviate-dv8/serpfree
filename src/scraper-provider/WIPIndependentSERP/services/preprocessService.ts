import { Page } from "puppeteer";
import { SearchResult } from "../";

export default class PreprocessService {
  async processGoogleResults(page: Page): Promise<SearchResult[]> {
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

  async processBingResults(page: Page): Promise<SearchResult[]> {
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

  async processDuckDuckGoResults(page: Page): Promise<SearchResult[]> {
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

  async processYahooResults(page: Page): Promise<SearchResult[]> {
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
}
