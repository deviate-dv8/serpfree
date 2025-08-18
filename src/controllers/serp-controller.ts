import { Request, Response, Router } from "express";
import z from "zod";
import { scraper } from "../";
import { SearchEngine } from "../scraper-provider";
import dotenv from "dotenv";

dotenv.config();

const SearchSchema = z.object({
  query: z.string().min(1, "Query must not be empty"),
  provider: z.enum(["google", "bing", "yahoo", "duckduckgo"], {
    message: "Provider must be one of google, bing, duckduckgo or yahoo",
  }),
});

// Helper function to map provider string to enum
function getSearchEngine(provider: string): SearchEngine {
  switch (provider) {
    case "google":
      return SearchEngine.GOOGLE;
    case "bing":
      return SearchEngine.BING;
    case "duckduckgo":
      return SearchEngine.DUCKDUCKGO;
    case "yahoo":
      return SearchEngine.YAHOO;
    default:
      throw new Error("Invalid provider");
  }
}

// Main search function with automatic cancellation
async function search(req: Request, res: Response) {
  // Validate request
  const parsed = SearchSchema.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({
      message: "Invalid request",
      errors: parsed.error.issues,
    });
  }

  // Check scraper availability
  if (!scraper) {
    return res.status(500).json({ message: "Scraper not initialized" });
  }
  if (!scraper.isReady()) {
    return res.status(503).json({ message: "Scraper is not ready" });
  }
  const { query, provider } = parsed.data;
  const requestId = `${Date.now()}_${Math.random().toString(36).substr(2, 5)}`;

  console.log(
    `[${requestId}] Starting search for query: "${query}" with provider: ${provider}`,
  );

  try {
    // Get search engine enum
    const searchEngine = getSearchEngine(provider);

    // Start cancellable search
    const { promise, cancel } = await scraper.search(query, searchEngine);

    let isCancelled = false;

    // Setup automatic cancellation on client disconnect
    const handleCancel = (event: string) => {
      if (!isCancelled) {
        isCancelled = true;
        console.log(
          `[${requestId}] Client ${event}, cancelling search for query: "${query}"`,
        );
        cancel();
      }
    };

    // Listen for ALL possible client disconnect events
    req.on("close", () => handleCancel("disconnected"));
    req.on("aborted", () => handleCancel("aborted"));
    req.on("error", () => handleCancel("errored"));

    // Also monitor if response is finished/destroyed
    res.on("close", () => handleCancel("response closed"));
    res.on("finish", () =>
      console.log(`[${requestId}] Response finished normally`),
    );

    console.log(
      `[${requestId}] Request events attached, waiting for results...`,
    );

    // Wait for search results
    const results = await promise;

    if (!isCancelled) {
      console.log(
        `[${requestId}] Search completed successfully with ${results.length} results`,
      );
      return res.json({
        success: true,
        provider: provider,
        results,
        requestId,
      });
    }
  } catch (error) {
    console.error(`[${requestId}] Search error:`, error);

    // Handle different error types
    if (error instanceof Error) {
      if (error.message === "Request cancelled") {
        console.log(`[${requestId}] Request was cancelled`);
        return res.status(499).json({
          success: false,
          message: "Request cancelled",
          requestId,
        });
      }

      if (error.message === "Invalid provider") {
        return res.status(400).json({
          success: false,
          message: "Invalid provider",
          requestId,
        });
      }
    }

    return res.status(500).json({
      success: false,
      message: "Search failed",
      query,
      error: error instanceof Error ? error.message : String(error),
      requestId,
    });
  }
}
async function testSearch(req: Request, res: Response) {
  return res.json({
    success: true,
    provider: "google",
    query: "Node.js frameworks",
    results: [
      {
        title: "The 5 Most Popular Node.js Web Frameworks in 2025",
        link: "https://dev.to/leapcell/the-5-most-popular-nodejs-web-frameworks-in-2025-12po",
        description:
          "23 Mar 2025 — This article will introduce the top 5 Node.js backend frameworks in 2025, their features, and common use cases.",
        rank: 1,
        domain: "dev.to",
      },
      {
        title: "Which framework in Node.js is most commonly used ...",
        link: "https://www.reddit.com/r/node/comments/1gdcp89/which_framework_in_nodejs_is_most_commonly_used/",
        description:
          "Expressjs is by far the most commonly used still. Nestjs wraps express as well if you want to bring more convention / framework to a expressjs application.",
        rank: 2,
        domain: "www.reddit.com",
      },
      {
        title: "Node Frameworks",
        link: "https://www.geeksforgeeks.org/node-js/node-js-frameworks/",
        description:
          "5 days ago — A node framework is a workspace platform that supports the use of Node.js and which allows developers to use JavaScript for developing front end ...",
        rank: 3,
        domain: "www.geeksforgeeks.org",
      },
      {
        title: "Best NodeJS frameworks for seamless backend development",
        link: "https://ably.com/blog/best-nodejs-frameworks",
        description:
          "6 Nov 2023 — NodeJS is a JavaScript runtime environment for running JavaScript applications outside the browser environment.",
        rank: 4,
        domain: "ably.com",
      },
      {
        title: "Express - Node.js web application framework",
        link: "https://expressjs.com/",
        description:
          "Express is a minimal and flexible Node.js web application framework that provides a robust set of features for web and mobile applications.",
        rank: 5,
        domain: "expressjs.com",
      },
      {
        title: "AdonisJS - A fully featured web framework for Node.js",
        link: "https://adonisjs.com/",
        description:
          "AdonisJS is a TypeScript-first web framework for building web apps and API servers. It comes with support for testing, modern tooling, an ecosystem of official ...",
        rank: 6,
        domain: "adonisjs.com",
      },
      {
        title: "12 Best Node.js Frameworks for App Development in 2024",
        link: "https://www.simform.com/blog/best-nodejs-frameworks/",
        description:
          "14 Apr 2021 — Express.js, aka Express, tops the list of best Node.js frameworks. It has a minimalistic approach and seems to be a classic and straightforward ...",
        rank: 7,
        domain: "www.simform.com",
      },
      {
        title: "NestJS - A progressive Node.js framework",
        link: "https://nestjs.com/",
        description:
          "Hello, nest! A progressive Node.js framework for building efficient, reliable and scalable server-side applications. Documentation Source code ...",
        rank: 8,
        domain: "nestjs.com",
      },
      {
        title: "Node.js Frameworks Roundup 2024 — Elysia / Hono / Nest ...",
        link: "https://dev.to/encore/nodejs-frameworks-roundup-2024-elysia-hono-nest-encore-which-should-you-pick-19oj",
        description:
          "1 Nov 2024 — In this post, I'll walk you through the hottest frameworks in the Node.js ecosystem, breaking down the strengths, weaknesses, and best use cases for each one.",
        rank: 9,
        domain: "dev.to",
      },
    ],
    requestId: "1755538986098_uet14",
  });
}
// Setup routes
const router = Router();
router.post("/search", process.env.TEST_MODE === "true" ? testSearch : search);

export default router;
