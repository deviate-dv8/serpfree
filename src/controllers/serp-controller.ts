import { Request, Response, Router } from "express";
import z from "zod";
import { scraper } from "../";
import { SearchEngine } from "../scraper-provider";

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
      error: error instanceof Error ? error.message : String(error),
      requestId,
    });
  }
}

// Setup routes
const router = Router();

router.post("/search", search);

export default router;
