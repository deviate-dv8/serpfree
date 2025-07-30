import dotenv from "dotenv";
import express from "express";
import SERPScraper from "./scraper-provider";
import routes from "./routes";
// Initialize
dotenv.config();
const app = express();
export let scraper: SERPScraper | null = null;
const PORT = 3000;

// Middleware
app.use(express.static("public"));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get("/", (req, res) => {
  res.json({ message: "Welcome to SERP Scraper API" });
});
app.use("/api", routes);
app.listen(PORT, () => {
  scraper = new SERPScraper(parseInt(process.env.TAB_LIMIT as string) || 1000);
  console.log("⚡️[server]: Server is running at http://localhost:" + PORT);
});

process.on("SIGINT", async () => {
  await scraper?.closeBrowser();
  process.exit();
});

process.on("SIGUSR2", async () => {
  await scraper?.closeBrowser();
  process.exit();
});
