import dotenv from "dotenv";
import express from "express";
import SERPScraper from "./scraper-provider";
import routes from "./routes";
import si from "systeminformation";

// Initialize
dotenv.config();
const app = express();
export let scraper: SERPScraper | null = null;
const PORT = 3000;

// Middleware
app.use(express.static("public"));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

async function logUsage() {
  const [mem, cpu] = await Promise.all([si.mem(), si.currentLoad()]);
  console.log(
    `Memory: ${(mem.used / 1024 / 1024).toFixed(2)} MB used / ${(mem.total / 1024 / 1024).toFixed(2)} MB`,
  );
  console.log(`CPU Load: ${cpu.currentLoad.toFixed(2)}%`);
}

app.get("/", (req, res) => {
  res.json({ message: "Welcome to SERP Scraper API" });
});
app.use("/api", routes);
app.listen(PORT, () => {
  scraper = new SERPScraper(parseInt(process.env.TAB_LIMIT as string) || 1000);
  console.log("⚡️[server]: Server is running at http://localhost:" + PORT);
  const PROXY_HOST = process.env.PROXY_HOST;
  const PROXY_PORT = process.env.PROXY_PORT;
  if (PROXY_HOST && PROXY_PORT) {
    console.log(`Using proxy: ${PROXY_HOST}:${PROXY_PORT}`);
  } else {
    console.log("No proxy configured.");
  }
  const USAGE_MONITORING = process.env.USAGE_MONITORING === "true";
  if (USAGE_MONITORING) {
    setInterval(logUsage, 1000);
    console.log("Usage monitoring enabled.");
  }
});

process.on("SIGINT", async () => {
  await scraper?.closeBrowser();
  process.exit();
});

process.on("SIGUSR2", async () => {
  await scraper?.closeBrowser();
  process.exit();
});
