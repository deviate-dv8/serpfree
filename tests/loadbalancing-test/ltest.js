import http from "k6/http";
import faker from "k6/x/faker";
import { fail, sleep } from "k6";
export const options = {
  vus: 100,
  duration: "60s",
};
const provider = ["bing", "yahoo", "duckduckgo"];
const provider2 = ["bing", "google"];
export default function () {
  const url = "http://localhost:3000/api/serp/search";
  // const url = "http://serpfree.onrender.com/api/serp/search";
  // const url = "https://webstandr-scraper.onrender.com/api/serp/search";
  const payload = JSON.stringify({
    query: faker.person.firstName(),
    // provider: "google",
    // provider: provider2[Math.floor(Math.random() * provider2.length)],
    provider: provider[Math.floor(Math.random() * provider.length)],
  });
  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };
  const response = http.post(url, payload, params);
  console.log(`Response status: ${response.status}`);
  if (response.status !== 200) {
    console.error(`Error: ${response.status} - ${response.body} - ${payload}`);
  }
}
