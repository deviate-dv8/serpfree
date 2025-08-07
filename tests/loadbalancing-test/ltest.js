import http from "k6/http";
import faker from "k6/x/faker";
export const options = {
  vus: 2,
  duration: "60s",
};
const provider = ["google", "bing", "yahoo", "duckduckgo"];
export default function () {
  // const url = "http://localhost:3000/api/serp/search";
  const url = "http://serpfree.onrender.com";
  // const url = "https://webstandr-scraper.onrender.com/api/serp/search";
  const payload = JSON.stringify({
    query: faker.person.firstName(),
    // provider: "google",
    provider: provider[Math.floor(Math.random() * provider.length)],
  });
  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };
  const response = http.post(url, payload, params);
  console.log(`Response status: ${response.status}`);
}
