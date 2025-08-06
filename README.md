# serpfree

An opensource SERPAPI alternative.

## Features

- Query search engine results programmatically.
- Supports multiple providers:
  - Google
  - Bing
  - DuckDuckGo
  - Yahoo
- Lightweight and easy to integrate into your projects.

## Installation

Fork the repository and run:

```bash
git clone https://github.com/deviate-dv8/serpfree.git
```

Install dependencies:

```bash
cd serpfree
npm install
```

## Usage

```bash
npm run dev
```

## Example Usage

POST:

```bash
curl -X POST http://localhost:3000/api/serp/search \
-H "Content-Type: application/json" \
-d '{
  "provider": "google",
  "query": "example search query"
}'
```

## Notes

- The project uses `puppeteer-real-browser` to avoid detection by Bing, Yahoo, and DuckDuckGo. 
- ~~Google works if your IP is already recognized by Google servers. Deploying it on a VPS with a fresh IP may fail initially due to Google's anti-bot mechanism. You will need to manually search once and pass their CAPTCHA to obtain a session ID.~~ **(Solved)**

## Future Updates

- Add support for location-based and language-specific queries.

## Cloud Deployment

The project is deployed on Render and can be accessed at:

https://serpfree.onrender.com


## Example Multi-tab Scraping

[Screencast From 2025-07-28 13-12-32.webm](https://github.com/user-attachments/assets/c33ab55f-a123-4120-bf27-256d0637410f)
