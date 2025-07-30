# Build Environment: Node + Puppeteer
FROM mcp/puppeteer

# Env
WORKDIR /app
ENV PATH /app/node_modules/.bin:$PATH

# Export port 8080 for Node
EXPOSE 3000

# Copy package files and install dependencies first for better caching
COPY package*.json ./
RUN npm install

# Copy all app files into Docker Work directory
COPY . .

# Build the application
RUN npm run build

# Install xvfb for headless browser support
RUN apt-get update && apt-get install -y xvfb && rm -rf /var/lib/apt/lists/*

CMD ["npm", "start"]
