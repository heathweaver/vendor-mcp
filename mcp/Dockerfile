FROM node:22-alpine AS builder

WORKDIR /app

COPY mcp/package.json mcp/package-lock.json ./
RUN npm install --ignore-scripts

COPY mcp/src ./src
COPY mcp/tsconfig.json .
RUN npm run build

# ── Runtime ──────────────────────────────────────────────────────────────────
FROM node:22-alpine AS release

WORKDIR /app

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json .
COPY --from=builder /app/package-lock.json .
COPY .env .env

RUN npm ci --omit=dev

# Install cloudflared so we can run tunnel setup from inside the container
RUN apk add --no-cache curl && \
    curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
    -o /usr/local/bin/cloudflared && \
    chmod +x /usr/local/bin/cloudflared

# Persist cloudflared credentials across container restarts
VOLUME ["/root/.cloudflared"]

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:8765/health', r => process.exit(r.statusCode === 200 ? 0 : 1)).on('error', () => process.exit(1))"

CMD ["sh", "-c", "cloudflared tunnel --config /root/.cloudflared/config.yml run & node dist/index.js"]
