FROM node:20-alpine AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

FROM node:20-alpine AS runtime

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY --from=builder /app/dist ./dist

ENV NODE_ENV=production

USER node

CMD ["node", "dist/src/index.js"]
