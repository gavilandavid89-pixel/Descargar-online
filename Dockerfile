FROM node:22-slim

RUN apt-get update && apt-get install -y \
    ffmpeg \
    python3 \
    python3-pip \
    git \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# JavaScript runtime required by current yt-dlp YouTube extraction.
RUN curl -fsSL https://deno.land/install.sh | sh \
    && mv /root/.deno/bin/deno /usr/local/bin/deno \
    && chmod a+rx /usr/local/bin/deno

# Current official yt-dlp binary.
RUN curl -L --fail https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && /usr/local/bin/yt-dlp --version

# BgUtils PO-token provider. YouTube's current enforcement can require
# per-video proof-of-origin tokens for GVS/player requests.
RUN git clone --depth 1 --branch 2.0.1 https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /opt/bgutil-ytdlp-pot-provider \
    && cd /opt/bgutil-ytdlp-pot-provider/server \
    && npm ci \
    && npx tsc

# Install the yt-dlp plugin that talks to the provider.
RUN python3 -m pip install --break-system-packages --no-cache-dir bgutil-ytdlp-pot-provider

WORKDIR /app
COPY package*.json ./
RUN YOUTUBE_DL_SKIP_DOWNLOAD=true npm install --omit=dev
COPY . .

RUN printf '%s\n' \
  '#!/bin/sh' \
  'set -e' \
  'node /opt/bgutil-ytdlp-pot-provider/server/build/main.js --host 127.0.0.1 --port 4416 >/tmp/bgutil-provider.log 2>&1 &' \
  'sleep 2' \
  'echo "[startup] yt-dlp: $(/usr/local/bin/yt-dlp --version)"' \
  'echo "[startup] bgutil provider started"' \
  'exec node /app/server.js' \
  > /usr/local/bin/start-app.sh \
  && chmod +x /usr/local/bin/start-app.sh

EXPOSE 3000
CMD ["/usr/local/bin/start-app.sh"]
