FROM node:20-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    python3 \
    curl \
    ca-certificates \
    unzip \
    && ln -sf /usr/bin/python3 /usr/local/bin/python \
    && python --version \
    && rm -rf /var/lib/apt/lists/*

# Deno supports modern JavaScript extractors in yt-dlp.
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh \
    && test -x /usr/local/bin/deno \
    && deno --version

# Install yt-dlp from the official release.
RUN curl -fL --retry 3 \
    https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp \
    -o /usr/local/bin/yt-dlp \
    && chmod 755 /usr/local/bin/yt-dlp \
    && yt-dlp --version

WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev

COPY . .
RUN test -s /app/update-ytdlp.sh \
    && test -s /app/entrypoint.sh \
    && chmod 755 /app/update-ytdlp.sh /app/entrypoint.sh \
    && sh -n /app/entrypoint.sh \
    && sh -n /app/update-ytdlp.sh

EXPOSE 3000
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]
