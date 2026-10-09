FROM node:20-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHON=/usr/bin/python3

# Runtime tools required by yt-dlp and media remuxing.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    python3 \
    python-is-python3 \
    curl \
    ca-certificates \
    unzip \
    && python --version \
    && python3 --version \
    && rm -rf /var/lib/apt/lists/*

# Deno is used by yt-dlp for supported JavaScript-dependent extractors.
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh \
    && test -x /usr/local/bin/deno \
    && /usr/local/bin/deno --version

# Install yt-dlp from the official release channel.
RUN curl -fL --retry 3 \
    https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp \
    -o /usr/local/bin/yt-dlp \
    && chmod 755 /usr/local/bin/yt-dlp \
    && /usr/local/bin/yt-dlp --version

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

RUN chmod +x /app/update-ytdlp.sh /app/entrypoint.sh

EXPOSE 3000

# Update yt-dlp on container start; retain the installed version if update fails.
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]
