FROM node:20-slim

# FFmpeg, Python3, curl and unzip
RUN apt-get update && apt-get install -y \
    ffmpeg \
    python3 \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno (JavaScript runtime required by current yt-dlp for full YouTube support)
RUN curl -fsSL https://deno.land/install.sh | sh \
    && mv /root/.deno/bin/deno /usr/local/bin/deno \
    && chmod a+rx /usr/local/bin/deno

# Install the latest official yt-dlp executable
RUN curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp \
    && chmod a+rx /usr/local/bin/yt-dlp \
    && /usr/local/bin/yt-dlp --version

WORKDIR /app

COPY package*.json ./
RUN YOUTUBE_DL_SKIP_DOWNLOAD=true npm install --omit=dev

COPY . .

EXPOSE 3000

CMD ["node", "server.js"]
