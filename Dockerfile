FROM node:20-slim

ENV DEBIAN_FRONTEND=noninteractive

# FFmpeg: mezcla/remux de vídeo y audio.
# Python + curl: utilidades de yt-dlp y actualización automática.
RUN apt-get update && apt-get install -y \
    ffmpeg \
    python3 \
    curl \
    ca-certificates \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Instalar Deno para los extractores JS modernos de yt-dlp.
RUN curl -fsSL https://deno.land/install.sh | DENO_INSTALL=/usr/local sh \
    && test -x /usr/local/bin/deno \
    && deno --version

# Primera instalación de yt-dlp desde el canal oficial latest.
RUN curl -fL --retry 3 \
    https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp \
    -o /usr/local/bin/yt-dlp \
    && chmod 755 /usr/local/bin/yt-dlp \
    && yt-dlp --version

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

RUN chmod +x /app/update-ytdlp.sh /app/entrypoint.sh

EXPOSE 3000

# Actualiza yt-dlp automáticamente cada vez que el contenedor arranca.
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["node", "server.js"]
