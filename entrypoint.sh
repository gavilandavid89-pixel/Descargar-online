#!/bin/sh
set -eu

# El actualizador vive dentro de /app; no asumir que está en /usr/local/bin.
if [ -x /app/update-ytdlp.sh ]; then
    /app/update-ytdlp.sh || echo '[WARN] No se pudo actualizar yt-dlp; se continuará con la versión instalada.' >&2
else
    echo '[WARN] No existe /app/update-ytdlp.sh; se omite la actualización.' >&2
fi

exec "$@"
