#!/bin/sh
set -eu

YTDLP_PATH="/usr/local/bin/yt-dlp"
LATEST_URL="https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp"
TMP_PATH="${YTDLP_PATH}.tmp.$$"

trap 'rm -f "$TMP_PATH"' EXIT HUP INT TERM

echo "[yt-dlp] Comprobando actualización..."
if curl -fL --retry 3 --retry-delay 1 --connect-timeout 10 --max-time 90 "$LATEST_URL" -o "$TMP_PATH"; then
  chmod 755 "$TMP_PATH"
  if "$TMP_PATH" --version >/dev/null 2>&1; then
    mv -f "$TMP_PATH" "$YTDLP_PATH"
    echo "[yt-dlp] Versión activa: $($YTDLP_PATH --version)"
  else
    rm -f "$TMP_PATH"
    echo "[yt-dlp] El archivo descargado no es válido; se conserva la versión anterior." >&2
  fi
else
  rm -f "$TMP_PATH"
  if [ -x "$YTDLP_PATH" ]; then
    echo "[yt-dlp] No se pudo actualizar; se conserva la versión instalada: $($YTDLP_PATH --version)" >&2
  else
    echo "[yt-dlp] No hay ejecutable disponible y la actualización falló." >&2
    exit 1
  fi
fi
