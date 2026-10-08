#!/bin/sh
set -eu

# El script se copia a /app en el Dockerfile. Mantener una única ruta canónica.
if [ -x /app/update-ytdlp.sh ]; then
    /app/update-ytdlp.sh
elif [ -f /app/update-ytdlp.sh ]; then
    echo "[entrypoint] update-ytdlp.sh existe pero no es ejecutable; usando sh." >&2
    sh /app/update-ytdlp.sh
else
    echo "[entrypoint] ERROR: falta el script /app/update-ytdlp.sh (revisa COPY . . y el contexto de build)." >&2
    exit 1
fi

exec "$@"
