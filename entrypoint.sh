#!/bin/sh
set -eu
/usr/local/bin/update-ytdlp.sh
exec "$@"
