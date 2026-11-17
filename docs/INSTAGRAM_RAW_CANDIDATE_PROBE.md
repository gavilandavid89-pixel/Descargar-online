# Instagram: diagnóstico de variantes MP4 sin metadatos

Fecha: 2026-11-17  
Rama: `feature/instagram-quality-extractor`  
Ejecución: [GitHub Actions #44036519571](https://github.com/gavilandavid89-pixel/Descargar-online/actions/runs/44036519571)  
Artefacto: `instagram-raw-candidate-probe` (retención de 3 días)

## Objetivo

Comprobar si los tres formatos directos MP4 que yt-dlp entrega como IDs `1`, `2` y `3`, pero sin dimensiones ni códec declarados, esconden una variante de mayor resolución que las dos pistas DASH conocidas.

Se utilizó el Reel público de prueba:

`https://www.instagram.com/reel/C9AEmIxyTQb/`

## Resultado

Los tres candidatos se pudieron inspeccionar con ffprobe y devolvieron exactamente los mismos datos:

| Formato | Códec | Resolución | FPS | Bitrate de vídeo | Duración del contenedor | Tamaño |
|---|---|---:|---:|---:|---:|---:|
| `1` | H.264 | 720×1280 | 30 | 2,345,199 bps | 14.997234 s | 4,554,577 bytes |
| `2` | H.264 | 720×1280 | 30 | 2,345,199 bps | 14.997234 s | 4,554,577 bytes |
| `3` | H.264 | 720×1280 | 30 | 2,345,199 bps | 14.997234 s | 4,554,577 bytes |

Los tres también incluyen audio AAC. La pista de vídeo dura 14.933333 s.

## Conclusión

- No apareció ninguna variante superior a 720×1280 en estos tres candidatos.
- Los tres candidatos son equivalentes en las propiedades multimedia medidas y en el tamaño; no aportan una calidad adicional.
- La selección actual de la variante DASH 720×1280 sigue siendo correcta para este Reel.
- No se deben presentar los IDs `1`, `2` o `3` como calidades independientes ni inventarles dimensiones a partir de su ID.
- La duración declarada por la API/EFG puede ser entera y aproximada; la inspección del archivo final con ffprobe ofrece una medida más precisa.

## Seguridad y alcance

El workflow solo inspecciona URLs HTTPS cuyos hosts terminan en `cdninstagram.com` o `fbcdn.net`. Las URLs firmadas no se imprimen ni se incluyen en el artefacto. No se usan cookies ni una sesión de Instagram.

Este resultado corresponde al Reel de prueba y no demuestra que todas las publicaciones de Instagram tengan el mismo comportamiento. Si otro Reel presenta candidatos sin metadatos, conviene repetir el sondeo antes de generalizar.
