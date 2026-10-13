# Respaldo opcional de descarga con Cobalt

MediaGrab Pro puede usar una instancia de la API de Cobalt como respaldo cuando yt-dlp no consigue descargar el formato seleccionado.

## Configuración

Añade estas variables en el servicio de Render de MediaGrab Pro:

- `COBALT_API_URL`: URL HTTPS de una instancia Cobalt que controles o cuyo propietario te haya autorizado a usar.
- `COBALT_API_KEY`: opcional, si la instancia usa autenticación `Api-Key`.
- `COBALT_API_TOKEN`: opcional, si la instancia usa autenticación `Bearer`. Si ambos se definen, se usa `COBALT_API_KEY`.

No configures `api.cobalt.tools` como si fuera una API pública gratuita sin permiso: la documentación de Cobalt indica que las instancias alojadas no están destinadas a integraciones de terceros sin autorización. Puedes desplegar una instancia propia, pero el coste y los límites dependen del alojamiento elegido.

## Qué hace el respaldo

- Solo se intenta después de que fallen las rutas de descarga de yt-dlp.
- Solicita a Cobalt la calidad estándar elegida y fuerza el túnel de la misma instancia.
- Solo acepta una URL de túnel HTTPS en el mismo origen configurado; no sigue URLs externas devueltas por la API.
- Limita el archivo recibido a 2 GB y después pasa por la validación multimedia existente de FFprobe y la validación de resolución.
- Por seguridad, actualmente solo se activa para resoluciones estándar: 144, 240, 360, 480, 720, 1080, 1440, 2160 y 4320p.

Este respaldo no reemplaza yt-dlp para obtener los metadatos y la lista de calidades de la pantalla. Si no se configura `COBALT_API_URL`, MediaGrab Pro continúa funcionando como antes. El endpoint `/api/health` informa `cobaltFallbackConfigured: true/false` sin revelar credenciales.
