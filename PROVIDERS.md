# MediaGrab Pro: proveedores y enlaces de imagen

## Proveedores de video

MediaGrab usa un flujo escalonado para que un único extractor no sea un punto único de fallo:

1. **GenDownload**: proveedor externo ya integrado que puede devolver metadatos y formatos descargables. Puede cambiar, limitar solicitudes o dejar de funcionar; no se considera una garantía.
2. **yt-dlp**: extractor autoalojado del proyecto. Es el respaldo general para los sitios que soporta y permite mantener el procesamiento bajo nuestro control.
3. **YouTube Data API**: fuente oficial de metadatos cuando se configura `YOUTUBE_API_KEY`. No entrega por sí misma los archivos de video ni las calidades descargables.
4. **Cobalt (opcional)**: respaldo final para obtener un enlace directo cuando fallan los anteriores. Se activa configurando `COBALT_API_URL` con la URL de una instancia propia o autorizada compatible con la API actual de Cobalt. No se llama a una instancia pública por defecto.

### Configuración opcional de Cobalt

Añade a las variables de entorno de Render:

- `COBALT_API_URL`: URL HTTPS de la API de tu instancia Cobalt, por ejemplo la raíz o ruta de API que indique la documentación de esa instancia.

Cobalt entrega un archivo directo y no siempre proporciona una lista de calidades. En ese caso MediaGrab muestra **Archivo original · Cobalt** sin afirmar que conoce la resolución real.

**Licencias y uso:** Cobalt es software libre bajo AGPL-3.0; revisa sus condiciones si modificas o alojas una instancia. La instancia pública de Cobalt no es un servicio gratuito universal para sitios monetizados: no dependas de ella en producción comercial. GenDownload es un servicio externo, no una API libre garantizada. yt-dlp es software libre, pero cada plataforma mantiene sus propios límites y reglas.

Fuentes:
- Cobalt: https://github.com/imputnet/cobalt
- API y despliegue de Cobalt: https://github.com/imputnet/cobalt/tree/main/api
- yt-dlp: https://github.com/yt-dlp/yt-dlp
- Ejemplo de API autoalojada para yt-dlp: https://github.com/fvadicamo/yt-dlp-api

## Enlaces directos a imágenes

- Detecta imágenes por el tipo MIME del servidor, aunque la URL no termine en `.jpg`, `.png` o similar.
- Muestra una vista previa y ofrece la descarga del archivo original.
- Acepta JPEG, PNG, WebP, GIF, AVIF, BMP, TIFF e iconos; SVG se excluye por seguridad.
- Límite de tamaño: 20 MB por imagen.
- Solo admite HTTPS y rechaza destinos que resuelvan a direcciones privadas/locales. Se siguen y vuelven a validar un máximo de cuatro redireccionamientos.
- Los enlaces a páginas web que contienen una imagen, en vez de apuntar al archivo de imagen, siguen necesitando un extractor específico para esa página.

## Seguridad y límites

Los proveedores externos pueden fallar por cambios en sus plataformas, límites, verificación, región o permisos. No se debe intentar eludir controles de acceso. Usa únicamente contenido al que tengas derecho de acceso y descarga.
