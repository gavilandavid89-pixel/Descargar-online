const express = require('express');
const rateLimit = require('express-rate-limit');
const { create: createYoutubeDl } = require('youtube-dl-exec');
const ytDlp = createYoutubeDl('/usr/local/bin/yt-dlp');
const path = require('path');
const fs = require('fs');

const app = express();

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Necesario para leer la IP real tras proxies como Render, Nginx o Cloudflare
app.set('trust proxy', 1);

// Limitador de información (30 análisis por 15 minutos por IP)
const infoLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 30,
    message: { error: 'Demasiadas consultas desde esta IP. Inténtalo más tarde.' }
});

// Limitador de descargas (5 descargas HD por hora por IP)
const downloadLimiter = rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 5,
    message: { error: 'Has alcanzado el límite de descargas HD por hora.' }
});

// Endpoint 1: Obtener información del video, metadata y calidades
app.post('/api/info', infoLimiter, async (req, res) => {
    const { url } = req.body;
    if (!url) return res.status(400).json({ error: 'La URL es requerida.' });

    try {
        const output = await ytDlp(url, {
            dumpSingleJson: true,
            noWarnings: true,
            jsRuntimes: 'deno',
            getComments: true
        });

        const rawFormats = (output.formats || []).filter(f => f.vcodec !== 'none');

        let formats = rawFormats.map(f => {
            const height = f.height || 0;
            // Regla estricta: si es igual o superior a 1080p, requiere ver anuncio
            const is_best = height >= 1080;
            return {
                format_id: f.format_id,
                resolution: f.resolution || `${f.width || 0}x${height}`,
                height: height,
                ext: f.ext,
                filesize: f.filesize || f.filesize_approx || null,
                is_best: is_best
            };
        });

        // Eliminar duplicados de resoluciones
        const uniqueFormats = [];
        const seenHeights = new Set();
        formats.sort((a, b) => b.height - a.height);

        for (const fmt of formats) {
            if (fmt.height > 0 && !seenHeights.has(fmt.height)) {
                seenHeights.add(fmt.height);
                uniqueFormats.push(fmt);
            }
        }

        // Si no hay resoluciones >= 1080p, marcar la resolución máxima como is_best
        if (uniqueFormats.length > 0 && !uniqueFormats.some(f => f.is_best)) {
            uniqueFormats[0].is_best = true;
        }

        const comments = (output.comments || []).slice(0, 5).map(c => ({
            author: c.author || 'Anónimo',
            text: c.text || '',
            like_count: c.like_count || 0
        }));

        res.json({
            title: output.title || 'Video sin título',
            thumbnail: output.thumbnail || '',
            duration: output.duration || 0,
            views: output.view_count || 0,
            likes: output.like_count || 0,
            uploader: output.uploader || 'Desconocido',
            comments: comments,
            formats: uniqueFormats
        });

    } catch (error) {
        console.error(error);
        res.status(500).json({ error: 'No se pudo obtener información de la URL proporcionada.' });
    }
});

// Endpoint 2: Descarga en Stream
app.post('/api/download-best', downloadLimiter, async (req, res) => {
    const { url } = req.body;
    if (!url) return res.status(400).json({ error: 'URL no válida.' });

    try {
        res.setHeader('Content-Type', 'video/mp4');
        res.setHeader('Content-Disposition', 'attachment; filename="video_descargado.mp4"');

        const subprocess = ytDlp.exec(url, {
            format: 'bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b',
            mergeOutputFormat: 'mp4',
            output: '-'
        });

        subprocess.stdout.pipe(res);

        subprocess.on('error', (err) => {
            console.error('Error procesando el video:', err);
            if (!res.headersSent) res.status(500).json({ error: 'Error en el procesamiento del video.' });
        });

    } catch (err) {
        if (!res.headersSent) res.status(500).json({ error: 'Error interno del servidor.' });
    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Servidor activo en el puerto ${PORT}`));
