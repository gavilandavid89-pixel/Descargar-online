const express = require('express');
const rateLimit = require('express-rate-limit');
const ytDlp = require('yt-dlp-exec');
const path = require('path');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const { promisify } = require('util');
const { pipeline } = require('stream');
const { spawn } = require('child_process');

const app = express();
const pipelineAsync = promisify(pipeline);
const YTDLP_BINARY = process.env.YTDLP_BINARY || '/usr/local/bin/yt-dlp';
const INFO_CACHE_TTL = 2 * 60 * 1000;
const infoCache = new Map();

app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.set('trust proxy', 1);

const infoLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 30,
    message: { error: 'Demasiadas consultas desde esta IP. Inténtalo más tarde.' }
});

const downloadLimiter = rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 5,
    message: { error: 'Has alcanzado el límite de descargas HD por hora.' }
});

function normalizeUrl(value) {
    return typeof value === 'string' ? value.trim() : '';
}

function detectPlatform(url) {
    try {
        const host = new URL(url).hostname.toLowerCase().replace(/^www\./, '');
        if (host === 'tiktok.com' || host.endsWith('.tiktok.com')) return 'tiktok';
        if (host === 'facebook.com' || host.endsWith('.facebook.com') || host === 'fb.watch') return 'facebook';
        if (host === 'x.com' || host.endsWith('.x.com') || host === 'twitter.com' || host.endsWith('.twitter.com')) return 'x';
        if (host === 'instagram.com' || host.endsWith('.instagram.com')) return 'instagram';
        if (host === 'youtube.com' || host.endsWith('.youtube.com') || host === 'youtu.be') return 'youtube';
        return 'generic';
    } catch {
        return 'unknown';
    }
}

function buildBaseArgs(platform) {
    const args = {
        noWarnings: true,
        noPlaylist: true,
        jsRuntimes: 'deno'
    };

    // No fijamos una lista de clientes de YouTube: sus respuestas pueden
    // cambiar y la selección predeterminada de yt-dlp puede adaptarse mejor.
    return args;
}

function safeFilename(name) {
    const clean = String(name || 'video')
        .normalize('NFKC')
        .replace(/[<>:"/\\|?*\x00-\x1F\r\n]/g, '_')
        .replace(/\s+/g, ' ')
        .trim()
        .slice(0, 120);
    return clean || 'video';
}

function contentDisposition(name, height) {
    const filename = safeFilename(String(name || 'video') + '_' + height + 'p.mp4');
    const ascii = filename.normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[^\x20-\x7E]/g, '_')
        .replace(/["\\]/g, '_')
        .replace(/[\r\n]/g, '_');
    return "attachment; filename=\"" + ascii + "\"; filename*=UTF-8''" + encodeURIComponent(filename);
}

function runBinary(args) {
    return new Promise((resolve, reject) => {
        const child = spawn(YTDLP_BINARY, args, { stdio: ['ignore', 'pipe', 'pipe'] });
        let stdout = '';
        let stderr = '';
        child.stdout.on('data', d => stdout += d.toString());
        child.stderr.on('data', d => stderr += d.toString());
        child.on('error', reject);
        child.on('close', code => {
            if (code === 0) resolve({ stdout, stderr });
            else reject(new Error(stderr || `yt-dlp terminó con código ${code}`));
        });
    });
}

function normalizeFormats(output) {
    const all = Array.isArray(output.formats) ? output.formats : [];
    const candidates = all.filter(f =>
        f.format_id !== undefined && f.format_id !== null && String(f.format_id).length > 0 &&
        f.vcodec && f.vcodec !== 'none' && Number(f.height || 0) > 0
    ).map(f => ({
        formatId: String(f.format_id),
        height: Number(f.height),
        width: Number(f.width || 0),
        fps: Number(f.fps || 0),
        ext: String(f.ext || '').toLowerCase(),
        vcodec: String(f.vcodec || ''),
        hasAudio: !!(f.acodec && f.acodec !== 'none'),
        filesize: f.filesize || f.filesize_approx || null
    }));

    // Mostrar únicamente alturas detectadas para este enlace; no inventar calidades.
    const byHeight = new Map();
    for (const format of candidates) {
        const current = byHeight.get(format.height);
        const score = item => (item.hasAudio ? 1000000 : 0) +
            (item.ext === 'mp4' ? 10000 : 0) +
            (item.vcodec.startsWith('avc') ? 1000 : 0) +
            (item.fps || 0) + Math.min(Number(item.filesize || 0) / 100000000, 100);
        if (!current || score(format) > score(current)) byHeight.set(format.height, format);
    }

    return [...byHeight.values()].sort((a, b) => b.height - a.height).map(f => ({
        quality: String(f.height) + 'p',
        height: f.height,
        width: f.width || null,
        fps: f.fps || null,
        ext: f.ext || 'unknown',
        filesize: f.filesize,
        hasAudio: f.hasAudio,
        formatId: f.formatId
    }));
}

function getYouTubeVideoId(value) {
    try {
        const parsed = new URL(value);
        const host = parsed.hostname.toLowerCase().replace(/^www\./, '');
        if (host === 'youtu.be') return parsed.pathname.split('/').filter(Boolean)[0] || '';
        if (host === 'youtube.com' || host.endsWith('.youtube.com')) {
            const direct = parsed.searchParams.get('v');
            if (direct) return direct;
            const parts = parsed.pathname.split('/').filter(Boolean);
            if (['shorts', 'embed', 'live', 'v'].includes(parts[0])) return parts[1] || '';
        }
    } catch {}
    return '';
}

function parseIsoDuration(value) {
    const match = /^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$/i.exec(String(value || ''));
    if (!match) return 0;
    return (Number(match[1] || 0) * 3600) + (Number(match[2] || 0) * 60) + Number(match[3] || 0);
}

async function getYouTubeApiInfo(url) {
    const apiKey = process.env.YOUTUBE_API_KEY;
    const videoId = getYouTubeVideoId(url);
    if (!apiKey || !videoId) return null;

    const endpoint = new URL('https://www.googleapis.com/youtube/v3/videos');
    endpoint.searchParams.set('part', 'snippet,contentDetails,statistics');
    endpoint.searchParams.set('id', videoId);
    endpoint.searchParams.set('key', apiKey);

    const response = await fetch(endpoint, { headers: { accept: 'application/json' } });
    if (!response.ok) throw new Error(`YouTube Data API respondió HTTP ${response.status}`);
    const payload = await response.json();
    const item = payload.items && payload.items[0];
    if (!item) throw new Error('La API de YouTube no encontró un vídeo público con ese enlace.');

    const snippet = item.snippet || {};
    const statistics = item.statistics || {};
    const thumbnails = snippet.thumbnails || {};
    const thumbnail = thumbnails.maxres || thumbnails.standard || thumbnails.high || thumbnails.medium || thumbnails.default;
    return {
        title: snippet.title || 'Video sin título',
        thumbnail: thumbnail && thumbnail.url || '',
        duration: parseIsoDuration(item.contentDetails && item.contentDetails.duration),
        views: Number(statistics.viewCount || 0),
        likes: Number(statistics.likeCount || 0),
        uploader: snippet.channelTitle || 'Desconocido',
        platform: 'youtube',
        formats: [],
        infoOnly: true,
        notice: 'Información obtenida de la API de YouTube. La descarga sigue dependiendo de los formatos que pueda extraer yt-dlp.'
    };
}

async function getInfo(url, platform) {
    const cached = infoCache.get(url);
    if (cached && Date.now() - cached.time < INFO_CACHE_TTL) return cached.data;

    let output;
    try {
        output = await ytDlp(url, {
            ...buildBaseArgs(platform),
            dumpSingleJson: true,
            skipDownload: true,
            getComments: false
        });
    } catch (extractError) {
        // La API oficial solo aporta metadatos; no sustituye la extracción
        // de formatos ni intenta eludir restricciones de acceso.
        if (platform === 'youtube' && process.env.YOUTUBE_API_KEY) {
            const apiInfo = await getYouTubeApiInfo(url);
            if (apiInfo) {
                infoCache.set(url, { time: Date.now(), data: apiInfo });
                return apiInfo;
            }
        }
        throw extractError;
    }

    // Solo exponemos metadatos públicos y estrictamente necesarios.
    // Nunca devolvemos cookies, headers, URLs internas, formatos completos,
    // rutas locales, datos de autenticación ni el objeto crudo de yt-dlp.
    const formats = normalizeFormats(output);
    if (!formats.length) throw new Error('No se encontraron calidades de vídeo disponibles.');

    const data = {
        title: typeof output.title === 'string' ? output.title : 'Video sin título',
        thumbnail: typeof output.thumbnail === 'string' ? output.thumbnail : '',
        duration: Number.isFinite(Number(output.duration)) ? Number(output.duration) : 0,
        views: Number.isFinite(Number(output.view_count)) ? Number(output.view_count) : 0,
        likes: Number.isFinite(Number(output.like_count)) ? Number(output.like_count) : 0,
        uploader: typeof output.uploader === 'string' ? output.uploader : 'Desconocido',
        platform,
        formats: formats.map(f => ({
            quality: f.quality,
            height: f.height,
            width: f.width,
            fps: f.fps,
            ext: f.ext,
            filesize: f.filesize,
            hasAudio: f.hasAudio,
            formatId: f.formatId
        }))
    };

    infoCache.set(url, { time: Date.now(), data });
    return data;
}

app.get('/api/health', async (req, res) => {
    try {
        const { stdout } = await runBinary(['--version']);
        res.json({ ok: true, ytDlpVersion: stdout.trim(), node: process.version });
    } catch (error) {
        res.status(500).json({ ok: false, error: error.message });
    }
});

app.post('/api/info', infoLimiter, async (req, res) => {
    const url = normalizeUrl(req.body?.url);
    if (!url) return res.status(400).json({ error: 'La URL es requerida.' });

    const platform = detectPlatform(url);

    try {
        const data = await getInfo(url, platform);
        res.json(data);
    } catch (error) {
        console.error('[yt-dlp info error]', error?.stderr || error?.message || error);
        res.status(500).json({
            error: 'No se pudo obtener información o formatos válidos de esta URL.',
            platform
        });
    }
});

function validHeight(value) {
    const n = Number(value);
    return Number.isInteger(n) && n >= 1 && n <= 4320;
}

async function downloadWithHeight(url, platform, selected, outputTemplate) {
    const height = Number(selected.height);
    const formatId = String(selected.formatId || '');
    if (!formatId || !/^[-\w.]+$/.test(formatId)) {
        throw new Error('El formato detectado ya no es válido. Vuelve a analizar el enlace.');
    }

    // Usar el ID detectado para la calidad seleccionada, no un selector genérico
    // que pueda acabar eligiendo otra pista. Añadir audio si la pista no lo trae.
    const selector = selected.hasAudio
        ? formatId + '/best[height=' + height + '][vcodec!=none][acodec!=none]/bestvideo[height=' + height + '][vcodec!=none]+bestaudio'
        : formatId + '+bestaudio/best[height=' + height + '][vcodec!=none][acodec!=none]/bestvideo[height=' + height + '][vcodec!=none]+bestaudio';
    const args = [
        '--no-warnings', '--no-playlist', '--no-part', '--no-overwrites',
        '--format', selector,
        '--merge-output-format', 'mp4',
        '--remux-video', 'mp4',
        '--output', outputTemplate,
        '--', url
    ];
    const base = buildBaseArgs(platform);
    if (base.jsRuntimes) args.unshift('--js-runtimes', base.jsRuntimes);
    if (base.extractorArgs) args.unshift('--extractor-args', base.extractorArgs);
    return runBinary(args);
}

async function probeMedia(filePath) {
    return new Promise((resolve, reject) => {
        const ffprobe = spawn('ffprobe', ['-v', 'error', '-show_entries', 'stream=codec_type', '-of', 'csv=p=0', filePath],
            { stdio: ['ignore', 'pipe', 'pipe'] });
        let stdout = '';
        let stderr = '';
        ffprobe.stdout.on('data', d => stdout += d.toString());
        ffprobe.stderr.on('data', d => stderr += d.toString());
        ffprobe.on('error', reject);
        ffprobe.on('close', code => {
            if (code !== 0) return reject(new Error(stderr || 'No se pudo verificar el archivo multimedia.'));
            const streams = stdout.split(/\r?\n/).map(s => s.trim()).filter(Boolean);
            resolve({ video: streams.includes('video'), audio: streams.includes('audio') });
        });
    });
}

async function muxVideoAndAudio(videoPath, audioPath, tempDir, id) {
    const finalPath = path.join(tempDir, id + '-final.mp4');
    await new Promise((resolve, reject) => {
        const args = ['-hide_banner', '-loglevel', 'error', '-y', '-i', videoPath];
        if (audioPath) args.push('-i', audioPath);
        args.push('-map', '0:v:0');
        if (audioPath) args.push('-map', '1:a:0');
        else args.push('-map', '0:a:0?');
        args.push('-c', 'copy', '-movflags', '+faststart', finalPath);
        const ffmpeg = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
        let stderr = '';
        ffmpeg.stderr.on('data', d => stderr += d.toString());
        ffmpeg.on('error', reject);
        ffmpeg.on('close', code => code === 0 ? resolve() : reject(new Error(stderr || 'FFmpeg no pudo combinar vídeo y audio.')));
    });
    return finalPath;
}

app.post('/api/download-best', downloadLimiter, async (req, res) => {
    const url = normalizeUrl(req.body?.url);
    const height = Number(req.body?.height);

    if (!url) return res.status(400).json({ error: 'URL no válida.' });
    if (!validHeight(height)) {
        return res.status(400).json({ error: 'Calidad no válida. Vuelve a analizar el vídeo.' });
    }

    const platform = detectPlatform(url);
    const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'mediagrab-'));
    const id = crypto.randomBytes(8).toString('hex');
    const outputTemplate = path.join(tempDir, `${id}.%(ext)s`);

    try {
        // No devolvemos datos privados del extractor al cliente.
        // Solo usamos la información pública ya filtrada para obtener título.
        const info = await getInfo(url, platform);
        const selected = info.formats.find(f => Number(f.height) === height);

        if (!selected) {
            throw new Error(`La calidad ${height}p ya no está disponible.`);
        }

        console.log(`[download] platform=${platform} height=${height}p`);

        await downloadWithHeight(url, platform, selected, outputTemplate);

        const mediaExtensions = new Set(['.mp4', '.m4v', '.mov', '.webm', '.mkv', '.m4a', '.mp3', '.aac', '.opus', '.ogg', '.ts']);
        const files = (await fs.promises.readdir(tempDir)).filter(name => {
            const lower = name.toLowerCase();
            return !lower.endsWith('.part') && !lower.endsWith('.ytdl') && !lower.endsWith('.json') && mediaExtensions.has(path.extname(lower));
        });
        if (!files.length) throw new Error('yt-dlp no generó un archivo multimedia.');

        // Inspeccionar cada archivo: no asumir que el primer MP4 es el vídeo.
        const inspected = [];
        for (const name of files) {
            const fullPath = path.join(tempDir, name);
            try { inspected.push({ path: fullPath, ...(await probeMedia(fullPath)) }); }
            catch (probeError) { console.warn('[media probe skipped]', path.extname(name), probeError.message); }
        }
        const videoFile = inspected.find(f => f.video);
        if (!videoFile) throw new Error('La plataforma no entregó una pista de vídeo válida para esta calidad.');

        let finalPath = videoFile.path;
        if (!videoFile.audio) {
            const audioFile = inspected.find(f => f.audio && !f.video);
            if (!audioFile) throw new Error('Se detectó vídeo, pero no se encontró una pista de audio para combinar.');
            finalPath = await muxVideoAndAudio(videoFile.path, audioFile.path, tempDir, id);
        } else if (path.extname(finalPath).toLowerCase() !== '.mp4') {
            finalPath = await muxVideoAndAudio(finalPath, null, tempDir, id);
        }
        const finalProbe = await probeMedia(finalPath);
        if (!finalProbe.video) throw new Error('El archivo final no contiene vídeo.');
        if (!finalProbe.audio) throw new Error('El archivo final no contiene audio; se canceló la descarga para no entregar un archivo incompleto.');

        const stat = await fs.promises.stat(finalPath);
        if (!stat.size) throw new Error('El archivo generado está vacío.');

        res.setHeader('Content-Type', 'video/mp4');
        res.setHeader(
            'Content-Disposition',
            contentDisposition(info.title, height)
        );
        res.setHeader('Content-Length', stat.size);
        res.setHeader('Cache-Control', 'no-store');

        await pipelineAsync(fs.createReadStream(finalPath), res);
    } catch (error) {
        // Detalle completo solo en el servidor. El usuario recibe un mensaje
        // limpio y sin URLs internas, comandos, cookies o trazas.
        console.error(`[yt-dlp download error] platform=${platform} height=${height}`, error?.stderr || error?.message || error);

        if (!res.headersSent) {
            res.status(500).json({
                error: `No se pudo descargar el vídeo en ${height}p. Prueba otra calidad disponible.`
            });
        }
    } finally {
        await fs.promises.rm(tempDir, { recursive: true, force: true }).catch(() => {});
    }
});

app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Servidor activo en el puerto ${PORT}`);
    console.log(`yt-dlp: ${YTDLP_BINARY}`);
});
