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
const INFO_CACHE_MAX = 100;
const infoCache = new Map();

app.use(express.json({ limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.disable('x-powered-by');
app.set('trust proxy', process.env.TRUST_PROXY === '1');
app.use((req, res, next) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
    next();
});

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
    if (typeof value !== 'string') return '';
    const raw = value.trim();
    if (!raw || raw.length > 2048) return '';
    try {
        const parsed = new URL(raw);
        if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') return '';
        parsed.hash = '';
        return parsed.toString();
    } catch {
        return '';
    }
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
        callHome: false,
        jsRuntimes: 'deno'
    };

    // Solo aplicamos argumentos de YouTube cuando realmente estamos con YouTube.
    if (platform === 'youtube') {
        args.extractorArgs = 'youtube:player_client=mweb,tv,web_safari';
    }

    return args;
}

function safeFilename(name) {
    const clean = String(name || 'video')
        .replace(/[<>:"/\\|?*\x00-\x1F]/g, '_')
        .replace(/\s+/g, ' ')
        .trim();
    return (clean || 'video').slice(0, 120);
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

    // Una sola opción por altura. La descarga posterior vuelve a resolver
    // esa altura en el servidor, evitando depender de IDs que pueden cambiar.
    const candidates = all
        .filter(f =>
            f.vcodec && f.vcodec !== 'none' &&
            Number(f.height || 0) > 0
        )
        .map(f => ({
            height: Number(f.height),
            width: Number(f.width || 0),
            fps: Number(f.fps || 0),
            ext: String(f.ext || '').toLowerCase(),
            vcodec: String(f.vcodec || ''),
            acodec: String(f.acodec || ''),
            hasAudio: !!(f.acodec && f.acodec !== 'none'),
            filesize: f.filesize || f.filesize_approx || null
        }));

    const byHeight = new Map();

    for (const f of candidates) {
        const current = byHeight.get(f.height);
        const score = item =>
            (item.ext === 'mp4' ? 1000 : 0) +
            (item.vcodec.startsWith('avc') ? 200 : 0) +
            (item.hasAudio ? 100 : 0) +
            (item.fps || 0);

        if (!current || score(f) > score(current)) {
            byHeight.set(f.height, f);
        }
    }

    const heights = [...byHeight.keys()].sort((a, b) => b - a);

    return heights.map(height => {
        const f = byHeight.get(height);
        return {
            quality: `${height}p`,
            height,
            width: f.width || null,
            fps: f.fps || null,
            ext: 'mp4',
            filesize: f.filesize,
            hasAudio: f.hasAudio,
            // La descarga se resuelve por altura, no por format_id.
            // Esto evita IDs obsoletos y garantiza una única calidad.
            selector: `bestvideo[height=${height}]+bestaudio/best[height=${height}]`
        };
    });
}

async function getInfo(url, platform) {
    const cached = infoCache.get(url);
    if (cached && Date.now() - cached.time < INFO_CACHE_TTL) return cached.data;

    const output = await ytDlp(url, {
        ...buildBaseArgs(platform),
        dumpSingleJson: true,
        skipDownload: true,
        getComments: false,
        noCheckCertificates: false,
        verbose: false
    });

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
            hasAudio: f.hasAudio
        }))
    };

    infoCache.set(url, { time: Date.now(), data });
    while (infoCache.size > INFO_CACHE_MAX) {
        infoCache.delete(infoCache.keys().next().value);
    }
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
    if (platform === 'unknown' || platform === 'generic') {
        return res.status(400).json({ error: 'URL no compatible. Usa YouTube, TikTok, Instagram, Facebook o X.' });
    }

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

async function downloadWithHeight(url, platform, height, outputTemplate) {
    // Exactamente una altura. yt-dlp elegirá una sola pista de vídeo de esa
    // altura y, si hace falta, una sola pista de audio.
    const selector = `bestvideo[height=${height}]+bestaudio/best[height=${height}]`;

    const args = [
        '--no-warnings',
        '--no-playlist',
        '--no-part',
        '--no-overwrites',
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

async function remuxIfNeeded(sourcePath, tempDir, id) {
    if (sourcePath.toLowerCase().endsWith('.mp4')) return sourcePath;

    const finalPath = path.join(tempDir, `${id}.mp4`);
    await new Promise((resolve, reject) => {
        const ffmpeg = spawn('ffmpeg', [
            '-hide_banner', '-loglevel', 'error', '-y',
            '-i', sourcePath,
            '-map', '0:v:0',
            '-map', '0:a:0?',
            '-c', 'copy',
            '-movflags', '+faststart',
            finalPath
        ], { stdio: ['ignore', 'ignore', 'pipe'] });

        let stderr = '';
        ffmpeg.stderr.on('data', d => stderr += d.toString());
        ffmpeg.on('error', reject);
        ffmpeg.on('close', code => {
            if (code === 0) resolve();
            else reject(new Error(stderr || 'FFmpeg no pudo crear el MP4.'));
        });
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
    if (platform === 'unknown' || platform === 'generic') {
        return res.status(400).json({ error: 'URL no compatible. Usa YouTube, TikTok, Instagram, Facebook o X.' });
    }
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

        await downloadWithHeight(url, platform, height, outputTemplate);

        const files = (await fs.promises.readdir(tempDir))
            .filter(name =>
                !name.endsWith('.part') &&
                !name.endsWith('.ytdl') &&
                !name.endsWith('.json')
            );

        if (!files.length) throw new Error('No se generó ningún archivo descargable.');

        const preferred = files.find(f => f.toLowerCase().endsWith('.mp4')) || files[0];
        const sourcePath = path.join(tempDir, preferred);
        const finalPath = await remuxIfNeeded(sourcePath, tempDir, id);

        const stat = await fs.promises.stat(finalPath);
        if (!stat.size) throw new Error('El archivo generado está vacío.');

        res.setHeader('Content-Type', 'video/mp4');
        res.setHeader(
            'Content-Disposition',
            `attachment; filename="${safeFilename(info.title)}_${height}p.mp4"`
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
