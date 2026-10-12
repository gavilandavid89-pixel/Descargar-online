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

// Cola limitada para que las conversiones FFmpeg no saturen el servidor.
const MAX_ACTIVE_DOWNLOADS = Math.max(1, Math.min(4, Number(process.env.MAX_ACTIVE_DOWNLOADS) || 2));
const MAX_QUEUED_DOWNLOADS = 4;
let activeDownloadCount = 0;
const downloadWaiters = [];

function acquireDownloadSlot() {
    if (activeDownloadCount < MAX_ACTIVE_DOWNLOADS) {
        activeDownloadCount++;
        return Promise.resolve({ waitMs: 0, release: releaseDownloadSlot });
    }
    if (downloadWaiters.length >= MAX_QUEUED_DOWNLOADS) {
        const error = new Error('El servidor está ocupado. Espera unos segundos y vuelve a intentarlo.');
        error.statusCode = 503;
        return Promise.reject(error);
    }
    const queuedAt = Date.now();
    return new Promise(resolve => downloadWaiters.push(() => {
        resolve({ waitMs: Date.now() - queuedAt, release: releaseDownloadSlot });
    }));
}

function releaseDownloadSlot() {
    const next = downloadWaiters.shift();
    if (next) next();
    else activeDownloadCount = Math.max(0, activeDownloadCount - 1);
}

app.use(express.json({ limit: '32kb' }));
app.use(express.urlencoded({ extended: false, limit: '32kb' }));
app.use(express.static(path.join(__dirname, 'public')));
app.set('trust proxy', 1);

const infoLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 30,
    skipFailedRequests: true,
    message: { error: 'Demasiadas consultas correctas desde esta IP. Inténtalo más tarde.' }
});

const downloadLimiter = rateLimit({
    windowMs: 60 * 60 * 1000,
    max: 5,
    // No descontar el cupo por intentos que terminan en HTTP 4xx/5xx.
    skipFailedRequests: true,
    message: { error: 'Has alcanzado el límite de descargas correctas por hora.' }
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

function escapeHtml(value) {
    return String(value || '').replace(/[&<>"']/g, ch => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[ch]);
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

function runBinary(args, options = {}) {
    return new Promise((resolve, reject) => {
        const child = spawn(YTDLP_BINARY, args, { stdio: ['ignore', 'pipe', 'pipe'] });
        let stdout = '';
        let stderr = '';
        let settled = false;
        const cleanup = () => options.signal?.removeEventListener('abort', abortChild);
        const finish = (fn, value) => {
            if (settled) return;
            settled = true;
            cleanup();
            fn(value);
        };
        const abortChild = () => {
            if (!child.killed) child.kill('SIGTERM');
        };
        if (options.signal?.aborted) abortChild();
        else options.signal?.addEventListener('abort', abortChild, { once: true });
        child.stdout.on('data', d => stdout += d.toString());
        child.stderr.on('data', d => stderr += d.toString());
        child.on('error', error => finish(reject, error));
        child.on('close', code => {
            if (options.signal?.aborted) {
                finish(reject, new Error('Ruta cancelada porque otra ruta terminó primero.'));
            } else if (code === 0) finish(resolve, { stdout, stderr });
            else finish(reject, new Error(stderr || `yt-dlp terminó con código ${code}`));
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

async function getInfo(url, platform, forceRefresh = false) {
    const cached = infoCache.get(url);
    if (!forceRefresh && cached && Date.now() - cached.time < INFO_CACHE_TTL) return cached.data;

    let output;
    const extractArgs = {
        ...buildBaseArgs(platform),
        dumpSingleJson: true,
        skipDownload: true,
        getComments: false
    };
    try {
        output = await ytDlp(url, extractArgs);
    } catch (firstExtractError) {
        // YouTube cambia con frecuencia el código del reproductor embebido
        // en la página. Si la extracción falla, limpiamos la caché de yt-dlp
        // una sola vez y reintentamos antes de devolver el error.
        if (platform === 'youtube') {
            try {
                await runBinary(['--rm-cache-dir']);
                output = await ytDlp(url, extractArgs);
            } catch (retryError) {
                // La API oficial solo aporta metadatos; no sustituye la
                // extracción de formatos ni evita restricciones de acceso.
                if (process.env.YOUTUBE_API_KEY) {
                    const apiInfo = await getYouTubeApiInfo(url);
                    if (apiInfo) {
                        infoCache.set(url, { time: Date.now(), data: apiInfo });
                        return apiInfo;
                    }
                }
                throw retryError || firstExtractError;
            }
        } else {
            throw firstExtractError;
        }
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

    const now = Date.now();
    for (const [key, entry] of infoCache) {
        if (now - entry.time >= INFO_CACHE_TTL) infoCache.delete(key);
    }
    infoCache.set(url, { time: now, data });
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
    if (!Number.isInteger(height) || height < 1) {
        throw new Error('La resolución detectada no es válida. Vuelve a analizar el enlace.');
    }
    if (formatId && !/^[-\w.]+$/.test(formatId)) {
        throw new Error('El formato detectado ya no es válido. Vuelve a analizar el enlace.');
    }

    // Una ruta principal y un único respaldo secuencial. No consumir CPU/red
    // lanzando varias extracciones simultáneas para la misma descarga.
    const selectors = [];
    if (formatId && selected.hasAudio) selectors.push(formatId);
    if (formatId && !selected.hasAudio) {
        selectors.push(formatId + '+bestaudio/best[height=' + height + ']');
    }
    selectors.push('best[height=' + height + '][vcodec!=none][acodec!=none]/best[height=' + height + ']');
    const uniqueSelectors = [...new Set(selectors)].slice(0, 2);
    const baseName = path.basename(outputTemplate).replace('.%(ext)s', '');
    const parentDir = path.dirname(outputTemplate);
    const startedAt = Date.now();
    let lastError;

    for (let i = 0; i < uniqueSelectors.length; i++) {
        const selector = uniqueSelectors[i];
        const routeDir = path.join(parentDir, 'route-' + (i + 1));
        await fs.promises.mkdir(routeDir, { recursive: true });
        const routeTemplate = path.join(routeDir, baseName + '.%(ext)s');
        const args = [
            '--no-warnings', '--no-playlist', '--no-part', '--no-overwrites',
            '--retries', '2', '--fragment-retries', '2', '--concurrent-fragments', '4',
            '--format', selector,
            '--output', routeTemplate,
            '--', url
        ];
        const base = buildBaseArgs(platform);
        if (base.jsRuntimes) args.unshift('--js-runtimes', base.jsRuntimes);
        if (base.extractorArgs) args.unshift('--extractor-args', base.extractorArgs);

        try {
            console.log('[download attempt]', JSON.stringify({
                platform, height, attempt: i + 1, maxAttempts: uniqueSelectors.length, selector
            }));
            await runBinary(args);
            const names = (await fs.promises.readdir(routeDir)).filter(name =>
                !name.endsWith('.part') && !name.endsWith('.ytdl') && !name.endsWith('.json')
            );
            const inspected = [];
            for (const name of names) {
                try {
                    inspected.push({
                        path: path.join(routeDir, name),
                        ...(await probeMedia(path.join(routeDir, name)))
                    });
                } catch {}
            }
            if (!inspected.some(media => media.video) || !inspected.some(media => media.audio)) {
                throw new Error('La ruta terminó, pero no generó pistas válidas de vídeo y audio.');
            }
            console.log('[download route success]', JSON.stringify({
                platform, height, attempt: i + 1, elapsedMs: Date.now() - startedAt
            }));
            return { route: i + 1, selector, outputDir: routeDir, attempts: i + 1, elapsedMs: Date.now() - startedAt };
        } catch (error) {
            lastError = error;
            const detail = String(error?.stderr || error?.message || error);
            console.warn('[download route failure]', JSON.stringify({
                platform, height, attempt: i + 1, elapsedMs: Date.now() - startedAt,
                message: detail.slice(0, 350)
            }));
            // Un bloqueo de acceso/verificación no se arregla repitiendo otra
            // selección de formato; detenerse para no alargar ni cargar el servicio.
            if (/sign in to confirm|not a bot|login required|private video|HTTP Error 403|403 Forbidden|confirm you.re not a bot/i.test(detail)) {
                break;
            }
        }
    }
    throw new Error('Falló la ruta principal y su único respaldo. ' + String(lastError?.message || lastError || 'Error desconocido'));
}
async function probeMedia(filePath) {
    return new Promise((resolve, reject) => {
        const ffprobe = spawn('ffprobe', [
            '-v', 'error',
            '-show_entries', 'stream=codec_type,codec_name,width,height',
            '-of', 'json',
            filePath
        ], { stdio: ['ignore', 'pipe', 'pipe'] });
        let stdout = '';
        let stderr = '';
        ffprobe.stdout.on('data', d => stdout += d.toString());
        ffprobe.stderr.on('data', d => stderr += d.toString());
        ffprobe.on('error', reject);
        ffprobe.on('close', code => {
            if (code !== 0) return reject(new Error(stderr || 'No se pudo verificar el archivo multimedia.'));
            try {
                const streams = JSON.parse(stdout).streams || [];
                const videoStream = streams.find(s => s.codec_type === 'video');
                const audioStream = streams.find(s => s.codec_type === 'audio');
                resolve({
                    video: !!videoStream,
                    audio: !!audioStream,
                    width: videoStream ? Number(videoStream.width || 0) : 0,
                    height: videoStream ? Number(videoStream.height || 0) : 0,
                    videoCodec: videoStream ? String(videoStream.codec_name || '') : '',
                    audioCodec: audioStream ? String(audioStream.codec_name || '') : ''
                });
            } catch {
                reject(new Error('FFprobe devolvió información multimedia inválida.'));
            }
        });
    });
}

// Genera un MP4 reconocible por galerías Android: conserva los streams compatibles
// y recodifica solo cuando el códec no es apropiado para MP4/galerías comunes.
async function finalizeMp4(videoPath, audioPath, tempDir, id, targetHeight = null) {
    const finalPath = path.join(tempDir, id + '-final.mp4');
    const videoProbe = await probeMedia(videoPath);
    if (!videoProbe.video) throw new Error('El archivo elegido no contiene una pista de vídeo.');

    const audioProbe = audioPath ? await probeMedia(audioPath) : videoProbe;
    if (audioPath && !audioProbe.audio) throw new Error('El archivo de audio separado no contiene una pista de audio.');
    if (!audioPath && !videoProbe.audio) throw new Error('No hay una pista de audio para incluir en el MP4.');

    const shouldDownscale = Number.isInteger(Number(targetHeight)) && Number(targetHeight) > 0 && videoProbe.height > Number(targetHeight);
    const videoNeedsTranscode = !['h264'].includes(videoProbe.videoCodec) || shouldDownscale;
    const audioCodec = audioPath ? audioProbe.audioCodec : videoProbe.audioCodec;
    const audioNeedsTranscode = audioCodec !== 'aac';

    await new Promise((resolve, reject) => {
        const args = ['-hide_banner', '-loglevel', 'error', '-y', '-i', videoPath];
        if (audioPath) args.push('-i', audioPath);
        args.push('-map', '0:v:0');
        if (audioPath) args.push('-map', '1:a:0');
        else args.push('-map', '0:a:0');
        if (shouldDownscale) args.push('-vf', 'scale=-2:' + Number(targetHeight));
        if (videoNeedsTranscode) args.push('-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '22');
        else args.push('-c:v', 'copy');
        if (audioNeedsTranscode) args.push('-c:a', 'aac', '-b:a', '192k');
        else args.push('-c:a', 'copy');
        args.push('-movflags', '+faststart', finalPath);
        const ffmpeg = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
        let stderr = '';
        ffmpeg.stderr.on('data', d => stderr += d.toString());
        ffmpeg.on('error', reject);
        ffmpeg.on('close', code => code === 0 ? resolve() : reject(new Error(stderr || 'FFmpeg no pudo preparar el MP4 final.')));
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
    const requestStartedAt = Date.now();
    let clientDisconnected = false;
    res.on('close', () => {
        if (!res.writableEnded) {
            clientDisconnected = true;
            console.warn('[download client disconnected]', JSON.stringify({ platform, height, elapsedMs: Date.now() - requestStartedAt }));
        }
    });
    let slot;
    try {
        slot = await acquireDownloadSlot();
    } catch (error) {
        return res.status(error.statusCode || 503).json({ error: error.message });
    }
    console.log('[download queue]', JSON.stringify({ platform, height, waitMs: slot.waitMs, active: activeDownloadCount, queued: downloadWaiters.length, maxActive: MAX_ACTIVE_DOWNLOADS }));
    const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'mediagrab-'));
    const id = crypto.randomBytes(8).toString('hex');
    const outputTemplate = path.join(tempDir, `${id}.%(ext)s`);

    try {
        // No devolvemos datos privados del extractor al cliente.
        // Solo usamos la información pública ya filtrada para obtener título.
        // Volver a consultar los formatos para no descargar con un formatId obsoleto.
        const info = await getInfo(url, platform);
        const exact = info.formats.find(f => Number(f.height) === height);
        const lowerOrEqual = info.formats.filter(f => Number(f.height) <= height).sort((a, b) => Number(b.height) - Number(a.height));
        const nearest = [...info.formats].sort((a, b) => Math.abs(Number(a.height) - height) - Math.abs(Number(b.height) - height));
        const selected = exact || lowerOrEqual[0] || nearest[0];

        if (!selected) {
            throw new Error('La plataforma no ha entregado formatos de vídeo utilizables.');
        }

        const selectedHeight = Number(selected.height);
        console.log(`[download] platform=${platform} requested=${height}p selected=${selectedHeight}p exact=${Boolean(exact)}`);

        const downloadResult = await downloadWithHeight(url, platform, selected, outputTemplate);
        console.log(`[download winner] platform=${platform} requested=${height}p selected=${selectedHeight}p route=${downloadResult.route} selector=${downloadResult.selector}`);

        const mediaExtensions = new Set(['.mp4', '.m4v', '.mov', '.webm', '.mkv', '.m4a', '.mp3', '.aac', '.opus', '.ogg', '.ts']);
        const files = (await fs.promises.readdir(downloadResult.outputDir)).filter(name => {
            const lower = name.toLowerCase();
            return !lower.endsWith('.part') && !lower.endsWith('.ytdl') && !lower.endsWith('.json') && mediaExtensions.has(path.extname(lower));
        });
        if (!files.length) throw new Error('yt-dlp no generó un archivo multimedia.');

        // Inspeccionar cada archivo: no asumir que el primer MP4 es el vídeo.
        const inspected = [];
        for (const name of files) {
            const fullPath = path.join(downloadResult.outputDir, name);
            try { inspected.push({ path: fullPath, ...(await probeMedia(fullPath)) }); }
            catch (probeError) { console.warn('[media probe skipped]', path.extname(name), probeError.message); }
        }
        const videoFile = inspected.find(f => f.video);
        if (!videoFile) throw new Error('La plataforma no entregó una pista de vídeo válida para esta calidad.');

        let finalPath;
        let agentDecision = 'normalizar_mp4';
        const expectedHeightForFastPath = selectedHeight > height ? height : selectedHeight;
        const isAlreadyCompatibleMp4 = videoFile.audio &&
            path.extname(videoFile.path).toLowerCase() === '.mp4' &&
            videoFile.videoCodec === 'h264' && videoFile.audioCodec === 'aac' &&
            videoFile.height === expectedHeightForFastPath;

        // Mini agente v1: evita FFmpeg si el archivo ya cumple el formato final.
        // La descarga ya se validó con ffprobe y conserva audio, vídeo y resolución.
        if (isAlreadyCompatibleMp4) {
            finalPath = videoFile.path;
            agentDecision = 'entrega_directa_mp4_compatible';
        } else if (!videoFile.audio) {
            const audioFile = inspected.find(f => f.audio && !f.video);
            if (!audioFile) throw new Error('Se detectó vídeo, pero no se encontró una pista de audio para combinar.');
            finalPath = await finalizeMp4(videoFile.path, audioFile.path, tempDir, id, height);
        } else {
            finalPath = await finalizeMp4(videoFile.path, null, tempDir, id, height);
        }

        const phaseAfterFinalizeMs = Date.now() - requestStartedAt;
        const finalProbe = await probeMedia(finalPath);
        if (!finalProbe.video) throw new Error('El archivo final no contiene vídeo.');
        if (!finalProbe.audio) throw new Error('El archivo final no contiene audio; se canceló la descarga para no entregar un archivo incompleto.');
        // No permitir que una plataforma entregue silenciosamente otra resolución.
        const expectedHeight = selectedHeight > height ? height : selectedHeight;
        if (finalProbe.height !== expectedHeight) {
            throw new Error(`El archivo final quedó en ${finalProbe.height}p, pero se esperaba ${expectedHeight}p. Se canceló para no etiquetar mal la calidad.`);
        }

        const stat = await fs.promises.stat(finalPath);
        if (!stat.size) throw new Error('El archivo generado está vacío.');

        res.setHeader('Content-Type', 'video/mp4');
        res.setHeader(
            'Content-Disposition',
            contentDisposition(info.title, finalProbe.height)
        );
        res.setHeader('X-Requested-Height', String(height));
        res.setHeader('X-Actual-Height', String(finalProbe.height));
        res.setHeader('Content-Length', stat.size);
        res.setHeader('Cache-Control', 'no-store');

        console.log('[mini-agent decision]', JSON.stringify({ platform, requestedHeight: height, selectedHeight, decision: agentDecision, videoCodec: videoFile.videoCodec, audioCodec: videoFile.audioCodec, sourceHeight: videoFile.height, downloadMs: downloadResult.elapsedMs, preparationAndFinalizationMs: Math.max(0, phaseAfterFinalizeMs - downloadResult.elapsedMs), bytes: stat.size }));
        await pipelineAsync(fs.createReadStream(finalPath), res);
        console.log('[download delivered]', JSON.stringify({ platform, requestedHeight: height, actualHeight: finalProbe.height, bytes: stat.size, elapsedMs: Date.now() - requestStartedAt, clientDisconnected }));
    } catch (error) {
        // Detalle completo solo en el servidor. El usuario recibe un mensaje
        // limpio y sin URLs internas, comandos, cookies o trazas.
        console.error('[download failed]', JSON.stringify({ platform, height, elapsedMs: Date.now() - requestStartedAt, headersSent: res.headersSent, clientDisconnected, message: String(error?.stderr || error?.message || error).slice(0, 1200) }));

        if (!res.headersSent) {
            const detail = String(error?.stderr || error?.message || '');
            let publicMessage;
            if (/sign in to confirm|not a bot|login required|private video|HTTP Error 403|403 Forbidden/i.test(detail)) {
                publicMessage = 'La plataforma bloqueó el acceso o exige verificación. Comprueba que el enlace sea público y accesible desde el navegador.';
            } else if (/requested format is not available|no suitable formats|no video formats|format .* not available|does not pass filter/i.test(detail)) {
                publicMessage = `La plataforma cambió los formatos disponibles y no se pudo obtener ${height}p. Vuelve a analizar el enlace para actualizar las calidades.`;
            } else if (/ffmpeg|ffprobe|mux|codec|moov atom|invalid data/i.test(detail)) {
                publicMessage = 'Se encontró contenido, pero falló la validación o conversión del vídeo. El sistema no entregó un archivo incompleto; vuelve a intentarlo.';
            } else {
                publicMessage = `No se pudo completar la descarga en ${height}p después de probar alternativas. Vuelve a analizar el enlace o prueba otra calidad disponible.`;
            }
            if (req.accepts('html')) {
                res.status(500).type('html').send(
                    '<!doctype html><html lang="es"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' +
                    '<title>Descarga no completada</title><body style="font-family:system-ui;background:#0f172a;color:#e2e8f0;padding:2rem;max-width:40rem;margin:auto">' +
                    '<h2>La descarga no se pudo completar</h2><p>' + escapeHtml(publicMessage) + '</p>' +
                    '<p><a style="color:#a5b4fc" href="/">Volver a MediaGrab</a></p></body></html>'
                );
            } else {
                res.status(500).json({ error: publicMessage });
            }
        }
    } finally {
        await fs.promises.rm(tempDir, { recursive: true, force: true }).catch(() => {});
        slot.release();
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
