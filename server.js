const express = require('express');
const rateLimit = require('express-rate-limit');
const path = require('path');
const fs = require('fs');
const os = require('os');
const crypto = require('crypto');
const { promisify } = require('util');
const { pipeline, Readable, Transform } = require('stream');
const { spawn } = require('child_process');

const app = express();
const pipelineAsync = promisify(pipeline);
const YTDLP_BINARY = process.env.YTDLP_BINARY || '/usr/local/bin/yt-dlp';
const TIKTOK_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 OPR/118.0.0.0';
const INFO_CACHE_TTL = 2 * 60 * 1000;
const infoCache = new Map();
// URLs multimedia de TikTok quedan solo en el servidor y caducan con la caché.
const tiktokDirectCache = new Map();

// Cola limitada para que las conversiones FFmpeg no saturen el servidor.
const MAX_ACTIVE_DOWNLOADS = Math.max(1, Math.min(4, Number(process.env.MAX_ACTIVE_DOWNLOADS) || 2));
const MAX_QUEUED_DOWNLOADS = 4;
let activeDownloadCount = 0;
const downloadWaiters = [];
const downloadJobs = new Map();

function updateDownloadJob(jobId, patch) {
    if (!/^[a-f0-9]{16,32}$/i.test(String(jobId || ''))) return;
    const now = Date.now();
    for (const [key, job] of downloadJobs) {
        if (now - job.updatedAt > 30 * 60 * 1000) downloadJobs.delete(key);
    }
    const current = downloadJobs.get(jobId) || { createdAt: now, status: 'active', phase: 'en cola', percent: 0 };
    downloadJobs.set(jobId, { ...current, ...patch, updatedAt: now });
}

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

function cookieFileArgs(platform) {
    const cookieFile = platform === 'youtube'
        ? (process.env.YOUTUBE_COOKIES_FILE || process.env.YTDLP_COOKIES_FILE)
        : process.env.YTDLP_COOKIES_FILE;
    return cookieFile ? ['--cookies', cookieFile] : [];
}

function buildBaseArgs(platform) {
    const args = {
        noWarnings: true,
        noPlaylist: true,
        jsRuntimes: 'deno'
    };
    if (platform === 'tiktok') args.userAgent = TIKTOK_USER_AGENT;

    // No fijamos una lista de clientes de YouTube: sus respuestas pueden
    // cambiar y la selección predeterminada de yt-dlp puede adaptarse mejor.
    return args;
}

function isTikTokHost(hostname) {
    const host = String(hostname || '').toLowerCase().replace(/\.$/, '');
    return host === 'tiktok.com' || host.endsWith('.tiktok.com');
}

function isTikTokMediaUrl(value) {
    try {
        const parsed = new URL(value);
        if (parsed.protocol !== 'https:') return false;
        const host = parsed.hostname.toLowerCase();
        return isTikTokHost(host) ||
            host.endsWith('.tiktokcdn.com') ||
            host.endsWith('.tiktokcdn-us.com') ||
            host.endsWith('.byteoversea.com') ||
            host.endsWith('.ibytedtos.com') ||
            host.endsWith('.bytefcdn-oversea.com') ||
            host.endsWith('.tiktokv.com');
    } catch {
        return false;
    }
}

function createTimeoutSignal(parentSignal, timeoutMs) {
    const controller = new AbortController();
    const onAbort = () => controller.abort(parentSignal?.reason);
    const timer = setTimeout(() => controller.abort(new Error('La solicitud superó el tiempo límite.')), timeoutMs);
    timer.unref?.();
    if (parentSignal?.aborted) controller.abort(parentSignal.reason);
    else parentSignal?.addEventListener('abort', onAbort, { once: true });
    return {
        signal: controller.signal,
        cleanup() {
            clearTimeout(timer);
            parentSignal?.removeEventListener('abort', onAbort);
        }
    };
}

function getTikTokHeight(candidate, video) {
    const direct = Number(candidate?.Height || candidate?.height || 0);
    if (Number.isInteger(direct) && direct > 0 && direct <= 4320) return direct;
    const named = String(candidate?.GearName || candidate?.gearName || '').match(/(4320|2160|1440|1080|960|720|540|480|360|240|144)/);
    if (named) return Number(named[1]);
    const fallback = Number(video?.height || 0);
    return Number.isInteger(fallback) && fallback > 0 && fallback <= 4320 ? fallback : 0;
}

// Respaldo inspirado en el extractor web de Cobalt: leer el JSON de rehidratación
// de una página pública y mantener las URLs multimedia únicamente en el backend.
async function getTikTokWebInfo(url, parentSignal) {
    const parsedInput = new URL(url);
    if (!isTikTokHost(parsedInput.hostname)) throw new Error('La URL no pertenece a TikTok.');
    const timeout = createTimeoutSignal(parentSignal, 20000);
    try {
        const response = await fetch(url, {
            redirect: 'follow',
            signal: timeout.signal,
            headers: {
                'user-agent': TIKTOK_USER_AGENT,
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'accept-language': 'en-US,en;q=0.8'
            }
        });
        if (!isTikTokHost(new URL(response.url).hostname)) throw new Error('TikTok redirigió a un dominio inesperado.');
        if (!response.ok) throw new Error('TikTok respondió HTTP ' + response.status + '.');
        if (Number(response.headers.get('content-length') || 0) > 10 * 1024 * 1024) {
            throw new Error('La página de TikTok es demasiado grande para analizarla.');
        }
        const html = await response.text();
        const script = html.match(/<script[^>]*id=["']__UNIVERSAL_DATA_FOR_REHYDRATION__["'][^>]*>([\s\S]*?)<\/script>/i);
        if (!script) throw new Error('TikTok no devolvió los datos de vídeo esperados.');
        const payload = JSON.parse(script[1]);
        const videoDetail = payload?.__DEFAULT_SCOPE__?.['webapp.video-detail'];
        if (!videoDetail || videoDetail.statusMsg) throw new Error('El vídeo no está disponible públicamente o TikTok no entregó sus datos.');
        const detail = videoDetail?.itemInfo?.itemStruct;
        if (!detail?.video) throw new Error('No se encontró una pista de vídeo pública en la página.');
        const video = detail.video;
        const cookieValues = typeof response.headers.getSetCookie === 'function'
            ? response.headers.getSetCookie()
            : [response.headers.get('set-cookie') || ''];
        const cookie = cookieValues.map(value => String(value).split(';')[0]).filter(Boolean).join('; ');
        const referer = response.url;
        const candidates = [];
        for (const item of (Array.isArray(video.bitrateInfo) ? video.bitrateInfo : [])) {
            const urls = item?.PlayAddr?.UrlList || item?.playAddr?.UrlList || item?.playAddr?.urlList || [];
            const mediaUrl = urls.find(isTikTokMediaUrl);
            const height = getTikTokHeight(item, video);
            if (mediaUrl && height) candidates.push({
                height,
                width: Number(item.Width || item.width || video.width || 0),
                fps: Number(item.Fps || item.fps || video.fps || 0),
                bitrate: Number(item.Bitrate || item.bitrate || 0),
                url: mediaUrl
            });
        }
        const playUrls = typeof video.playAddr === 'string'
            ? [video.playAddr]
            : (video.playAddr?.UrlList || video.playAddr?.urlList || []);
        const playUrl = playUrls.find(isTikTokMediaUrl);
        const playHeight = getTikTokHeight({}, video);
        if (playUrl && playHeight) candidates.push({
            height: playHeight, width: Number(video.width || 0), fps: Number(video.fps || 0), bitrate: 0, url: playUrl
        });
        const byDimensions = new Map();
        for (const item of candidates) {
            const key = String(item.width || 0) + 'x' + String(item.height);
            const current = byDimensions.get(key);
            if (!current || item.bitrate > current.bitrate) byDimensions.set(key, item);
        }
        const formats = [...byDimensions.values()].sort((a, b) => b.height - a.height || b.width - a.width || b.bitrate - a.bitrate).map(item => ({
            quality: item.height + 'p',
            height: item.height,
            width: item.width || null,
            fps: item.fps || null,
            ext: 'mp4',
            filesize: null,
            hasAudio: true,
            bitrateKbps: Number(item.bitrate || 0) / 1000,
            formatId: 'tiktok-direct-' + String(item.width || 0) + 'x' + item.height
        }));
        if (!formats.length) throw new Error('TikTok no entregó enlaces multimedia con una resolución verificable.');
        const mediaByFormat = new Map();
        for (const item of byDimensions.values()) mediaByFormat.set('tiktok-direct-' + String(item.width || 0) + 'x' + item.height, {
            url: item.url, cookie, referer
        });
        const now = Date.now();
        for (const [key, entry] of tiktokDirectCache) {
            if (now - entry.time >= INFO_CACHE_TTL) tiktokDirectCache.delete(key);
        }
        tiktokDirectCache.set(url, { time: now, mediaByFormat });
        const author = detail.author || {};
        const stats = detail.stats || {};
        const thumbnail = video.cover || video.originCover || video.dynamicCover || '';
        return {
            title: typeof detail.desc === 'string' && detail.desc.trim() ? detail.desc.trim().slice(0, 300) : 'Vídeo de TikTok',
            thumbnail: Array.isArray(thumbnail) ? thumbnail[0] || '' : String(thumbnail || ''),
            duration: Number(video.duration || 0),
            views: Number(stats.playCount || 0),
            likes: Number(stats.diggCount || 0),
            uploader: author.nickname || author.uniqueId || 'TikTok',
            platform: 'tiktok',
            formats
        };
    } finally {
        timeout.cleanup();
    }
}

async function downloadTikTokDirect(url, formatId, outputTemplate, parentSignal) {
    let entry = tiktokDirectCache.get(url);
    if (!entry || Date.now() - entry.time >= INFO_CACHE_TTL || !entry.mediaByFormat.has(formatId)) {
        await getTikTokWebInfo(url, parentSignal);
        entry = tiktokDirectCache.get(url);
    }
    const media = entry?.mediaByFormat.get(formatId);
    if (!media || !isTikTokMediaUrl(media.url)) throw new Error('El enlace multimedia de TikTok caducó o ya no es válido. Vuelve a analizar el vídeo.');
    const timeout = createTimeoutSignal(parentSignal, 12 * 60 * 1000);
    const baseName = path.basename(outputTemplate).replace('.%(ext)s', '');
    const routeDir = path.join(path.dirname(outputTemplate), 'route-direct');
    const filePath = path.join(routeDir, baseName + '.mp4');
    const startedAt = Date.now();
    try {
        await fs.promises.mkdir(routeDir, { recursive: true });
        const headers = {
            'user-agent': TIKTOK_USER_AGENT,
            'referer': media.referer || 'https://www.tiktok.com/',
            'accept': '*/*'
        };
        if (media.cookie && isTikTokHost(new URL(media.url).hostname)) headers.cookie = media.cookie;
        const response = await fetch(media.url, { headers, signal: timeout.signal, redirect: 'follow' });
        if (!isTikTokMediaUrl(response.url)) throw new Error('El enlace multimedia redirigió a un dominio no permitido.');
        if (!response.ok || !response.body) throw new Error('El servidor multimedia de TikTok respondió HTTP ' + response.status + '.');
        const contentType = String(response.headers.get('content-type') || '').toLowerCase();
        if (contentType.includes('text/html') || contentType.includes('application/json')) {
            throw new Error('TikTok devolvió una página de verificación en lugar del vídeo.');
        }
        await pipelineAsync(Readable.fromWeb(response.body), fs.createWriteStream(filePath), { signal: timeout.signal });
        const stat = await fs.promises.stat(filePath);
        if (!stat.size) throw new Error('TikTok devolvió un archivo vacío.');
        return { route: 'direct', selector: formatId, outputDir: routeDir, attempts: 1, elapsedMs: Date.now() - startedAt };
    } finally {
        timeout.cleanup();
    }
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
        const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 180000;
        let stdout = '';
        let stderr = '';
        let settled = false;
        let timedOut = false;
        let timeoutHandle;
        let killHandle;
        const cleanup = () => {
            clearTimeout(timeoutHandle);
            clearTimeout(killHandle);
            options.signal?.removeEventListener('abort', abortChild);
        };
        const finish = (fn, value) => {
            if (settled) return;
            settled = true;
            cleanup();
            fn(value);
        };
        const abortChild = () => {
            if (child.exitCode === null && !child.killed) {
                child.kill('SIGTERM');
                killHandle = setTimeout(() => {
                    if (child.exitCode === null) child.kill('SIGKILL');
                }, 3000);
                killHandle.unref?.();
            }
        };
        timeoutHandle = setTimeout(() => {
            timedOut = true;
            abortChild();
        }, timeoutMs);
        timeoutHandle.unref?.();
        if (options.signal?.aborted) abortChild();
        else options.signal?.addEventListener('abort', abortChild, { once: true });
        child.stdout.on('data', d => { if (stdout.length < 8 * 1024 * 1024) stdout += d.toString(); });
        child.stderr.on('data', d => { if (stderr.length < 256 * 1024) stderr += d.toString(); });
        child.on('error', error => finish(reject, error));
        child.on('close', code => {
            if (timedOut) {
                const error = new Error(`yt-dlp superó el límite de ${Math.round(timeoutMs / 1000)} segundos.`);
                error.code = 'ETIMEDOUT';
                error.stderr = stderr;
                finish(reject, error);
            } else if (options.signal?.aborted) {
                const error = new Error('La solicitud fue cancelada porque el cliente se desconectó.');
                error.code = 'ABORT_ERR';
                error.stderr = stderr;
                finish(reject, error);
            } else if (code === 0) finish(resolve, { stdout, stderr });
            else {
                const error = new Error(stderr || `yt-dlp terminó con código ${code}`);
                error.stderr = stderr;
                error.code = code;
                finish(reject, error);
            }
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
        filesize: f.filesize || f.filesize_approx || null,
        bitrateKbps: Number(f.tbr || ((Number(f.vbr) || 0) + (Number(f.abr) || 0)) || f.vbr || 0)
    }));

    // Una misma altura puede tener varias anchuras (especialmente en Reels).
    // Mantener cada dimensión distinta y solo deduplicar pistas con las mismas
    // dimensiones, quedándonos con la de mayor bitrate anunciado.
    const byDimensions = new Map();
    for (const format of candidates) {
        const key = String(format.width || 0) + 'x' + String(format.height);
        const current = byDimensions.get(key);
        const score = item => (item.bitrateKbps * 1000) +
            (item.hasAudio ? 100000 : 0) +
            (item.ext === 'mp4' ? 10000 : 0) +
            (item.vcodec.startsWith('avc') ? 1000 : 0) +
            (item.fps || 0) * 10 +
            Math.min(Number(item.filesize || 0) / 100000000, 100);
        if (!current || score(format) > score(current)) byDimensions.set(key, format);
    }

    return [...byDimensions.values()].sort((a, b) =>
        b.height - a.height || b.width - a.width || b.bitrateKbps - a.bitrateKbps
    ).map(f => ({
        quality: String(f.height) + 'p',
        height: f.height,
        width: f.width || null,
        fps: f.fps || null,
        ext: f.ext || 'unknown',
        filesize: f.filesize,
        hasAudio: f.hasAudio,
        bitrateKbps: Number(f.bitrateKbps || 0),
        formatId: f.formatId
    }));
}

function isAllowedGenDownloadStreamUrl(value) {
    try {
        const parsed = new URL(value);
        const hostname = parsed.hostname.toLowerCase().replace(/\.$/, '');
        return parsed.protocol === 'https:' &&
            !parsed.username && !parsed.password &&
            ['gendownload.com', 'dl.gendownload.com'].includes(hostname) &&
            parsed.pathname === '/api/stream' &&
            Boolean(parsed.searchParams.get('t')) &&
            /^\d+$/.test(parsed.searchParams.get('i') || '');
    } catch {
        return false;
    }
}

function getFormatHeight(format) {
    const explicit = Number(format?.height || format?.resolutionHeight || 0);
    if (Number.isInteger(explicit) && explicit > 0 && explicit <= 4320) return explicit;
    const label = String(format?.label || format?.quality || format?.resolution || '');
    const match = /(\d{3,4})\s*p\b/i.exec(label);
    if (match) return Number(match[1]);
    if (/\b4k\b/i.test(label)) return 2160;
    if (/\b2k\b/i.test(label)) return 1440;
    return 0;
}

async function getGenDownloadInfo(url, platform, signal = undefined) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    const abortFromCaller = () => controller.abort();
    if (signal) {
        if (signal.aborted) controller.abort();
        else signal.addEventListener('abort', abortFromCaller, { once: true });
    }

    try {
        const response = await fetch('https://gendownload.com/api/extract', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', accept: 'application/json' },
            body: JSON.stringify({ url }),
            signal: controller.signal
        });
        if (!response.ok) throw new Error('GenDownload respondió HTTP ' + response.status);
        const payload = await response.json();
        if (!payload || typeof payload !== 'object' || !Array.isArray(payload.formats)) {
            throw new Error('GenDownload devolvió una respuesta sin formatos válidos.');
        }

        const providerFormats = [];
        const formats = payload.formats.map((format, index) => {
            const type = String(format?.type || '').toLowerCase();
            const height = getFormatHeight(format);
            const streamUrl = String(format?.url || '');
            if (type !== 'video' || !height || !isAllowedGenDownloadStreamUrl(streamUrl)) return null;
            const ext = String(format?.ext || 'mp4').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 8) || 'mp4';
            const formatId = 'gendownload-' + index;
            providerFormats.push({ formatId, height, url: streamUrl, ext, label: String(format?.label || (height + 'p')).slice(0, 80) });
            return {
                quality: String(format?.label || (height + 'p')).slice(0, 80),
                height,
                width: Number(format?.width || 0) || 0,
                fps: Number(format?.fps || 0) || 0,
                ext,
                filesize: Number.isFinite(Number(format?.filesize)) && Number(format.filesize) > 0 ? Number(format.filesize) : undefined,
                hasAudio: true,
                bitrateKbps: Number(format?.bitrateKbps || 0) || 0,
                formatId
            };
        }).filter(Boolean);

        if (!formats.length) throw new Error('GenDownload no devolvió enlaces de vídeo utilizables.');
        const duration = normalizeDurationSeconds(payload.duration, payload.duration_string, payload.durationString);
        const data = {
            title: typeof payload.title === 'string' && payload.title.trim() ? payload.title.trim().slice(0, 300) : 'Video sin título',
            thumbnail: typeof payload.thumbnail === 'string' && /^https:\/\//i.test(payload.thumbnail) ? payload.thumbnail : '',
            duration,
            views: Number.isFinite(Number(payload.views)) ? Number(payload.views) : 0,
            likes: Number.isFinite(Number(payload.likes)) ? Number(payload.likes) : 0,
            uploader: typeof payload.author === 'string' ? payload.author.slice(0, 200) : 'Desconocido',
            platform,
            formats,
            extractionProvider: 'gendownload'
        };
        return { data, providerFormats };
    } finally {
        clearTimeout(timer);
        if (signal) signal.removeEventListener('abort', abortFromCaller);
    }
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
    const match = /^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$/i.exec(String(value || '').trim());
    if (!match) return 0;
    return (Number(match[1] || 0) * 3600) + (Number(match[2] || 0) * 60) + Number(match[3] || 0);
}

function normalizeDurationSeconds(...values) {
    for (const value of values) {
        if (typeof value === 'number' && Number.isFinite(value) && value >= 0) {
            if (value > 0) return Math.floor(value);
            continue;
        }
        if (typeof value !== 'string') continue;
        const text = value.trim();
        if (!text) continue;
        if (/^PT/i.test(text)) {
            const seconds = parseIsoDuration(text);
            if (seconds > 0) return Math.floor(seconds);
            continue;
        }
        if (/^\d+(?:\.\d+)?$/.test(text)) {
            const seconds = Number(text);
            if (Number.isFinite(seconds) && seconds > 0) return Math.floor(seconds);
            continue;
        }
        if (/^\d{1,3}:\d{1,2}(?::\d{1,2})?$/.test(text)) {
            const parts = text.split(':').map(Number);
            const seconds = parts.length === 3
                ? parts[0] * 3600 + parts[1] * 60 + parts[2]
                : parts[0] * 60 + parts[1];
            if (Number.isFinite(seconds) && seconds > 0) return seconds;
        }
    }
    return 0;
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

async function getInfo(url, platform, forceRefresh = false, signal = undefined) {
    const cached = infoCache.get(url);
    if (!forceRefresh && cached && Date.now() - cached.time < INFO_CACHE_TTL) return cached.data;

    // GenDownload resuelve formatos y enlaces listos para descargar sin convertir
    // el vídeo en Render. Si falla, conservamos yt-dlp y los respaldos existentes.
    try {
        const external = await getGenDownloadInfo(url, platform, signal);
        if (external) {
            const now = Date.now();
            for (const [key, entry] of infoCache) {
                if (now - entry.time >= INFO_CACHE_TTL) infoCache.delete(key);
            }
            infoCache.set(url, { time: now, data: external.data, providerFormats: external.providerFormats });
            console.log('[extract provider success]', JSON.stringify({
                provider: 'gendownload', platform, formats: external.data.formats.length,
                duration: external.data.duration, hasThumbnail: Boolean(external.data.thumbnail)
            }));
            return external.data;
        }
    } catch (error) {
        console.warn('[gendownload extract fallback]', JSON.stringify({
            platform, message: String(error?.message || error).slice(0, 300)
        }));
    }

    let output;
    const extractArgs = [
        '--no-warnings', '--no-playlist', '--no-write-comments', '--dump-single-json', '--skip-download',
        '--js-runtimes', 'deno', '--extractor-retries', '2',
        '--socket-timeout', '15',
        ...cookieFileArgs(platform),
        ...(platform === 'tiktok' ? ['--user-agent', TIKTOK_USER_AGENT] : []),
        '--',
        url
    ];
    try {
        const result = await runBinary(extractArgs, { timeoutMs: 45000, signal });
        output = JSON.parse(result.stdout);
    } catch (firstExtractError) {
        // TikTok y YouTube cambian con frecuencia sus extractores. Un solo
        // reintento tras limpiar la caché evita bucles y reduce carga en Render.
        if (['youtube', 'tiktok'].includes(platform) && firstExtractError.code !== 'ETIMEDOUT' && firstExtractError.code !== 'ABORT_ERR') {
            try {
                await runBinary(['--rm-cache-dir'], { timeoutMs: 15000 });
                const retry = await runBinary(extractArgs, { timeoutMs: 45000, signal });
                output = JSON.parse(retry.stdout);
            } catch (retryError) {
                // TikTok puede devolver el HTML público aunque falle su extractor yt-dlp.
                // Este respaldo mantiene los enlaces multimedia en el backend.
                if (platform === 'tiktok') {
                    try {
                        const directInfo = await getTikTokWebInfo(url, signal);
                        infoCache.set(url, { time: Date.now(), data: directInfo });
                        return directInfo;
                    } catch (fallbackError) {
                        console.warn('[tiktok web fallback failed]', String(fallbackError?.message || fallbackError).slice(0, 400));
                    }
                }
                // La API oficial aporta metadatos, pero no sustituye formatos descargables.
                if (platform === 'youtube' && process.env.YOUTUBE_API_KEY) {
                    try {
                        const apiInfo = await getYouTubeApiInfo(url);
                        if (apiInfo) {
                            infoCache.set(url, { time: Date.now(), data: apiInfo });
                            return apiInfo;
                        }
                    } catch (apiError) {
                        console.warn('[youtube api fallback failed]', apiError.message);
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
    if (!formats.length) {
        if (platform === 'tiktok') {
            try {
                const directInfo = await getTikTokWebInfo(url, signal);
                infoCache.set(url, { time: Date.now(), data: directInfo });
                return directInfo;
            } catch (fallbackError) {
                console.warn('[tiktok web fallback failed]', String(fallbackError?.message || fallbackError).slice(0, 400));
            }
        }
        throw new Error('No se encontraron calidades de vídeo disponibles.');
    }

    const data = {
        title: typeof output.title === 'string' ? output.title : 'Video sin título',
        thumbnail: typeof output.thumbnail === 'string' ? output.thumbnail : '',
        duration: normalizeDurationSeconds(output.duration, output.duration_string, output.release_duration, output.durationString),
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
            bitrateKbps: f.bitrateKbps,
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

app.get('/api/download-status/:jobId', (req, res) => {
    const jobId = String(req.params.jobId || '');
    if (!/^[a-f0-9]{16,32}$/i.test(jobId)) return res.status(400).json({ error: 'ID de descarga no válido.' });
    const job = downloadJobs.get(jobId);
    if (!job || Date.now() - job.updatedAt > 30 * 60 * 1000) {
        downloadJobs.delete(jobId);
        return res.status(404).json({ error: 'No se encontró el estado de esta descarga.' });
    }
    res.setHeader('Cache-Control', 'no-store');
    res.json({
        status: job.status,
        phase: job.phase,
        percent: Number(job.percent || 0),
        message: job.message || '',
        bytesSent: Number(job.bytesSent || 0),
        totalBytes: Number(job.totalBytes || 0)
    });
});

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
        const detail = String(error?.stderr || error?.message || '');
        let publicMessage = 'No se pudo obtener información o formatos válidos de esta URL.';
        if (platform === 'tiktok') {
            publicMessage = 'TikTok no entregó datos multimedia públicos compatibles en este intento. Prueba el enlace directo de la publicación y vuelve a intentarlo.';
        } else if (platform === 'youtube' && /sign in to confirm|not a bot|login required/i.test(detail)) {
            publicMessage = process.env.YOUTUBE_COOKIES_FILE || process.env.YTDLP_COOKIES_FILE
                ? 'YouTube sigue exigiendo verificación con la sesión configurada. Revisa que el archivo de cookies sea válido y esté actualizado.'
                : 'YouTube exige una verificación desde este servidor. Para enlaces a los que tienes acceso, puedes configurar un archivo de cookies autorizado en Render; sin él, algunos vídeos no estarán disponibles.';
        }
        res.status(502).json({ error: publicMessage, platform });
    }
});

function validHeight(value) {
    const n = Number(value);
    return Number.isInteger(n) && n >= 1 && n <= 4320;
}

async function downloadWithHeight(url, platform, selected, outputTemplate, signal = undefined) {
    const height = Number(selected.height);
    const formatId = String(selected.formatId || '');
    if (!Number.isInteger(height) || height < 1) {
        throw new Error('La resolución detectada no es válida. Vuelve a analizar el enlace.');
    }
    if (formatId && !/^[-\w.]+$/.test(formatId)) {
        throw new Error('El formato detectado ya no es válido. Vuelve a analizar el enlace.');
    }

    if (platform === 'tiktok' && formatId.startsWith('tiktok-direct-')) {
        return downloadTikTokDirect(url, formatId, outputTemplate, signal);
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
            ...cookieFileArgs(platform),
            '--no-warnings', '--no-playlist', '--no-part', '--no-overwrites',
            '--retries', '2', '--fragment-retries', '2', '--concurrent-fragments', '4',
            '--format', selector,
            '--output', routeTemplate,
            '--', url
        ];
        const base = buildBaseArgs(platform);
        if (base.jsRuntimes) args.unshift('--js-runtimes', base.jsRuntimes);
        if (base.extractorArgs) args.unshift('--extractor-args', base.extractorArgs);
        if (base.userAgent) args.unshift('--user-agent', base.userAgent);

        try {
            console.log('[download attempt]', JSON.stringify({
                platform, height, attempt: i + 1, maxAttempts: uniqueSelectors.length, selector
            }));
            await runBinary(args, { timeoutMs: 12 * 60 * 1000, signal });
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
const BROWSER_DIRECT_MEDIA_HOSTS = {
    instagram: ['cdninstagram.com', 'fbcdn.net'],
    facebook: ['fbcdn.net', 'fbsbx.com'],
    x: ['twimg.com']
};

function isAllowedBrowserDirectUrl(value, platform) {
    try {
        const parsed = new URL(value);
        if (parsed.protocol !== 'https:' || parsed.username || parsed.password) return false;
        const hostname = parsed.hostname.toLowerCase().replace(/\.$/, '');
        const allowed = BROWSER_DIRECT_MEDIA_HOSTS[platform] || [];
        return allowed.some(domain => hostname === domain || hostname.endsWith('.' + domain));
    } catch {
        return false;
    }
}

// Entrega al navegador una pista ya combinada cuando la plataforma ofrece
// un archivo directo desde un CDN conocido. Si no se puede verificar, se usa
// la ruta normal de Render/FFmpeg. YouTube queda fuera mientras sus enlaces
// requieran verificaciones adicionales.
async function getBrowserDirectUrl(url, platform, selected, signal) {
    if (!['instagram', 'facebook', 'x'].includes(platform)) return null;
    if (!selected?.hasAudio || String(selected.ext || '').toLowerCase() !== 'mp4') return null;
    const formatId = String(selected.formatId || '');
    if (!formatId || !/^[\w.-]{1,200}$/.test(formatId)) return null;

    const base = buildBaseArgs(platform);
    const args = [
        ...cookieFileArgs(platform),
        '--no-warnings', '--no-playlist', '--get-url',
        '--format', formatId,
        ...(base.jsRuntimes ? ['--js-runtimes', base.jsRuntimes] : []),
        ...(base.userAgent ? ['--user-agent', base.userAgent] : []),
        '--', url
    ];
    try {
        const result = await runBinary(args, { timeoutMs: 10000, signal });
        const urls = result.stdout.split(/\r?\n/).map(value => value.trim()).filter(Boolean);
        if (urls.length !== 1 || !isAllowedBrowserDirectUrl(urls[0], platform)) return null;
        return urls[0];
    } catch (error) {
        console.warn('[browser direct unavailable]', JSON.stringify({
            platform,
            message: String(error?.stderr || error?.message || error).slice(0, 250)
        }));
        return null;
    }
}

async function probeMedia(filePath) {
    return new Promise((resolve, reject) => {
        const ffprobe = spawn('ffprobe', [
            '-v', 'error',
            '-show_entries', 'stream=codec_type,codec_name,width,height,bit_rate:format=duration,bit_rate,size',
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
                const probe = JSON.parse(stdout);
                const streams = probe.streams || [];
                const videoStream = streams.find(s => s.codec_type === 'video');
                const audioStream = streams.find(s => s.codec_type === 'audio');
                const streamBitrate = Number(videoStream?.bit_rate || 0);
                const formatBitrate = Number(probe.format?.bit_rate || 0);
                resolve({
                    video: !!videoStream,
                    audio: !!audioStream,
                    width: videoStream ? Number(videoStream.width || 0) : 0,
                    height: videoStream ? Number(videoStream.height || 0) : 0,
                    videoCodec: videoStream ? String(videoStream.codec_name || '') : '',
                    audioCodec: audioStream ? String(audioStream.codec_name || '') : '',
                    bitrateKbps: (streamBitrate || formatBitrate) / 1000,
                    duration: Number(probe.format?.duration || 0)
                });
            } catch {
                reject(new Error('FFprobe devolvió información multimedia inválida.'));
            }
        });
    });
}

// Genera un MP4 reconocible por galerías Android: conserva los streams compatibles
// y recodifica solo cuando el códec no es apropiado para MP4/galerías comunes.
async function finalizeMp4(videoPath, audioPath, tempDir, id, targetHeight = null, jobId = '') {
    const finalPath = path.join(tempDir, id + '-final.mp4');
    const videoProbe = await probeMedia(videoPath);
    if (!videoProbe.video) throw new Error('El archivo elegido no contiene una pista de vídeo.');

    const audioProbe = audioPath ? await probeMedia(audioPath) : videoProbe;
    if (audioPath && !audioProbe.audio) throw new Error('El archivo de audio separado no contiene una pista de audio.');
    if (!audioPath && !videoProbe.audio) throw new Error('No hay una pista de audio para incluir en el MP4.');

    const shouldDownscale = Number.isInteger(Number(targetHeight)) && Number(targetHeight) > 0 && videoProbe.height > Number(targetHeight);
    // AV1 puede conservarse al remultiplexar a MP4; recodificarlo es caro en CPU.
    // Solo reescalar obliga a recodificar el vídeo para respetar la resolución solicitada.
    const videoNeedsTranscode = !['h264', 'av1'].includes(videoProbe.videoCodec) || shouldDownscale;
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
        args.push('-movflags', '+faststart', '-progress', 'pipe:2', '-nostats', finalPath);
        const ffmpeg = spawn('ffmpeg', args, { stdio: ['ignore', 'ignore', 'pipe'] });
        let stderr = '';
        let progressBuffer = '';
        ffmpeg.stderr.on('data', d => {
            const chunk = d.toString();
            stderr += chunk;
            progressBuffer += chunk;
            const lines = progressBuffer.split(String.fromCharCode(10));
            progressBuffer = lines.pop() || '';
            for (const rawLine of lines) {
                const line = rawLine.trim();
                if (!line.startsWith('out_time_ms=') || !jobId || !(videoProbe.duration > 0)) continue;
                const elapsedSeconds = Number(line.slice(12)) / 1000000;
                const fraction = Math.max(0, Math.min(1, elapsedSeconds / videoProbe.duration));
                const percent = Math.max(5, Math.min(94, Math.floor(5 + fraction * 89)));
                updateDownloadJob(jobId, { status: 'active', phase: 'preparando MP4', percent, message: 'Convirtiendo vídeo: ' + Math.floor(fraction * 100) + '%' });
            }
        });
        ffmpeg.on('error', reject);
        ffmpeg.on('close', code => code === 0 ? resolve() : reject(new Error(stderr || 'FFmpeg no pudo preparar el MP4 final.')));
    });
    return finalPath;
}

app.post('/api/download-best', downloadLimiter, async (req, res) => {
    const url = normalizeUrl(req.body?.url);
    const height = Number(req.body?.height);
    const requestedFormatId = String(req.body?.formatId || '');

    if (!url) return res.status(400).json({ error: 'URL no válida.' });
    if (!validHeight(height)) {
        return res.status(400).json({ error: 'Calidad no válida. Vuelve a analizar el vídeo.' });
    }
    if (requestedFormatId && !/^[\w.-]{1,200}$/.test(requestedFormatId)) {
        return res.status(400).json({ error: 'La pista seleccionada no es válida. Vuelve a analizar el vídeo.' });
    }

    const platform = detectPlatform(url);
    const jobId = String(req.body?.jobId || '');
    if (jobId && !/^[a-f0-9]{16,32}$/i.test(jobId)) return res.status(400).json({ error: 'ID de descarga no válido.' });
    if (jobId) updateDownloadJob(jobId, { status: 'active', phase: 'en cola', percent: 0, message: 'Esperando un espacio de descarga.' });
    const requestStartedAt = Date.now();
    let clientDisconnected = false;
    const downloadAbortController = new AbortController();
    res.on('close', () => {
        if (!res.writableEnded) {
            clientDisconnected = true;
            downloadAbortController.abort();
            console.warn('[download client disconnected]', JSON.stringify({ platform, height, elapsedMs: Date.now() - requestStartedAt }));
        }
    });
    let slot;
    try {
        slot = await acquireDownloadSlot();
    } catch (error) {
        updateDownloadJob(jobId, { status: 'error', phase: 'error', message: error.message });
        return res.status(error.statusCode || 503).json({ error: error.message });
    }
    updateDownloadJob(jobId, { status: 'active', phase: 'analizando', percent: 0, message: 'Comprobando el enlace y las calidades disponibles.' });
    console.log('[download queue]', JSON.stringify({ platform, height, waitMs: slot.waitMs, active: activeDownloadCount, queued: downloadWaiters.length, maxActive: MAX_ACTIVE_DOWNLOADS }));
    let tempDir;
    const id = crypto.randomBytes(8).toString('hex');
    try {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'mediagrab-'));
        const outputTemplate = path.join(tempDir, `${id}.%(ext)s`);
        // No devolvemos datos privados del extractor al cliente.
        // Solo usamos la información pública ya filtrada para obtener título.
        // Volver a consultar los formatos para no descargar con un formatId obsoleto.
        const info = await getInfo(url, platform, false, downloadAbortController.signal);
        const requestedFormat = requestedFormatId
            ? info.formats.find(f => String(f.formatId) === requestedFormatId && Number(f.height) === height)
            : null;
        const exact = info.formats.find(f => Number(f.height) === height);
        const lowerOrEqual = info.formats.filter(f => Number(f.height) <= height).sort((a, b) => Number(b.height) - Number(a.height));
        const nearest = [...info.formats].sort((a, b) => Math.abs(Number(a.height) - height) - Math.abs(Number(b.height) - height));
        const selected = requestedFormat || exact || lowerOrEqual[0] || nearest[0];

        if (!selected) {
            throw new Error('La plataforma no ha entregado formatos de vídeo utilizables.');
        }

        const selectedHeight = Number(selected.height);
        console.log('[download selection]', JSON.stringify({
            platform, requestedHeight: height, selectedHeight,
            requestedFormatId: requestedFormatId || null,
            selectedFormatId: selected.formatId,
            selectedWidth: Number(selected.width || 0),
            selectedBitrateKbps: Number(selected.bitrateKbps || 0),
            exactFormat: Boolean(requestedFormat),
            extractionProvider: String(selected.formatId || '').startsWith('gendownload-') ? 'gendownload' : 'yt-dlp'
        }));

        // Los enlaces de GenDownload se guardan solo en la caché del servidor.
        // Validamos host, ruta y token antes de redirigir para evitar SSRF/URLs inyectadas.
        if (String(selected.formatId || '').startsWith('gendownload-')) {
            const cachedExtraction = infoCache.get(url);
            const providerFormat = cachedExtraction?.providerFormats?.find(item =>
                item.formatId === selected.formatId && Number(item.height) === selectedHeight
            );
            if (!providerFormat || !isAllowedGenDownloadStreamUrl(providerFormat.url)) {
                throw new Error('El enlace de descarga de GenDownload caducó o no pudo validarse. Analiza el vídeo de nuevo.');
            }
            updateDownloadJob(jobId, {
                status: 'completed', phase: 'descarga directa del proveedor', percent: 100,
                message: 'Enlace de descarga listo. El navegador recibirá el archivo directamente.'
            });
            res.setHeader('Cache-Control', 'no-store');
            console.log('[provider direct delivery]', JSON.stringify({
                provider: 'gendownload', platform, selectedHeight, ext: providerFormat.ext,
                sourceHost: new URL(providerFormat.url).hostname,
                elapsedMs: Date.now() - requestStartedAt
            }));
            res.redirect(302, providerFormat.url);
            return;
        }

        const browserDirectUrl = await getBrowserDirectUrl(url, platform, selected, downloadAbortController.signal);
        if (browserDirectUrl) {
            const sourceHost = new URL(browserDirectUrl).hostname;
            updateDownloadJob(jobId, {
                status: 'completed', phase: 'descarga directa en el navegador', percent: 100,
                message: 'El navegador recibe el vídeo directamente desde la plataforma. Revisa la carpeta Descargas.'
            });
            res.setHeader('Cache-Control', 'no-store');
            console.log('[mobile direct delivery]', JSON.stringify({
                platform, selectedHeight, sourceHost, elapsedMs: Date.now() - requestStartedAt
            }));
            res.redirect(302, browserDirectUrl);
            return;
        }

        updateDownloadJob(jobId, { status: 'active', phase: 'descargando desde la plataforma', percent: 0, message: 'La plataforma está enviando el archivo al servidor.' });
        const downloadResult = await downloadWithHeight(url, platform, selected, outputTemplate, downloadAbortController.signal);
        updateDownloadJob(jobId, { status: 'active', phase: 'preparando MP4', percent: 0, message: 'Validando audio, vídeo y compatibilidad con Android.' });
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
            ['h264', 'av1'].includes(videoFile.videoCodec) &&
            videoFile.audioCodec === 'aac' &&
            videoFile.height === expectedHeightForFastPath;

        // Evitar recodificar MP4 con H.264 o AV1 y audio AAC si la resolución ya coincide.
        // Recodificar vídeos largos/4K consume mucha CPU y no mejora el detalle original.
        // AV1 en MP4 es compatible con dispositivos modernos; equipos antiguos podrían no reproducirlo.
        if (isAlreadyCompatibleMp4) {
            finalPath = videoFile.path;
            agentDecision = videoFile.videoCodec === 'av1'
                ? 'entrega_directa_mp4_av1_sin_recodificar'
                : 'entrega_directa_mp4_h264_sin_recodificar';
        } else if (!videoFile.audio) {
            const audioFile = inspected.find(f => f.audio && !f.video);
            if (!audioFile) throw new Error('Se detectó vídeo, pero no se encontró una pista de audio para combinar.');
            finalPath = await finalizeMp4(videoFile.path, audioFile.path, tempDir, id, height, jobId);
        } else {
            finalPath = await finalizeMp4(videoFile.path, null, tempDir, id, height, jobId);
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
        const selectedWidth = Number(selected.width || 0);
        const expectedWidth = selectedWidth > 0 && selectedHeight > 0
            ? Math.round(selectedWidth * expectedHeight / selectedHeight)
            : 0;
        if (expectedWidth > 0 && Math.abs(finalProbe.width - expectedWidth) > 2) {
            throw new Error(`El archivo final quedó en ${finalProbe.width}×${finalProbe.height}, pero la pista elegida indicaba aproximadamente ${expectedWidth}×${expectedHeight}. Se canceló para evitar entregar una resolución distinta.`);
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
        updateDownloadJob(jobId, { status: 'active', phase: 'enviando al dispositivo', percent: 0, bytesSent: 0, totalBytes: stat.size, message: 'Descargando el archivo en tu dispositivo.' });

        console.log('[mini-agent decision]', JSON.stringify({ platform, requestedHeight: height, selectedHeight, selectedWidth: Number(selected.width || 0), selectedBitrateKbps: Number(selected.bitrateKbps || 0), decision: agentDecision, videoCodec: videoFile.videoCodec, audioCodec: videoFile.audioCodec, sourceWidth: videoFile.width, sourceHeight: videoFile.height, sourceBitrateKbps: Number(videoFile.bitrateKbps || 0), finalWidth: Number(finalProbe.width || 0), finalHeight: Number(finalProbe.height || 0), finalBitrateKbps: Number(finalProbe.bitrateKbps || 0), durationSeconds: Number(finalProbe.duration || 0), downloadMs: downloadResult.elapsedMs, preparationAndFinalizationMs: Math.max(0, phaseAfterFinalizeMs - downloadResult.elapsedMs), bytes: stat.size }));
        let bytesSent = 0;
        const progressStream = new Transform({
            transform(chunk, encoding, callback) {
                bytesSent += chunk.length;
                updateDownloadJob(jobId, {
                    status: 'active', phase: 'enviando al dispositivo',
                    percent: stat.size ? Math.min(99, Math.floor(bytesSent * 100 / stat.size)) : 0,
                    bytesSent, totalBytes: stat.size,
                    message: 'Descargando el archivo en tu dispositivo.'
                });
                callback(null, chunk);
            }
        });
        await pipelineAsync(fs.createReadStream(finalPath), progressStream, res);
        updateDownloadJob(jobId, { status: 'completed', phase: 'completado', percent: 100, bytesSent: stat.size, totalBytes: stat.size, message: 'El archivo se envió al navegador. Comprueba la carpeta Descargas.' });
        console.log('[download delivered]', JSON.stringify({ platform, requestedHeight: height, actualWidth: finalProbe.width, actualHeight: finalProbe.height, bytes: stat.size, elapsedMs: Date.now() - requestStartedAt, clientDisconnected }));
    } catch (error) {
        // Detalle completo solo en el servidor. El usuario recibe un mensaje
        // limpio y sin URLs internas, comandos, cookies o trazas.
        console.error('[download failed]', JSON.stringify({ platform, height, elapsedMs: Date.now() - requestStartedAt, headersSent: res.headersSent, clientDisconnected, message: String(error?.stderr || error?.message || error).slice(0, 1200) }));
        updateDownloadJob(jobId, { status: clientDisconnected ? 'cancelled' : 'error', phase: clientDisconnected ? 'cancelado' : 'error', percent: 0, message: clientDisconnected ? 'La descarga se canceló al cerrar la conexión.' : 'No se pudo completar la descarga.' });

        if (!res.headersSent) {
            const detail = String(error?.stderr || error?.message || '');
            let publicMessage;
            if (/sign in to confirm|not a bot|login required|private video|HTTP Error 403|403 Forbidden/i.test(detail)) {
                publicMessage = 'La plataforma bloqueó el acceso o exige verificación. Comprueba que el enlace sea público y accesible desde el navegador.';
            } else if (platform === 'tiktok' && /Unexpected response|TikTok|fetch.fail|multimedia/i.test(detail)) {
                publicMessage = 'TikTok cambió o bloqueó la respuesta multimedia. El respaldo directo tampoco pudo validar un archivo; prueba otra publicación pública más tarde.';
            } else if (/requested format is not available|no suitable formats|no video formats|format .* not available|does not pass filter/i.test(detail)) {
                publicMessage = `La plataforma cambió los formatos disponibles y no se pudo obtener ${height}p. Vuelve a analizar el enlace para actualizar las calidades.`;
            } else if (/ffmpeg|ffprobe|mux|codec|moov atom|invalid data/i.test(detail)) {
                publicMessage = 'Se encontró contenido, pero falló la validación o conversión del vídeo. El sistema no entregó un archivo incompleto; vuelve a intentarlo.';
            } else {
                publicMessage = `No se pudo completar la descarga en ${height}p después de probar alternativas. Vuelve a analizar el enlace o prueba otra calidad disponible.`;
            }
            updateDownloadJob(jobId, { status: 'error', phase: 'error', message: publicMessage });
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
        if (tempDir) await fs.promises.rm(tempDir, { recursive: true, force: true }).catch(() => {});
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
