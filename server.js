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

async function getInfo(url, platform, forceRefresh = false) {
    const cached = infoCache.get(url);
    if (!forceRefresh && cached && Date.now() - cached.time < INFO_CACHE_TTL) return cached.data;

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
    if (!Number.isInteger(height) || height < 1) {
        throw new Error('La resolución detectada no es válida. Vuelve a analizar el enlace.');
    }
    if (formatId && !/^[-\w.]+$/.test(formatId)) {
        throw new Error('El formato detectado ya no es válido. Vuelve a analizar el enlace.');
    }

    // Reintentar con estrategias alternativas de la misma altura antes de fallar.
    // Dejamos que FFmpeg genere el MP4 compatible después de verificar las pistas.
    const selectors = [];
    if (formatId && selected.hasAudio) selectors.push(formatId);
    if (formatId && !selected.hasAudio) selectors.push(formatId + '+bestaudio/best[height=' + height + ']');
    selectors.push('best[height=' + height + '][vcodec!=none][acodec!=none]');
    selectors.push('bestvideo[height=' + height + '][vcodec!=none]+bestaudio/best[height=' + height + ']');
    const uniqueSelectors = [...new Set(selectors)];
    const baseName = path.basename(outputTemplate).replace('.%(ext)s', '');
    let lastError;

    for (let i = 0; i < uniqueSelectors.length; i++) {
        const selector = uniqueSelectors[i];
        const attemptTemplate = path.join(path.dirname(outputTemplate), baseName + '.attempt' + (i + 1) + '.%(ext)s');
        const args = [
            '--no-warnings', '--no-playlist', '--no-part', '--no-overwrites',
            '--format', selector,
            '--output', attemptTemplate,
            '--', url
        ];
        const base = buildBaseArgs(platform);
        if (base.jsRuntimes) args.unshift('--js-runtimes', base.jsRuntimes);
        if (base.extractorArgs) args.unshift('--extractor-args', base.extractorArgs);

        try {
            await runBinary(args);
            const dir = path.dirname(outputTemplate);
            const prefix = baseName + '.attempt' + (i + 1) + '.';
            const names = (await fs.promises.readdir(dir)).filter(name =>
                name.startsWith(prefix) && !name.endsWith('.part') && !name.endsWith('.ytdl')
            );
            const inspected = [];
            for (const name of names) {
                try {
                    inspected.push(await probeMedia(path.join(dir, name)));
                } catch {}
            }
            if (!inspected.some(media => media.video)) {
                throw new Error('El selector terminó sin generar una pista de vídeo válida.');
            }
            if (!inspected.some(media => media.audio)) {
                throw new Error('El selector no generó una pista de audio válida.');
            }
            return { attempts: i + 1, selector };
        } catch (error) {
            lastError = error;
            console.warn('[download retry]', JSON.stringify({ platform, height, attempt: i + 1, message: String(error?.message || error).slice(0, 500) }));
            const dir = path.dirname(outputTemplate);
            const prefix = baseName + '.attempt' + (i + 1) + '.';
            const leftovers = await fs.promises.readdir(dir).catch(() => []);
            await Promise.all(leftovers.filter(name => name.startsWith(prefix)).map(name =>
                fs.promises.rm(path.join(dir, name), { force: true }).catch(() => {})
            ));
        }
    }

    throw lastError || new Error('No se pudo descargar ninguna combinación de formatos disponible.');
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
        if (videoNeedsTranscode) args.push('-c:v', 'libx264', '-preset', 'veryfast', '-crf', '20');
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
    const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'mediagrab-'));
    const id = crypto.randomBytes(8).toString('hex');
    const outputTemplate = path.join(tempDir, `${id}.%(ext)s`);

    try {
        // No devolvemos datos privados del extractor al cliente.
        // Solo usamos la información pública ya filtrada para obtener título.
        // Volver a consultar los formatos para no descargar con un formatId obsoleto.
        const info = await getInfo(url, platform, true);
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
        console.log(`[download] selector_attempts=${downloadResult.attempts} platform=${platform} selected=${selectedHeight}p`);

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

        let finalPath;
        if (!videoFile.audio) {
            const audioFile = inspected.find(f => f.audio && !f.video);
            if (!audioFile) throw new Error('Se detectó vídeo, pero no se encontró una pista de audio para combinar.');
            finalPath = await finalizeMp4(videoFile.path, audioFile.path, tempDir, id, height);
        } else {
            finalPath = await finalizeMp4(videoFile.path, null, tempDir, id, height);
        }

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

        await pipelineAsync(fs.createReadStream(finalPath), res);
    } catch (error) {
        // Detalle completo solo en el servidor. El usuario recibe un mensaje
        // limpio y sin URLs internas, comandos, cookies o trazas.
        console.error(`[yt-dlp download error] platform=${platform} height=${height}`, error?.stderr || error?.message || error);

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
            res.status(500).json({ error: publicMessage });
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
