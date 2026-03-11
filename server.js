const express = require('express');
const http = require('http');
const https = require('https');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar');
const multer = require('multer');
const os = require('os');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const LOG_DIR = path.join(os.homedir(), '.claude-code-router/logs');
const UPLOAD_DIR = path.join(__dirname, 'uploads');
const PROXY_TARGET = process.env.CLAUDE_PROXY_TARGET;
const PROXY_LOG_DIR = process.env.CLAUDE_PROXY_LOG_DIR || UPLOAD_DIR;
const DEFAULT_TAIL_BYTES = Number.parseInt(process.env.CLAUDE_LOG_TAIL_BYTES || '2000000', 10);
const DEFAULT_LINE_READ_BYTES = Number.parseInt(process.env.CLAUDE_LOG_LINE_MAX_BYTES || '5000000', 10);
const DEFAULT_PROXY_LOG_BODY_BYTES = Number.parseInt(process.env.CLAUDE_PROXY_LOG_BODY_BYTES || '2000000', 10);

if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR, { recursive: true });
}
if (!fs.existsSync(UPLOAD_DIR)) {
    fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

function resolveProxyLogDir() {
    const candidates = [PROXY_LOG_DIR, UPLOAD_DIR];
    for (const dir of candidates) {
        try {
            fs.mkdirSync(dir, { recursive: true });
            const testPath = path.join(dir, '.__claude_proxy_write_test');
            const fd = fs.openSync(testPath, 'a');
            fs.closeSync(fd);
            fs.unlinkSync(testPath);
            return dir;
        } catch (e) {
        }
    }
    return UPLOAD_DIR;
}

const PROXY_LOG_DIR_RESOLVED = resolveProxyLogDir();

function decodeFilename(filename) {
    try {
        return Buffer.from(filename, 'latin1').toString('utf8');
    } catch (e) {
        return filename;
    }
}

const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, UPLOAD_DIR);
    },
    filename: (req, file, cb) => {
        const decodedName = decodeFilename(file.originalname);
        cb(null, decodedName);
    }
});

const upload = multer({
    storage: storage,
    fileFilter: (req, file, cb) => {
        const decodedName = decodeFilename(file.originalname);
        if (decodedName.endsWith('.log')) {
            cb(null, true);
        } else {
            cb(new Error('Only .log files are allowed'));
        }
    }
});

app.use(express.static(path.join(__dirname, 'public')));
app.use('/claude/proxy', express.raw({ type: '*/*', limit: '50mb' }));
app.use(express.json());

function redactHeaders(headers) {
    const redacted = {};
    const sensitive = new Set([
        'authorization',
        'proxy-authorization',
        'x-api-key',
        'api-key',
        'x-api-token',
        'x-auth-token'
    ]);
    Object.entries(headers || {}).forEach(([key, value]) => {
        if (sensitive.has(key.toLowerCase())) {
            redacted[key] = '[REDACTED]';
        } else {
            redacted[key] = value;
        }
    });
    return redacted;
}

function redactJson(value) {
    if (Array.isArray(value)) {
        return value.map(item => redactJson(item));
    }
    if (value && typeof value === 'object') {
        const result = {};
        Object.entries(value).forEach(([key, val]) => {
            const lower = key.toLowerCase();
            if (['api_key', 'apikey', 'token', 'secret', 'authorization', 'access_token', 'refresh_token'].includes(lower)) {
                result[key] = '[REDACTED]';
            } else {
                result[key] = redactJson(val);
            }
        });
        return result;
    }
    return value;
}

function serializeBody(buffer, contentType) {
    if (!buffer || buffer.length === 0) {
        return null;
    }
    const ct = (contentType || '').toLowerCase();
    const isJson = ct.includes('application/json') || ct.includes('+json');
    const isText =
        ct.startsWith('text/') ||
        ct.includes('text/event-stream') ||
        ct.includes('application/x-ndjson') ||
        ct.includes('application/xml') ||
        ct.includes('application/xhtml+xml') ||
        ct.includes('application/x-www-form-urlencoded');

    if (isJson) {
        const text = buffer.toString('utf8');
        try {
            const json = JSON.parse(text);
            return { type: 'json', size: buffer.length, value: redactJson(json) };
        } catch (e) {
            return { type: 'text', size: buffer.length, value: text };
        }
    }
    if (isText) {
        const text = buffer.toString('utf8');
        const limit = Number.parseInt(process.env.CLAUDE_PROXY_LOG_TEXT_LIMIT || '200000', 10);
        if (Number.isFinite(limit) && limit > 0 && text.length > limit) {
            return { type: 'text', size: buffer.length, truncated: true, value: text.slice(0, limit) };
        }
        return { type: 'text', size: buffer.length, value: text };
    }
    const text = buffer.toString('utf8');
    const limit = Number.parseInt(process.env.CLAUDE_PROXY_LOG_TEXT_LIMIT || '200000', 10);
    if (Number.isFinite(limit) && limit > 0 && text.length > limit) {
        return { type: 'text', size: buffer.length, truncated: true, value: text.slice(0, limit) };
    }
    return { type: 'text', size: buffer.length, value: text };
}

function appendJsonl(filePath, record) {
    fs.appendFile(filePath, JSON.stringify(record) + '\n', (error) => {
        if (error) {
            console.error('Error writing proxy log:', error);
        }
    });
}

function formatUtc8HalfHour(date) {
    const d = new Date(date.getTime() + 8 * 60 * 60 * 1000);
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mi = d.getUTCMinutes();
    const half = mi < 30 ? '00' : '30';
    return `${yyyy}-${mm}-${dd}-${hh}-${half}`;
}

function sanitizeForFilename(value) {
    return String(value || '')
        .replace(/\s+/g, '')
        .replace(/[^0-9a-zA-Z._-]/g, '_')
        .slice(0, 200) || 'unknown';
}

function getClientIp(req) {
    const xff = req.headers['x-forwarded-for'];
    if (typeof xff === 'string' && xff.trim()) {
        return xff.split(',')[0].trim();
    }
    const xri = req.headers['x-real-ip'];
    if (typeof xri === 'string' && xri.trim()) {
        return xri.trim();
    }
    const remote = req.socket && req.socket.remoteAddress;
    if (typeof remote === 'string' && remote.trim()) {
        return remote.startsWith('::ffff:') ? remote.slice(7) : remote;
    }
    return 'unknown';
}

function buildTargetUrl(req) {
    const prefix = '/claude/proxy';
    let pathWithQuery = req.originalUrl.startsWith(prefix)
        ? req.originalUrl.slice(prefix.length)
        : req.originalUrl;
    if (pathWithQuery === '') {
        pathWithQuery = '/';
    }
    if (!pathWithQuery.startsWith('/')) {
        pathWithQuery = `/${pathWithQuery}`;
    }
    return new URL(pathWithQuery, PROXY_TARGET);
}

function getRequestBodyBuffer(req) {
    if ((req.method === 'GET' || req.method === 'HEAD') && (!req.headers['content-length'] || req.headers['content-length'] === '0')) {
        return Buffer.alloc(0);
    }
    if (req.body !== undefined) {
        if (Buffer.isBuffer(req.body)) {
            return req.body;
        }
        if (typeof req.body === 'string') {
            return Buffer.from(req.body);
        }
        return Buffer.from(JSON.stringify(req.body));
    }
    return new Promise((resolve, reject) => {
        const chunks = [];
        req.on('data', chunk => chunks.push(chunk));
        req.on('end', () => resolve(Buffer.concat(chunks)));
        req.on('error', reject);
    });
}

function parseSseText(text) {
    const normalized = String(text || '').replace(/\r\n/g, '\n');
    const blocks = normalized.split('\n\n').filter(b => b.trim());
    const events = [];

    for (const block of blocks) {
        const lines = block.split('\n').filter(l => l.trim() && !l.startsWith(':'));
        let event;
        let id;
        let retry;
        const dataLines = [];

        for (const line of lines) {
            const idx = line.indexOf(':');
            if (idx === -1) {
                continue;
            }
            const field = line.slice(0, idx).trim();
            let value = line.slice(idx + 1);
            if (value.startsWith(' ')) {
                value = value.slice(1);
            }
            if (field === 'event') {
                event = value;
            } else if (field === 'data') {
                dataLines.push(value);
            } else if (field === 'id') {
                id = value;
            } else if (field === 'retry') {
                const n = Number.parseInt(value, 10);
                retry = Number.isFinite(n) ? n : value;
            }
        }

        if (!event && dataLines.length === 0 && id === undefined && retry === undefined) {
            continue;
        }

        const dataText = dataLines.join('\n');
        let data = dataText;
        if (dataText && dataText !== '[DONE]') {
            try {
                data = JSON.parse(dataText);
            } catch (e) {
            }
        }

        const evt = { event, data };
        if (id !== undefined) {
            evt.id = id;
        }
        if (retry !== undefined) {
            evt.retry = retry;
        }
        events.push(evt);
    }

    return { type: 'sse', events };
}

function formatLoggedBody(entry) {
    if (!entry || !entry.body) {
        return entry;
    }
    const headers = entry.headers || {};
    const ct = String(headers['content-type'] || headers['Content-Type'] || '').toLowerCase();
    const isJson = ct.includes('application/json') || ct.includes('+json');
    const isSse = ct.includes('text/event-stream');

    if (entry.body.type === 'base64' && typeof entry.body.value === 'string') {
        try {
            const text = Buffer.from(entry.body.value, 'base64').toString('utf8');
            const limit = Number.parseInt(process.env.CLAUDE_PROXY_LOG_TEXT_LIMIT || '200000', 10);
            const truncated = Number.isFinite(limit) && limit > 0 && text.length > limit;
            const textBody = truncated
                ? { type: 'text', size: entry.body.size, decodedFrom: 'base64', truncated: true, value: text.slice(0, limit) }
                : { type: 'text', size: entry.body.size, decodedFrom: 'base64', value: text };

            let nextEntry = { ...entry, body: textBody };
            if (isSse) {
                const parsed = parseSseText(textBody.value);
                nextEntry.body = { ...parsed, size: entry.body.size, decodedFrom: 'base64', truncated: textBody.truncated };
                return nextEntry;
            }
            if (isJson) {
                try {
                    const json = JSON.parse(textBody.value);
                    nextEntry.body = { type: 'json', size: entry.body.size, decodedFrom: 'base64', truncated: textBody.truncated, value: redactJson(json) };
                } catch (e) {
                }
            }
            return nextEntry;
        } catch (e) {
            return entry;
        }
    }

    if (entry.body.type === 'text' && typeof entry.body.value === 'string') {
        if (isSse) {
            const parsed = parseSseText(entry.body.value);
            return { ...entry, body: { ...parsed, size: entry.body.size, truncated: entry.body.truncated } };
        }
        if (isJson) {
            try {
                const json = JSON.parse(entry.body.value);
                return { ...entry, body: { type: 'json', size: entry.body.size, truncated: entry.body.truncated, value: redactJson(json) } };
            } catch (e) {
            }
        }
    }

    return entry;
}

function generateRequestId() {
    try {
        const { randomUUID } = require('crypto');
        return randomUUID();
    } catch (e) {
        return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }
}

app.all('/claude/proxy*', async (req, res) => {
    const requestId = generateRequestId();
    const start = Date.now();
    const clientIp = getClientIp(req);
    const logHalfHour = formatUtc8HalfHour(new Date());
    const logFilePath = path.join(PROXY_LOG_DIR_RESOLVED, `claude-${sanitizeForFilename(clientIp)}-${logHalfHour}.log`);
    const targetUrl = buildTargetUrl(req);
    const bodyBuffer = await getRequestBodyBuffer(req);
    const headers = { ...req.headers };
    delete headers.host;
    delete headers['content-length'];
    headers['accept-encoding'] = 'identity';
    if (bodyBuffer && bodyBuffer.length > 0) {
        headers['content-length'] = Buffer.byteLength(bodyBuffer);
    }
    const options = {
        protocol: targetUrl.protocol,
        hostname: targetUrl.hostname,
        port: targetUrl.port || (targetUrl.protocol === 'https:' ? 443 : 80),
        method: req.method,
        path: `${targetUrl.pathname}${targetUrl.search}`,
        headers
    };

    const maxLoggedBytes = Number.isFinite(DEFAULT_PROXY_LOG_BODY_BYTES) && DEFAULT_PROXY_LOG_BODY_BYTES > 0 ? DEFAULT_PROXY_LOG_BODY_BYTES : 2000000;
    const requestLogBuffer = bodyBuffer && bodyBuffer.length > maxLoggedBytes ? bodyBuffer.subarray(0, maxLoggedBytes) : bodyBuffer;
    const requestBodySerialized = serializeBody(requestLogBuffer, req.headers['content-type']);
    const requestBodyRecord = requestBodySerialized && bodyBuffer && bodyBuffer.length > maxLoggedBytes
        ? { ...requestBodySerialized, truncatedBytes: true, originalSize: bodyBuffer.length }
        : requestBodySerialized;

    appendJsonl(logFilePath, {
        type: 'request',
        id: requestId,
        time: new Date().toISOString(),
        clientIp,
        method: req.method,
        url: req.originalUrl,
        target: targetUrl.toString(),
        headers: redactHeaders(req.headers),
        body: requestBodyRecord
    });

    const client = targetUrl.protocol === 'https:' ? https : http;
    const proxyReq = client.request(options, (proxyRes) => {
        const responseChunks = [];
        let responseBytes = 0;
        let responseLoggedBytes = 0;
        let responseTruncated = false;
        res.status(proxyRes.statusCode || 502);
        Object.entries(proxyRes.headers || {}).forEach(([key, value]) => {
            if (value !== undefined) {
                res.setHeader(key, value);
            }
        });
        proxyRes.on('data', chunk => {
            responseBytes += chunk.length;
            if (!responseTruncated) {
                const remaining = maxLoggedBytes - responseLoggedBytes;
                if (remaining > 0) {
                    const part = chunk.length > remaining ? chunk.subarray(0, remaining) : chunk;
                    responseChunks.push(part);
                    responseLoggedBytes += part.length;
                } else {
                    responseTruncated = true;
                }
                if (responseBytes > maxLoggedBytes) {
                    responseTruncated = true;
                }
            }
            res.write(chunk);
        });
        proxyRes.on('end', () => {
            res.end();
            const responseBuffer = Buffer.concat(responseChunks);
            const responseBodySerialized = serializeBody(responseBuffer, proxyRes.headers && proxyRes.headers['content-type']);
            const responseBodyRecord = responseBodySerialized && responseTruncated
                ? { ...responseBodySerialized, truncatedBytes: true, originalSize: responseBytes }
                : responseBodySerialized;
            appendJsonl(logFilePath, {
                type: 'response',
                id: requestId,
                time: new Date().toISOString(),
                durationMs: Date.now() - start,
                status: proxyRes.statusCode,
                headers: redactHeaders(proxyRes.headers || {}),
                body: responseBodyRecord
            });
        });
    });

    proxyReq.on('error', (error) => {
        appendJsonl(logFilePath, {
            type: 'response',
            id: requestId,
            time: new Date().toISOString(),
            durationMs: Date.now() - start,
            status: 502,
            error: { message: error.message }
        });
        res.status(502).json({ error: 'Proxy request failed', message: error.message });
    });

    if (bodyBuffer && bodyBuffer.length > 0) {
        proxyReq.write(bodyBuffer);
    }
    proxyReq.end();
});

function isProxyLogFilename(name) {
    return /^claude-.*\.log$/i.test(name);
}

function getUploadDirSourceForFilename(name) {
    if (PROXY_LOG_DIR_RESOLVED === UPLOAD_DIR && isProxyLogFilename(name)) {
        return 'proxy';
    }
    return 'upload';
}

function getLogFiles() {
    const files = [];
    
    try {
        if (fs.existsSync(LOG_DIR)) {
            const logFiles = fs.readdirSync(LOG_DIR)
                .filter(file => file.endsWith('.log'))
                .map(file => {
                    const filePath = path.join(LOG_DIR, file);
                    const stats = fs.statSync(filePath);
                    return {
                        name: file,
                        path: filePath,
                        size: stats.size,
                        mtime: stats.mtime.toISOString(),
                        source: 'system'
                    };
                });
            files.push(...logFiles);
        }
    } catch (error) {
        console.error('Error reading log directory:', error);
    }
    
    try {
        if (fs.existsSync(UPLOAD_DIR)) {
            const uploadFiles = fs.readdirSync(UPLOAD_DIR)
                .filter(file => file.endsWith('.log'))
                .map(file => {
                    const filePath = path.join(UPLOAD_DIR, file);
                    const stats = fs.statSync(filePath);
                    return {
                        name: file,
                        path: filePath,
                        size: stats.size,
                        mtime: stats.mtime.toISOString(),
                        source: getUploadDirSourceForFilename(file)
                    };
                });
            files.push(...uploadFiles);
        }
    } catch (error) {
        console.error('Error reading upload directory:', error);
    }
    
    return files.sort((a, b) => new Date(b.mtime) - new Date(a.mtime));
}

function resolveLogFilePath(filename, source) {
    if (source === 'upload') {
        return path.join(UPLOAD_DIR, filename);
    }
    if (source === 'proxy') {
        return path.join(PROXY_LOG_DIR_RESOLVED, filename);
    }
    return path.join(LOG_DIR, filename);
}

function trimLeadingPartialLine(buffer, startOffset) {
    if (startOffset <= 0) {
        return { buffer, startOffset };
    }
    const newlineIndex = buffer.indexOf('\n');
    if (newlineIndex === -1) {
        return { buffer: Buffer.alloc(0), startOffset: startOffset + buffer.length };
    }
    return {
        buffer: buffer.slice(newlineIndex + 1),
        startOffset: startOffset + newlineIndex + 1
    };
}

function countNewlinesBeforeOffset(filePath, offset) {
    if (offset <= 0) return 0;
    const fd = fs.openSync(filePath, 'r');
    const CHUNK = 65536;
    let count = 0;
    let pos = 0;
    const buf = Buffer.alloc(Math.min(CHUNK, offset));
    while (pos < offset) {
        const toRead = Math.min(CHUNK, offset - pos);
        fs.readSync(fd, buf, 0, toRead, pos);
        for (let i = 0; i < toRead; i++) {
            if (buf[i] === 0x0A) count++;
        }
        pos += toRead;
    }
    fs.closeSync(fd);
    return count;
}

function readFileTail(filePath, tailBytes) {
    const stats = fs.statSync(filePath);
    const size = stats.size;
    const maxRead = Number.isFinite(tailBytes) && tailBytes > 0 ? tailBytes : size;
    const bytesToRead = Math.min(maxRead, size);
    const startOffset = Math.max(0, size - bytesToRead);
    const fd = fs.openSync(filePath, 'r');
    const buffer = Buffer.alloc(bytesToRead);
    fs.readSync(fd, buffer, 0, bytesToRead, startOffset);
    fs.closeSync(fd);
    const trimmed = trimLeadingPartialLine(buffer, startOffset);
    const skippedLines = countNewlinesBeforeOffset(filePath, trimmed.startOffset);
    return {
        content: trimmed.buffer.toString('utf8'),
        startOffset: trimmed.startOffset,
        truncated: size > bytesToRead,
        size,
        skippedLines
    };
}

function readFileSegment(filePath, startOffset, maxBytes) {
    const stats = fs.statSync(filePath);
    const size = stats.size;
    const safeStart = Math.max(0, Math.min(startOffset || 0, size));
    const maxRead = Number.isFinite(maxBytes) && maxBytes > 0 ? maxBytes : size;
    const bytesToRead = Math.min(maxRead, size - safeStart);
    const fd = fs.openSync(filePath, 'r');
    const buffer = Buffer.alloc(bytesToRead);
    fs.readSync(fd, buffer, 0, bytesToRead, safeStart);
    fs.closeSync(fd);
    const trimmed = trimLeadingPartialLine(buffer, safeStart);
    const skippedLines = countNewlinesBeforeOffset(filePath, trimmed.startOffset);
    return {
        content: trimmed.buffer.toString('utf8'),
        startOffset: trimmed.startOffset,
        truncated: safeStart > 0 || size > (trimmed.startOffset + trimmed.buffer.length),
        size,
        skippedLines
    };
}

function getFileContent(filename, source, options = {}) {
    try {
        const filePath = resolveLogFilePath(filename, source);
        if (!fs.existsSync(filePath)) {
            return null;
        }
        if (options.full) {
            return {
                content: fs.readFileSync(filePath, 'utf-8'),
                startOffset: 0,
                truncated: false,
                size: fs.statSync(filePath).size
            };
        }
        if (Number.isFinite(options.startOffset) && options.startOffset >= 0) {
            return readFileSegment(filePath, options.startOffset, DEFAULT_LINE_READ_BYTES);
        }
        const tailBytes = Number.isFinite(options.tailBytes) ? options.tailBytes : DEFAULT_TAIL_BYTES;
        return readFileTail(filePath, tailBytes);
    } catch (error) {
        console.error('Error reading file:', error);
        return null;
    }
}

app.get('/api/files', (req, res) => {
    const files = getLogFiles();
    res.json(files);
});

app.get('/api/files/:filename', (req, res) => {
    const filename = req.params.filename;
    const source = req.query.source || 'system';
    if (!filename.endsWith('.log')) {
        return res.status(400).json({ error: 'Invalid file type' });
    }
    const full = req.query.full === '1';
    const tailBytes = Number.parseInt(req.query.tailBytes, 10);
    const contentData = getFileContent(filename, source, { full, tailBytes });
    if (contentData === null) {
        return res.status(404).json({ error: 'File not found' });
    }
    res.json({ filename, content: contentData.content, source, startOffset: contentData.startOffset, truncated: contentData.truncated, size: contentData.size, skippedLines: contentData.skippedLines || 0 });
});

app.get('/api/files/:filename/line/:line', (req, res) => {
    const filename = req.params.filename;
    const lineNum = parseInt(req.params.line, 10);
    const source = req.query.source || 'system';

    if (!filename.endsWith('.log')) {
        return res.status(400).json({ error: 'Invalid file type' });
    }
    if (isNaN(lineNum) || lineNum < 1) {
        return res.status(400).json({ error: 'Invalid line number' });
    }

    const filePath = resolveLogFilePath(filename, source);
    if (!filePath || !fs.existsSync(filePath)) {
        return res.status(404).json({ error: 'File not found' });
    }

    // Read the file line by line to find the target line without loading entire file
    const readline = require('readline');
    const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
        crlfDelay: Infinity
    });
    let currentLine = 0;
    let found = false;
    rl.on('line', (line) => {
        currentLine++;
        if (currentLine === lineNum) {
            found = true;
            rl.close();
            if (!line.trim()) {
                return res.status(404).json({ error: 'Line is empty' });
            }
            try {
                let json = JSON.parse(line);
                json = formatLoggedBody(json);
                res.setHeader('Content-Type', 'application/json; charset=utf-8');
                res.send(JSON.stringify(json, null, 2));
            } catch (e) {
                res.setHeader('Content-Type', 'text/plain; charset=utf-8');
                res.send(line);
            }
        }
    });
    rl.on('close', () => {
        if (!found) {
            res.status(404).json({ error: 'Line not found' });
        }
    });
    rl.on('error', (err) => {
        console.error('Error reading line:', err);
        res.status(500).json({ error: 'Failed to read file' });
    });
});

app.delete('/api/files/:filename', (req, res) => {
    const filename = req.params.filename;
    const source = req.query.source || 'system';
    if (!filename.endsWith('.log')) {
        return res.status(400).json({ error: 'Invalid file type' });
    }
    
    const filePath = resolveLogFilePath(filename, source);
    
    if (!fs.existsSync(filePath)) {
        return res.status(404).json({ error: 'File not found' });
    }
    
    if (source === 'upload' || source === 'proxy') {
        try {
            fs.unlinkSync(filePath);
            broadcast({
                type: 'files',
                data: getLogFiles()
            });
            res.json({ success: true, message: 'File deleted' });
        } catch (error) {
            console.error('Error deleting file:', error);
            res.status(500).json({ error: 'Failed to delete file: ' + error.message });
        }
    } else {
        const { exec } = require('child_process');
        exec(`rm -f "${filePath}"`, (error, stdout, stderr) => {
            if (error) {
                console.error('Error deleting file:', error);
                res.status(500).json({ error: 'Failed to delete file: ' + error.message });
                return;
            }
            res.json({ success: true, message: 'File deleted' });
        });
    }
});

app.post('/api/upload', upload.single('file'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }
    broadcast({
        type: 'files',
        data: getLogFiles()
    });
    res.json({ 
        success: true, 
        message: 'File uploaded',
        filename: req.file.filename
    });
});

const clients = new Set();

wss.on('connection', (ws) => {
    clients.add(ws);
    console.log('Client connected');

    ws.send(JSON.stringify({
        type: 'files',
        data: getLogFiles()
    }));

    ws.on('close', () => {
        clients.delete(ws);
        console.log('Client disconnected');
    });
});

function broadcast(message) {
    const data = JSON.stringify(message);
    clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(data);
        }
    });
}

const watchedFiles = new Map();

if (fs.existsSync(LOG_DIR)) {
    const watcher = chokidar.watch(path.join(LOG_DIR, '*.log'), {
        persistent: true,
        ignoreInitial: true,
        usePolling: true,
        interval: 500,
        awaitWriteFinish: {
            stabilityThreshold: 300,
            pollInterval: 100
        }
    });

    watcher.on('add', (filePath) => {
        console.log('File added:', filePath);
        broadcast({
            type: 'files',
            data: getLogFiles()
        });
    });

    watcher.on('change', (filePath) => {
        const filename = path.basename(filePath);
        console.log('File changed:', filename);
        broadcast({
            type: 'files',
            data: getLogFiles()
        });
        broadcast({
            type: 'fileChanged',
            data: {
                filename,
                source: 'system'
            }
        });
    });

    watcher.on('unlink', (filePath) => {
        console.log('File removed:', filePath);
        broadcast({
            type: 'files',
            data: getLogFiles()
        });
        broadcast({
            type: 'fileRemoved',
            data: { filename: path.basename(filePath) }
        });
    });

    watcher.on('error', (error) => {
        console.error('Watcher error:', error);
    });

    watcher.on('ready', () => {
        console.log('Watcher ready');
    });

    console.log(`Watching for changes in: ${LOG_DIR}`);
} else {
    console.warn(`Log directory does not exist: ${LOG_DIR}`);
}

if (fs.existsSync(UPLOAD_DIR)) {
    const uploadWatcher = chokidar.watch(path.join(UPLOAD_DIR, '*.log'), {
        persistent: true,
        ignoreInitial: true,
        usePolling: true,
        interval: 500,
        awaitWriteFinish: {
            stabilityThreshold: 300,
            pollInterval: 100
        }
    });

    uploadWatcher.on('add', (filePath) => {
        console.log('File added:', filePath);
        broadcast({
            type: 'files',
            data: getLogFiles()
        });
    });

    uploadWatcher.on('change', (filePath) => {
        const filename = path.basename(filePath);
        const source = getUploadDirSourceForFilename(filename);
        console.log('File changed:', filename);
        broadcast({
            type: 'files',
            data: getLogFiles()
        });
        broadcast({
            type: 'fileChanged',
            data: {
                filename,
                source
            }
        });
    });

    uploadWatcher.on('unlink', (filePath) => {
        console.log('File removed:', filePath);
        broadcast({
            type: 'files',
            data: getLogFiles()
        });
        broadcast({
            type: 'fileRemoved',
            data: { filename: path.basename(filePath) }
        });
    });

    uploadWatcher.on('error', (error) => {
        console.error('Watcher error:', error);
    });

    uploadWatcher.on('ready', () => {
        console.log('Watcher ready');
    });

    console.log(`Watching for changes in: ${UPLOAD_DIR}`);
} else {
    console.warn(`Upload directory does not exist: ${UPLOAD_DIR}`);
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
