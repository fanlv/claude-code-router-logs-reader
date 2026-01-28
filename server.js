const express = require('express');
const http = require('http');
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

if (!fs.existsSync(UPLOAD_DIR)) {
    fs.mkdirSync(UPLOAD_DIR, { recursive: true });
}

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
app.use(express.json());

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
                        source: 'upload'
                    };
                });
            files.push(...uploadFiles);
        }
    } catch (error) {
        console.error('Error reading upload directory:', error);
    }
    
    return files.sort((a, b) => new Date(b.mtime) - new Date(a.mtime));
}

function getFileContent(filename, source) {
    try {
        let filePath;
        if (source === 'upload') {
            filePath = path.join(UPLOAD_DIR, filename);
        } else {
            filePath = path.join(LOG_DIR, filename);
        }
        if (!fs.existsSync(filePath)) {
            return null;
        }
        return fs.readFileSync(filePath, 'utf-8');
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
    const content = getFileContent(filename, source);
    if (content === null) {
        return res.status(404).json({ error: 'File not found' });
    }
    res.json({ filename, content, source });
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
    
    const content = getFileContent(filename, source);
    if (content === null) {
        return res.status(404).json({ error: 'File not found' });
    }
    
    const lines = content.split('\n').filter(line => line.trim());
    if (lineNum > lines.length) {
        return res.status(404).json({ error: 'Line not found' });
    }
    
    const line = lines[lineNum - 1];
    try {
        const json = JSON.parse(line);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.send(JSON.stringify(json, null, 2));
    } catch (e) {
        res.setHeader('Content-Type', 'text/plain; charset=utf-8');
        res.send(line);
    }
});

app.delete('/api/files/:filename', (req, res) => {
    const filename = req.params.filename;
    const source = req.query.source || 'system';
    if (!filename.endsWith('.log')) {
        return res.status(400).json({ error: 'Invalid file type' });
    }
    
    let filePath;
    if (source === 'upload') {
        filePath = path.join(UPLOAD_DIR, filename);
    } else {
        filePath = path.join(LOG_DIR, filename);
    }
    
    if (!fs.existsSync(filePath)) {
        return res.status(404).json({ error: 'File not found' });
    }
    
    if (source === 'upload') {
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
                content: getFileContent(filename, 'system'),
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});
