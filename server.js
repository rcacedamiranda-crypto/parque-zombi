#!/usr/bin/env node
/* Parque Zombi - servidor de salas (sin dependencias, solo Node.js 14+)
   - Sirve el juego en  http://TU-IP:8080  (abre esa direccion en el celular o la laptop)
   - Conecta a los jugadores por WebSocket (el APK y el navegador usan el mismo servidor)
   Uso:  node server.js        (puerto distinto:  PORT=9000 node server.js) */
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), os = require('os');
const PORT = +process.env.PORT || 8080;
const GAME = process.env.GAME_FILE || [path.join(__dirname, 'index.html'), path.join(__dirname, '..', 'index.html')].find(p => fs.existsSync(p)) || path.join(__dirname, 'index.html');
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_CLIENTS = 300, MAX_MSG = 32 * 1024, MAX_ROOM = 64, MSG_PER_SEC = 120;
const rooms = new Map();   // nombre -> Map(id -> {c, pres})
const clients = new Set();
let nextId = 1;

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    fs.readFile(GAME, (e, d) => {
      if (e) { res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' }); res.end('No encuentro index.html (debe estar junto a la carpeta "servidor").'); return; }
      res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' }); res.end(d);
    });
    return;
  }
  if (u === '/health') { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('ok'); return; }
  res.writeHead(404, { 'content-type': 'text/plain' }); res.end('No encontrado');
});

function frame(op, payload) {
  const len = payload.length; let h;
  if (len < 126) h = Buffer.from([0x80 | op, len]);
  else if (len < 65536) { h = Buffer.alloc(4); h[0] = 0x80 | op; h[1] = 126; h.writeUInt16BE(len, 2); }
  else { h = Buffer.alloc(10); h[0] = 0x80 | op; h[1] = 127; h.writeUInt32BE(0, 2); h.writeUInt32BE(len, 6); }
  return Buffer.concat([h, payload]);
}
function sendRaw(c, buf) {
  if (c.dead || !c.sock.writable) return;
  if (c.sock.writableLength > 2e6) { drop(c); return; }   // cliente lento: lo soltamos
  try { c.sock.write(buf); } catch (e) { drop(c); }
}
const sendText = (c, s) => sendRaw(c, frame(1, Buffer.from(s)));

server.on('upgrade', (req, sock) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket' || clients.size >= MAX_CLIENTS) { sock.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { id: 'p' + (nextId++), sock, rooms: new Set(), buf: Buffer.alloc(0), frag: null, dead: false, tick: 0, n: 0 };
  clients.add(c);
  sock.on('data', d => { c.buf = Buffer.concat([c.buf, d]); if (c.buf.length > MAX_MSG * 2 + 64) { drop(c); return; } parse(c); });
  sock.on('close', () => drop(c));
  sock.on('error', () => drop(c));
  sendText(c, JSON.stringify({ t: 'hi', id: c.id }));
  console.log('+ jugador ' + c.id + ' (' + clients.size + ' conectados)');
});

function parse(c) {
  for (;;) {
    const b = c.buf; if (b.length < 2 || c.dead) return;
    const fin = (b[0] & 0x80) !== 0, op = b[0] & 0x0f, masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; if (b.readUInt32BE(2)) { drop(c); return; } len = b.readUInt32BE(6); off = 10; }
    if (len > MAX_MSG || !masked) { drop(c); return; }
    if (b.length < off + 4 + len) return;
    const mask = b.subarray(off, off + 4), payload = Buffer.alloc(len);
    for (let i = 0; i < len; i++) payload[i] = b[off + 4 + i] ^ mask[i & 3];
    c.buf = b.subarray(off + 4 + len);
    if (op === 8) { try { c.sock.end(frame(8, Buffer.alloc(0))); } catch (e) {} drop(c); return; }
    if (op === 9) { sendRaw(c, frame(10, payload)); continue; }
    if (op === 10) continue;
    if (op === 1) c.frag = [payload]; else if (op === 0 && c.frag) c.frag.push(payload); else continue;
    if (fin) { const msg = Buffer.concat(c.frag).toString('utf8'); c.frag = null; handle(c, msg); }
  }
}

function broadcast(name, obj) {
  const r = rooms.get(name); if (!r) return;
  const buf = frame(1, Buffer.from(JSON.stringify(obj)));
  for (const m of r.values()) sendRaw(m.c, buf);
}
function leave(c, name) {
  const r = rooms.get(name); if (!r || !r.has(c.id)) return;
  r.delete(c.id); c.rooms.delete(name);
  if (!r.size) rooms.delete(name); else broadcast(name, { t: 'left', room: name, id: c.id });
}
function drop(c) {
  if (c.dead) return; c.dead = true;
  for (const n of [...c.rooms]) leave(c, n);
  clients.delete(c); try { c.sock.destroy(); } catch (e) {}
  console.log('- jugador ' + c.id + ' (' + clients.size + ' conectados)');
}
const BAD = new Set(['__proto__', 'constructor', 'prototype']);
function handle(c, msg) {
  const now = Date.now(); if (now - c.tick > 1000) { c.tick = now; c.n = 0; } if (++c.n > MSG_PER_SEC) return;
  let m; try { m = JSON.parse(msg); } catch (e) { return; }
  if (!m || typeof m.room !== 'string') return;
  const name = m.room.replace(/[^\w\-]/g, '').slice(0, 40); if (!name) return;
  if (m.t === 'join') {
    let r = rooms.get(name);
    if (!r) { r = new Map(); rooms.set(name, r); }
    if (!r.has(c.id)) {
      if (r.size >= (name === 'lobby' ? MAX_CLIENTS : MAX_ROOM) || c.rooms.size >= 4) return;
      r.set(c.id, { c, pres: Object.create(null) }); c.rooms.add(name);
    }
    const peers = {}; for (const [id, e] of r) peers[id] = e.pres;
    sendText(c, JSON.stringify({ t: 'snap', room: name, peers }));
  } else if (m.t === 'pres') {
    const r = rooms.get(name), me = r && r.get(c.id); if (!me) return;
    const p = m.patch; if (!p || typeof p !== 'object' || Array.isArray(p)) return;
    for (const k of Object.keys(p).slice(0, 40)) { if (BAD.has(k)) continue; if (p[k] === null) delete me.pres[k]; else me.pres[k] = p[k]; }
    if (JSON.stringify(me.pres).length > MAX_MSG) { me.pres = Object.create(null); return; }
    broadcast(name, { t: 'upd', room: name, id: c.id, pr: me.pres });
  } else if (m.t === 'leave') leave(c, name);
}

setInterval(() => { for (const c of clients) sendRaw(c, frame(9, Buffer.alloc(0))); }, 25000);   // mantiene viva la conexion

server.listen(PORT, '0.0.0.0', () => {
  console.log('\n=== Parque Zombi - servidor encendido ===');
  console.log('Puerto: ' + PORT);
  const ips = []; for (const l of Object.values(os.networkInterfaces())) for (const i of l || []) if (i.family === 'IPv4' && !i.internal) ips.push(i.address);
  console.log('\nEn ESTA laptop abre:   http://localhost:' + PORT);
  if (ips.length) {
    console.log('\nEn los celulares (mismo WiFi) abre el navegador en:');
    ips.forEach(ip => console.log('   http://' + ip + ':' + PORT));
    console.log('\nO en la app/APK, en "Servidor", escribe:');
    ips.forEach(ip => console.log('   ' + ip + ':' + PORT));
  } else console.log('\n(No encuentro tu IP de WiFi: conectate a una red WiFi.)');
  console.log('\nDeja esta ventana abierta mientras juegan. Para apagar: Ctrl+C\n');
});
server.on('error', e => { console.error(e.code === 'EADDRINUSE' ? 'El puerto ' + PORT + ' ya esta en uso. Prueba: PORT=9000 node server.js' : e); process.exit(1); });
