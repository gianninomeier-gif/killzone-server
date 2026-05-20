const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('KILLZONE Game Server running');
});

const wss = new WebSocket.Server({ server });

// rooms[code] = { host, players: {id: ws}, meta: {killGoal, maxP, status} }
const rooms = {};
// market: { [skinId]: { price, history, trend } }
let market = {};
// ws -> { id, roomCode }
const clients = new Map();

// ── Market Ticker ─────────────────────────────────────────────
const SKINS_META = [
  {id:'fire',basePrice:80},{id:'ice',basePrice:220},{id:'cherry',basePrice:210},
  {id:'toxic',basePrice:230},{id:'void',basePrice:400},{id:'gold',basePrice:440},
  {id:'cobalt',basePrice:390},{id:'magma',basePrice:420},{id:'galaxy',basePrice:690},
  {id:'storm',basePrice:750},{id:'blood_moon',basePrice:800},{id:'matrix',basePrice:720},
  {id:'rainbow',basePrice:1230},{id:'phantom',basePrice:980},{id:'dragon',basePrice:1100},
  {id:'cosmic',basePrice:1500}
];

function initMarket() {
  for (const sk of SKINS_META) {
    market[sk.id] = { price: sk.basePrice, history: [sk.basePrice], trend: 0 };
  }
}

function tickMarket() {
  const updates = {};
  for (const sk of SKINS_META) {
    const m = market[sk.id]; if (!m) continue;
    const vol = sk.basePrice > 900 ? 0.04 : sk.basePrice > 500 ? 0.035 : sk.basePrice > 300 ? 0.03 : 0.025;
    const drift = (Math.random() - 0.49) * vol;
    let p = Math.max(sk.basePrice * 0.2, Math.min(sk.basePrice * 6, m.price * (1 + drift)));
    p = Math.round(p);
    const history = [...m.history, p]; if (history.length > 40) history.shift();
    market[sk.id] = { price: p, history, trend: p > m.price ? 1 : p < m.price ? -1 : 0 };
    updates[sk.id] = market[sk.id];
  }
  broadcast(null, { type: 'MARKET_UPDATE', market: updates });
}

initMarket();
setInterval(tickMarket, 8000);

// ── Helpers ───────────────────────────────────────────────────
function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function broadcast(roomCode, obj, excludeId = null) {
  if (!roomCode) {
    // Broadcast to all
    for (const [ws] of clients) send(ws, obj);
    return;
  }
  const room = rooms[roomCode]; if (!room) return;
  for (const [pid, ws] of Object.entries(room.players)) {
    if (pid !== excludeId) send(ws, obj);
  }
}

function getRoom(code) { return rooms[code]; }

function removePlayerFromRoom(id, roomCode) {
  const room = rooms[roomCode]; if (!room) return;
  delete room.players[id];
  if (Object.keys(room.players).length === 0) {
    delete rooms[roomCode];
    return;
  }
  // If host left, assign new host
  if (room.host === id) {
    room.host = Object.keys(room.players)[0];
    broadcast(roomCode, { type: 'NEW_HOST', hostId: room.host });
  }
  broadcast(roomCode, { type: 'PLAYER_LEFT', id });
}

// ── Connection Handler ────────────────────────────────────────
wss.on('connection', (ws) => {
  clients.set(ws, { id: null, roomCode: null });

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    const client = clients.get(ws);

    switch (m.type) {

      case 'CREATE_ROOM': {
        const code = Math.random().toString(36).slice(2, 6).toUpperCase();
        rooms[code] = {
          host: m.id,
          players: { [m.id]: ws },
          meta: { killGoal: m.killGoal || 10, maxP: m.maxP || 4, status: 'lobby' },
          playerData: { [m.id]: m.playerData }
        };
        client.id = m.id;
        client.roomCode = code;
        send(ws, { type: 'ROOM_CREATED', roomCode: code, market });
        break;
      }

      case 'JOIN_ROOM': {
        const room = getRoom(m.roomCode);
        if (!room) { send(ws, { type: 'JOIN_DENY', reason: 'Raum nicht gefunden' }); return; }
        if (Object.keys(room.players).length >= room.meta.maxP) { send(ws, { type: 'JOIN_DENY', reason: 'Raum voll' }); return; }
        if (room.meta.status === 'running') { send(ws, { type: 'JOIN_DENY', reason: 'Spiel läuft' }); return; }
        room.players[m.id] = ws;
        room.playerData[m.id] = m.playerData;
        client.id = m.id;
        client.roomCode = m.roomCode;
        // Send full lobby state to joiner
        send(ws, { type: 'JOIN_OK', roomCode: m.roomCode, players: room.playerData, meta: room.meta, market });
        // Notify others
        broadcast(m.roomCode, { type: 'PLAYER_JOINED', id: m.id, playerData: m.playerData }, m.id);
        break;
      }

      case 'META_UPDATE': {
        const room = getRoom(client.roomCode); if (!room || room.host !== client.id) return;
        Object.assign(room.meta, m.meta);
        broadcast(client.roomCode, { type: 'META_UPDATE', meta: room.meta }, client.id);
        break;
      }

      case 'GAME_START': {
        const room = getRoom(client.roomCode); if (!room || room.host !== client.id) return;
        room.meta.status = 'running';
        broadcast(client.roomCode, { type: 'GAME_START', players: m.players, seed: m.seed, killGoal: m.killGoal });
        break;
      }

      case 'INPUT': {
        // Client → Host only
        const room = getRoom(client.roomCode); if (!room) return;
        const hostWs = room.players[room.host];
        send(hostWs, { ...m });
        break;
      }

      case 'STATE': {
        // Host → all clients (not host itself)
        broadcast(client.roomCode, m, client.id);
        break;
      }

      case 'GAME_OVER': {
        broadcast(client.roomCode, m);
        const room = getRoom(client.roomCode);
        if (room) room.meta.status = 'lobby';
        break;
      }

      case 'MARKET_BUY':
      case 'MARKET_SELL': {
        const sk = SKINS_META.find(s => s.id === m.skinId); if (!sk) return;
        const mk = market[m.skinId]; if (!mk) return;
        if (m.type === 'MARKET_BUY') {
          mk.price = Math.round(mk.price * 1.03);
        } else {
          mk.price = Math.round(mk.price * 0.97);
        }
        mk.history = [...mk.history, mk.price]; if (mk.history.length > 40) mk.history.shift();
        mk.trend = m.type === 'MARKET_BUY' ? 1 : -1;
        broadcast(null, { type: 'MARKET_UPDATE', market: { [m.skinId]: mk } });
        break;
      }

      case 'LEAVE': {
        if (client.roomCode) removePlayerFromRoom(client.id, client.roomCode);
        break;
      }
    }
  });

  ws.on('close', () => {
    const client = clients.get(ws);
    if (client && client.roomCode) removePlayerFromRoom(client.id, client.roomCode);
    clients.delete(ws);
  });
});

server.listen(PORT, () => console.log(`KILLZONE server on port ${PORT}`));
