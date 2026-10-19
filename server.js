const express = require('express');
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const { WebSocketServer } = require('ws');
const { Chess } = require('chess.js');
/* ---------- تخزين في MongoDB ---------- */
const { MongoClient } = require('mongodb');
let db = { users: {}, tokens: {} };
let col, timer = null;
const flush = () => col.replaceOne({ _id: 'main' }, { _id: 'main', ...db }, { upsert: true });
const save = () => {
  if (!col || timer) return;
  timer = setTimeout(async () => {
    timer = null;
    try { await flush(); } catch (e) { console.error('save failed', e.message); }
  }, 300);
};
async function initDb() {
  const client = new MongoClient(process.env.MONGODB_URI);
  await client.connect();
  col = client.db('chess').collection('state');
  const doc = await col.findOne({ _id: 'main' });
  if (doc) db = { users: doc.users || {}, tokens: doc.tokens || {} };
}
process.on('SIGTERM', async () => { try { if (col) await flush(); } catch {} process.exit(0); });


const hashPw = (pw, salt) => crypto.scryptSync(pw, salt, 32).toString('hex');
const findByName = (n) => Object.values(db.users).find(u => u.name.toLowerCase() === n.toLowerCase());
function newId() {
  let id;
  do { id = String(Math.floor(100000 + Math.random() * 900000)); } while (db.users[id]);
  return id;
}
function issueToken(id) {
  const t = crypto.randomBytes(24).toString('hex');
  db.tokens[t] = id; save(); return t;
}

/* ---------- HTTP: تسجيل ودخول ---------- */
const app = express();
app.use(express.json());
app.get('/', (req, res) => res.type('html').send(HTML));

app.post('/api/register', (req, res) => {
  const name = String(req.body.name || '').trim();
  const pw = String(req.body.password || '');
  if (name.length < 2 || name.length > 20) return res.status(400).json({ error: 'nameLen' });
  if (pw.length < 6) return res.status(400).json({ error: 'pwLen' });
  if (findByName(name)) return res.status(400).json({ error: 'nameTaken' });
  const id = newId(), salt = crypto.randomBytes(16).toString('hex');
  db.users[id] = { id, name, salt, hash: hashPw(pw, salt), friends: [], incoming: [] };
  res.json({ token: issueToken(id) });
});

app.post('/api/login', (req, res) => {
  const u = findByName(String(req.body.name || '').trim());
  const pw = String(req.body.password || '');
  const bad = () => res.status(400).json({ error: 'badLogin' });
  if (!u) return bad();
  const a = Buffer.from(hashPw(pw, u.salt)), b = Buffer.from(u.hash);
  if (!crypto.timingSafeEqual(a, b)) return bad();
  res.json({ token: issueToken(u.id) });
});

/* ---------- WebSocket ---------- */
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const online = {};     // userId -> ws
const games = {};      // gameId -> { chess, w, b, over }
const userGame = {};   // userId -> gameId
const pending = new Set(); // "from>to"

const send = (ws, msg) => ws && ws.readyState === 1 && ws.send(JSON.stringify(msg));
const toUser = (id, msg) => send(online[id], msg);
const info = (id, code, params = {}) => toUser(id, { type: 'info', code, params });

function pushFriends(id) {
  const u = db.users[id]; if (!u) return;
  toUser(id, {
    type: 'friends',
    friends: u.friends.map(f => ({ id: f, name: db.users[f].name, online: !!online[f], busy: !!userGame[f] })),
    incoming: u.incoming.map(f => ({ id: f, name: db.users[f].name })),
  });
}
function notifyFriends(id) {
  for (const f of db.users[id].friends) pushFriends(f);
}

/* ---------- الألعاب ---------- */
function sendState(gid) {
  const G = games[gid]; if (!G) return;
  const c = G.chess, legal = {};
  for (const m of c.moves({ verbose: true })) (legal[m.from] ||= []).push(m.to);
  const last = c.history({ verbose: true }).at(-1);
  let status = 'playing', winner = null;
  if (G.over) { status = 'resigned'; winner = G.over.winner; }
  else if (c.isCheckmate()) { status = 'checkmate'; winner = c.turn() === 'w' ? 'b' : 'w'; }
  else if (c.isDraw()) status = 'draw';
  for (const col of ['w', 'b']) {
    const opp = G[col === 'w' ? 'b' : 'w'];
    toUser(G[col], {
      type: 'state', fen: c.fen(), turn: c.turn(), status, winner, check: c.inCheck(),
      legal: status === 'playing' ? legal : {}, last: last ? [last.from, last.to] : null,
      opponentOnline: !!online[opp],
    });
  }
}
function announce(gid, only) {
  const G = games[gid];
  for (const col of ['w', 'b']) {
    if (only && G[col] !== only) continue;
    const opp = G[col === 'w' ? 'b' : 'w'];
    toUser(G[col], { type: 'start', color: col, opponent: { id: opp, name: db.users[opp].name } });
  }
  sendState(gid);
}
function startGame(a, b) {
  const [w, bl] = Math.random() < 0.5 ? [a, b] : [b, a];
  const gid = crypto.randomBytes(6).toString('hex');
  games[gid] = { chess: new Chess(), w, b: bl, over: null };
  userGame[w] = userGame[bl] = gid;
  announce(gid);
  for (const id of [a, b]) { pushFriends(id); notifyFriends(id); }
}
const isOver = (G) => !!G.over || G.chess.isGameOver();
function endGame(gid) {
  const G = games[gid]; if (!G) return;
  delete userGame[G.w]; delete userGame[G.b]; delete games[gid];
}

wss.on('connection', (ws, req) => {
  const token = new URL(req.url, 'http://x').searchParams.get('token');
  const id = db.tokens[token];
  if (!id || !db.users[id]) return ws.close(4001);
  if (online[id] && online[id] !== ws) online[id].close(4002);
  online[id] = ws; ws.uid = id;

  const me = db.users[id];
  send(ws, { type: 'me', id, name: me.name });
  pushFriends(id); notifyFriends(id);
  if (userGame[id]) announce(userGame[id], id);

  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const target = db.users[String(m.id || '')];
    const gid = userGame[id], G = games[gid];

    switch (m.type) {
      case 'addFriend': {
        if (!target || target.id === id) return info(id, 'noSuchId');
        if (me.friends.includes(target.id)) return info(id, 'alreadyFriend');
        if (me.incoming.includes(target.id)) { acceptFriend(id, target.id); return; }
        if (!target.incoming.includes(id)) target.incoming.push(id);
        save(); pushFriends(target.id);
        info(id, 'requestSent', { name: target.name });
        break;
      }
      case 'acceptFriend': if (me.incoming.includes(m.id)) acceptFriend(id, m.id); break;
      case 'declineFriend':
        me.incoming = me.incoming.filter(x => x !== m.id); save(); pushFriends(id); break;
      case 'removeFriend':
        if (!target) return;
        me.friends = me.friends.filter(x => x !== m.id);
        target.friends = target.friends.filter(x => x !== id);
        save(); pushFriends(id); pushFriends(target.id); break;

      case 'challenge':
        if (!target || !me.friends.includes(target.id)) return;
        if (gid) return info(id, 'youInGame');
        if (!online[target.id]) return info(id, 'friendOffline');
        if (userGame[target.id]) return info(id, 'friendInGame');
        pending.add(`${id}>${target.id}`);
        toUser(target.id, { type: 'challenge', from: { id, name: me.name } });
        info(id, 'challengeSent', { name: target.name });
        break;
      case 'acceptChallenge':
        if (!pending.delete(`${m.id}>${id}`)) return;
        if (!online[m.id] || userGame[m.id] || gid) return info(id, 'challengeGone');
        startGame(m.id, id); break;
      case 'declineChallenge':
        if (pending.delete(`${m.id}>${id}`)) info(m.id, 'challengeDeclined', { name: me.name }); break;

      case 'move': {
        if (!G || isOver(G) || G.chess.turn() !== (G.w === id ? 'w' : 'b')) return;
        try { G.chess.move({ from: m.from, to: m.to, promotion: 'q' }); } catch { return; }
        sendState(gid); break;
      }
      case 'resign':
        if (G && !isOver(G)) { G.over = { winner: G.w === id ? 'b' : 'w' }; sendState(gid); } break;
      case 'rematch':
        if (G && isOver(G)) {
          [G.w, G.b] = [G.b, G.w]; G.chess = new Chess(); G.over = null; announce(gid);
        } break;
      case 'leave':
        if (G) {
          const opp = G.w === id ? G.b : G.w;
          endGame(gid);
          toUser(opp, { type: 'ended' }); toUser(id, { type: 'ended' });
          for (const u of [id, opp]) { pushFriends(u); notifyFriends(u); }
        } break;

      case 'signal': // ترحيل إشارات WebRTC للصوت
        if (G) toUser(G.w === id ? G.b : G.w, { type: 'signal', data: m.data }); break;
    }
  });

  ws.on('close', () => {
    if (online[id] !== ws) return;
    delete online[id];
    const gid = userGame[id], G = games[gid];
    if (G) {
      const opp = G.w === id ? G.b : G.w;
      if (!online[opp]) endGame(gid); else sendState(gid);
    }
    notifyFriends(id);
  });
});

function acceptFriend(me, other) {
  const a = db.users[me], b = db.users[other];
  a.incoming = a.incoming.filter(x => x !== other);
  b.incoming = b.incoming.filter(x => x !== me);
  if (!a.friends.includes(other)) a.friends.push(other);
  if (!b.friends.includes(me)) b.friends.push(me);
  save(); pushFriends(me); pushFriends(other);
  info(other, 'friendAccepted', { name: a.name });
}

const PORT = process.env.PORT || 3000;
initDb()
  .then(() => server.listen(PORT, () => console.log('listening ' + PORT)))
  .catch(e => { console.error('DB connect failed', e); process.exit(1); });

/* ---------- الواجهة (الصفحة كاملة داخل هذا الملف) ---------- */
const HTML = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Chess</title>
<style>
  :root { --bg:#16202a; --panel:#1f2d3a; --text:#eef2f5; --muted:#8fa3b3; --light:#e8dcc0; --dark:#5b7f6b; --sel:#e0b84a; --dot:rgba(20,30,40,.35); --last:rgba(224,184,74,.45); --ok:#4cc38a; }
  * { box-sizing:border-box; }
  body { margin:0; padding-top:52px !important; background:var(--bg); color:var(--text); font-family:system-ui,"Segoe UI",Tahoma,sans-serif; min-height:100vh; display:flex; justify-content:center; padding:16px; }
  #lang { position:fixed; top:10px; inset-inline-end:10px; z-index:4; width:auto; background:var(--panel); color:var(--text); border:0; border-radius:8px; padding:8px 10px; font:inherit; }
  .screen { display:none; width:100%; max-width:520px; flex-direction:column; gap:12px; }
  .screen.on { display:flex; }
  h1, h2 { margin:0; } h2 { font-size:1.05rem; color:var(--muted); font-weight:600; }
  input, button { font:inherit; padding:12px 14px; border-radius:10px; border:0; }
  input { background:var(--panel); color:var(--text); width:100%; }
  button { background:var(--sel); color:#1a1a1a; font-weight:600; cursor:pointer; }
  button.ghost { background:var(--panel); color:var(--text); }
  button.small { padding:8px 12px; font-size:.9rem; }
  button:disabled { opacity:.4; cursor:default; }
  button:focus-visible, input:focus-visible { outline:3px solid #fff; outline-offset:2px; }
  .row { display:flex; gap:8px; align-items:center; }
  .row > input { flex:1; }
  .card { background:var(--panel); border-radius:12px; padding:12px 14px; display:flex; flex-direction:column; gap:10px; }
  .item { display:flex; align-items:center; gap:10px; }
  .item .name { flex:1; font-weight:600; }
  .dot { width:10px; height:10px; border-radius:50%; background:#55626d; flex:none; }
  .dot.on { background:var(--ok); } .dot.busy { background:var(--sel); }
  .muted { color:var(--muted); font-size:.9rem; }
  #authScreen { margin-top:6vh; text-align:center; }
  #authMsg, #toast { color:#ffb4a8; min-height:1.2em; }
  #myId { font-size:1.4rem; letter-spacing:.15em; direction:ltr; cursor:pointer; background:#0f1820; padding:6px 12px; border-radius:8px; }
  #banner { display:none; position:fixed; top:12px; left:50%; transform:translateX(-50%); width:calc(100% - 32px); max-width:480px; background:#2a3f52; border:2px solid var(--sel); border-radius:12px; padding:12px 14px; z-index:5; }
  #toast { position:fixed; bottom:16px; left:50%; transform:translateX(-50%); background:#0f1820; color:var(--text); padding:10px 16px; border-radius:10px; display:none; z-index:6; }
  #bar { display:flex; justify-content:space-between; align-items:center; background:var(--panel); padding:10px 14px; border-radius:10px; gap:8px; }
  #status { font-weight:600; }
  #board { direction:ltr; display:grid; grid-template-columns:repeat(8,1fr); width:100%; aspect-ratio:1; border-radius:6px; overflow:hidden; user-select:none; }
  .sq { position:relative; display:flex; align-items:center; justify-content:center; font-size:min(9vw,46px); line-height:1; cursor:pointer; }
  .sq.l { background:var(--light); } .sq.d { background:var(--dark); }
  .sq.last::before { content:""; position:absolute; inset:0; background:var(--last); }
  .sq.sel { box-shadow:inset 0 0 0 4px var(--sel); }
  .sq.chk { background:#c0453a !important; }
  .sq.hint::after { content:""; position:absolute; width:28%; height:28%; border-radius:50%; background:var(--dot); }
  .sq span { position:relative; z-index:1; }
  .w { color:#fff; text-shadow:0 0 2px #000,0 0 2px #000,0 0 1px #000; }
  .b { color:#111; text-shadow:0 0 1px rgba(255,255,255,.5); }
  #voiceState { font-size:.9rem; color:var(--muted); text-align:center; }
</style>
</head>
<body>
<select id="lang" aria-label="Language">
  <option value="ar">العربية</option>
  <option value="en">English</option>
  <option value="de">Deutsch</option>
</select>

<!-- تسجيل الدخول -->
<div id="authScreen" class="screen on">
  <h1>♞ <span data-i18n="title"></span></h1>
  <p class="muted" data-i18n="tagline"></p>
  <input id="name" data-ph="name" maxlength="20" autocomplete="username">
  <input id="pw" type="password" data-ph="pw" autocomplete="current-password">
  <div class="row">
    <button id="loginBtn" style="flex:1" data-i18n="login"></button>
    <button id="registerBtn" class="ghost" style="flex:1" data-i18n="register"></button>
  </div>
  <div id="authMsg"></div>
</div>

<!-- الصفحة الرئيسية -->
<div id="homeScreen" class="screen">
  <div class="card">
    <div class="row"><b id="myName" style="flex:1"></b><button id="logoutBtn" class="ghost small" data-i18n="logout"></button></div>
    <div class="row"><span class="muted" data-i18n="myIdLabel"></span><span id="myId"></span></div>
  </div>

  <div class="card">
    <h2 data-i18n="addFriend"></h2>
    <div class="row">
      <input id="friendId" inputmode="numeric" data-ph="friendIdPh" maxlength="6" style="direction:ltr;text-align:center">
      <button id="addBtn" data-i18n="add"></button>
    </div>
  </div>

  <div class="card" id="reqCard" style="display:none"><h2 data-i18n="requests"></h2><div id="reqList"></div></div>

  <div class="card">
    <h2 data-i18n="myFriends"></h2>
    <div id="friendList"></div>
    <div id="noFriends" class="muted" data-i18n="noFriends"></div>
  </div>
</div>

<!-- اللعبة -->
<div id="gameScreen" class="screen">
  <div id="bar"><span id="status">…</span><span id="colorLabel" class="muted"></span></div>
  <div id="board" role="grid" ></div>
  <div class="row">
    <button id="voiceBtn" style="flex:1"></button>
    <button id="resignBtn" class="ghost" data-i18n="resign"></button>
    <button id="rematchBtn" style="display:none" data-i18n="rematch"></button>
    <button id="leaveBtn" class="ghost" data-i18n="leave"></button>
  </div>
  <div id="voiceState"></div>
  <audio id="remoteAudio" autoplay playsinline></audio>
</div>

<div id="banner"></div>
<div id="toast"></div>

<script>
/* ================= اللغات / Languages / Sprachen ================= */
const T = {
  ar: {
    title:'شطرنج أونلاين', tagline:'أنشئ حساباً بسرعة، وبتاخد رقم ID خاص فيك تعطيه لأصدقائك',
    name:'الاسم', pw:'كلمة السر (6 أحرف على الأقل)', login:'دخول', register:'حساب جديد', logout:'خروج',
    myIdLabel:'رقمك (اضغط للنسخ):', idCopied:'تم نسخ الـ ID',
    addFriend:'إضافة صديق', friendIdPh:'أدخل ID صديقك', add:'إضافة', requests:'طلبات الصداقة', myFriends:'أصدقائي',
    noFriends:'لسا ما عندك أصدقاء. أضف أول صديق بالـ ID', accept:'قبول', decline:'رفض', challenge:'تحدّي',
    online:'متصل', offline:'غير متصل', inGame:'بلعبة', removeConfirm:'حذف {name} من الأصدقاء؟',
    challengeMsg:'{name} يتحداك للعب شطرنج!',
    voiceOn:'🎤 تشغيل الصوت', voiceOff:'🔇 إيقاف الصوت', voiceStopped:'الصوت متوقف',
    voiceWaiting:'بانتظار صديقك يشغّل الصوت…', voiceFriendWants:'صديقك يريد الصوت، اضغط "تشغيل الصوت"',
    voiceLive:'🔊 الصوت شغّال', voiceFail:'فشل الاتصال الصوتي (قد تحتاج خادم TURN)',
    micDenied:'ما قدرنا نوصل للمايك. اسمح بالصلاحية من المتصفح',
    resign:'استسلام', rematch:'إعادة', leave:'خروج', resignConfirm:'تستسلم؟', leaveConfirm:'الخروج بينهي اللعبة. متأكد؟',
    draw:'تعادل', won:'فزت! 🎉', lost:'خسرت', checkmate:'كش مات', oppResigned:'صديقك استسلم', youResigned:'استسلمت',
    oppOffline:'صديقك غير متصل…', yourTurn:'دورك', yourTurnCheck:'دورك (كش!)', turnOf:'دور {name}',
    youAre:'{name} · أنت {color}', white:'الأبيض', black:'الأسود',
    sessionEnded:'انتهت الجلسة', otherDevice:'تم فتح حسابك من مكان آخر', reconnecting:'انقطع الاتصال، جاري إعادة المحاولة…',
    nameLen:'الاسم من 2 إلى 20 حرف', pwLen:'كلمة السر 6 أحرف على الأقل', nameTaken:'هذا الاسم مستخدم، اختر اسماً آخر',
    badLogin:'الاسم أو كلمة السر غير صحيحة', noSuchId:'ما لقينا حساب بهذا الـ ID', alreadyFriend:'هو صديقك أصلاً',
    requestSent:'تم إرسال طلب الصداقة إلى {name}', youInGame:'أنت بلعبة حالياً', friendOffline:'صديقك غير متصل',
    friendInGame:'صديقك بلعبة حالياً', challengeSent:'تم إرسال التحدي إلى {name}', challengeGone:'التحدي لم يعد متاحاً',
    challengeDeclined:'{name} رفض التحدي', friendAccepted:'{name} قبل طلب الصداقة',
  },
  en: {
    title:'Online Chess', tagline:'Create an account in seconds. You get your own ID to share with friends.',
    name:'Name', pw:'Password (at least 6 characters)', login:'Log in', register:'Sign up', logout:'Log out',
    myIdLabel:'Your ID (tap to copy):', idCopied:'ID copied',
    addFriend:'Add a friend', friendIdPh:"Enter your friend's ID", add:'Add', requests:'Friend requests', myFriends:'My friends',
    noFriends:'No friends yet. Add your first friend by ID.', accept:'Accept', decline:'Decline', challenge:'Challenge',
    online:'Online', offline:'Offline', inGame:'In a game', removeConfirm:'Remove {name} from your friends?',
    challengeMsg:'{name} challenges you to a game of chess!',
    voiceOn:'🎤 Turn on voice', voiceOff:'🔇 Turn off voice', voiceStopped:'Voice is off',
    voiceWaiting:'Waiting for your friend to turn on voice…', voiceFriendWants:'Your friend wants voice. Tap "Turn on voice".',
    voiceLive:'🔊 Voice is live', voiceFail:'Voice connection failed (a TURN server may be needed)',
    micDenied:"Can't access the microphone. Allow it in your browser settings.",
    resign:'Resign', rematch:'Rematch', leave:'Leave', resignConfirm:'Resign this game?', leaveConfirm:'Leaving ends the game. Are you sure?',
    draw:'Draw', won:'You won! 🎉', lost:'You lost', checkmate:'Checkmate', oppResigned:'Your friend resigned', youResigned:'You resigned',
    oppOffline:'Your friend is offline…', yourTurn:'Your turn', yourTurnCheck:'Your turn (check!)', turnOf:"{name}'s turn",
    youAre:'{name} · You are {color}', white:'White', black:'Black',
    sessionEnded:'The session has ended', otherDevice:'Your account was opened elsewhere', reconnecting:'Connection lost, retrying…',
    nameLen:'Name must be 2 to 20 characters', pwLen:'Password must be at least 6 characters', nameTaken:'That name is taken, pick another',
    badLogin:'Wrong name or password', noSuchId:'No account with that ID', alreadyFriend:'Already your friend',
    requestSent:'Friend request sent to {name}', youInGame:"You're already in a game", friendOffline:'Your friend is offline',
    friendInGame:'Your friend is in a game', challengeSent:'Challenge sent to {name}', challengeGone:'That challenge is no longer available',
    challengeDeclined:'{name} declined the challenge', friendAccepted:'{name} accepted your friend request',
  },
  de: {
    title:'Online-Schach', tagline:'Erstelle in Sekunden ein Konto. Du bekommst eine eigene ID für deine Freunde.',
    name:'Name', pw:'Passwort (mindestens 6 Zeichen)', login:'Anmelden', register:'Registrieren', logout:'Abmelden',
    myIdLabel:'Deine ID (zum Kopieren tippen):', idCopied:'ID kopiert',
    addFriend:'Freund hinzufügen', friendIdPh:'ID deines Freundes eingeben', add:'Hinzufügen', requests:'Freundschaftsanfragen', myFriends:'Meine Freunde',
    noFriends:'Noch keine Freunde. Füge den ersten per ID hinzu.', accept:'Annehmen', decline:'Ablehnen', challenge:'Herausfordern',
    online:'Online', offline:'Offline', inGame:'Im Spiel', removeConfirm:'{name} aus deinen Freunden entfernen?',
    challengeMsg:'{name} fordert dich zu einer Partie Schach heraus!',
    voiceOn:'🎤 Sprache einschalten', voiceOff:'🔇 Sprache ausschalten', voiceStopped:'Sprache ist aus',
    voiceWaiting:'Warte, bis dein Freund die Sprache einschaltet…', voiceFriendWants:'Dein Freund möchte sprechen. Tippe auf „Sprache einschalten“.',
    voiceLive:'🔊 Sprache aktiv', voiceFail:'Sprachverbindung fehlgeschlagen (evtl. TURN-Server nötig)',
    micDenied:'Kein Zugriff auf das Mikrofon. Erlaube ihn in den Browser-Einstellungen.',
    resign:'Aufgeben', rematch:'Revanche', leave:'Verlassen', resignConfirm:'Partie aufgeben?', leaveConfirm:'Beim Verlassen endet die Partie. Sicher?',
    draw:'Remis', won:'Du hast gewonnen! 🎉', lost:'Du hast verloren', checkmate:'Schachmatt', oppResigned:'Dein Freund hat aufgegeben', youResigned:'Du hast aufgegeben',
    oppOffline:'Dein Freund ist offline…', yourTurn:'Du bist dran', yourTurnCheck:'Du bist dran (Schach!)', turnOf:'{name} ist dran',
    youAre:'{name} · Du spielst {color}', white:'Weiß', black:'Schwarz',
    sessionEnded:'Die Sitzung wurde beendet', otherDevice:'Dein Konto wurde woanders geöffnet', reconnecting:'Verbindung verloren, neuer Versuch…',
    nameLen:'Der Name muss 2 bis 20 Zeichen lang sein', pwLen:'Das Passwort braucht mindestens 6 Zeichen', nameTaken:'Dieser Name ist vergeben',
    badLogin:'Name oder Passwort falsch', noSuchId:'Kein Konto mit dieser ID', alreadyFriend:'Ihr seid bereits befreundet',
    requestSent:'Anfrage an {name} gesendet', youInGame:'Du bist bereits in einer Partie', friendOffline:'Dein Freund ist offline',
    friendInGame:'Dein Freund ist in einer Partie', challengeSent:'Herausforderung an {name} gesendet', challengeGone:'Die Herausforderung ist nicht mehr verfügbar',
    challengeDeclined:'{name} hat abgelehnt', friendAccepted:'{name} hat deine Anfrage angenommen',
  },
};
const detect = () => { const l = (navigator.language || 'en').slice(0, 2); return T[l] ? l : 'en'; };
let lang = localStorage.getItem('lang') || detect();
const t = (k, p = {}) => (T[lang][k] ?? T.en[k] ?? k).replace(/\\{(\\w+)\\}/g, (_, x) => p[x] ?? '');

/* ================= أدوات ================= */
const PIECES = { K:'♚', Q:'♛', R:'♜', B:'♝', N:'♞', P:'♟' };
const $ = (id) => document.getElementById(id);
const h = (tag, attrs = {}, ...kids) => {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) k.startsWith('on') ? e[k] = v : k === 'class' ? e.className = v : e.setAttribute(k, v);
  for (const kid of kids.flat()) e.append(kid);
  return e;
};
const show = (name) => document.querySelectorAll('.screen').forEach(s => s.classList.toggle('on', s.id === name + 'Screen'));
const send = (o) => ws && ws.readyState === 1 && ws.send(JSON.stringify(o));
let toastT;
function toast(key, p) { const el = $('toast'); el.textContent = t(key, p); el.style.display = 'block'; clearTimeout(toastT); toastT = setTimeout(() => el.style.display = 'none', 3500); }

let ws, me = null, myColor = 'w', opp = null, S = null, selected = null, inGame = false;
let lastFriends = null, bannerFrom = null, authErr = null, voiceKey = 'voiceStopped';

function applyLang() {
  document.documentElement.lang = lang;
  document.documentElement.dir = lang === 'ar' ? 'rtl' : 'ltr';
  document.title = t('title');
  $('lang').value = lang;
  document.querySelectorAll('[data-i18n]').forEach(el => el.textContent = t(el.dataset.i18n));
  document.querySelectorAll('[data-ph]').forEach(el => el.placeholder = t(el.dataset.ph));
  $('authMsg').textContent = authErr ? t(authErr) : '';
  $('voiceBtn').textContent = t(pc ? 'voiceOff' : 'voiceOn');
  $('voiceState').textContent = t(voiceKey);
  if (opp) $('colorLabel').textContent = t('youAre', { name: opp.name, color: t(myColor === 'w' ? 'white' : 'black') });
  if (lastFriends) renderFriends(lastFriends);
  if (S) render();
  if (bannerFrom && $('banner').style.display === 'block') showChallenge(bannerFrom);
}
$('lang').onchange = (e) => { lang = e.target.value; localStorage.setItem('lang', lang); applyLang(); };
const setVoice = (key) => { voiceKey = key; $('voiceState').textContent = t(key); };

/* ================= الحساب ================= */
async function auth(kind) {
  authErr = null; $('authMsg').textContent = '';
  const r = await fetch('/api/' + kind, { method:'POST', headers:{ 'Content-Type':'application/json' },
    body: JSON.stringify({ name:$('name').value, password:$('pw').value }) });
  const d = await r.json();
  if (!r.ok) { authErr = d.error; $('authMsg').textContent = t(d.error); return; }
  localStorage.setItem('token', d.token);
  connect();
}
$('loginBtn').onclick = () => auth('login');
$('registerBtn').onclick = () => auth('register');
$('logoutBtn').onclick = () => { localStorage.removeItem('token'); closeVoice(); ws && ws.close(1000); me = null; inGame = false; show('auth'); };

/* ================= الاتصال ================= */
function connect() {
  const token = localStorage.getItem('token'); if (!token) return show('auth');
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(\`\${proto}://\${location.host}/?token=\${token}\`);
  const mine = ws;
  ws.onmessage = (e) => onMsg(JSON.parse(e.data));
  ws.onclose = (e) => {
    if (e.code === 1000) return;
    if (e.code === 4001) { localStorage.removeItem('token'); show('auth'); return; }
    if (e.code === 4002) { toast('otherDevice'); show('auth'); return; }
    toast('reconnecting');
    setTimeout(() => { if (ws === mine && localStorage.getItem('token')) connect(); }, 2000);
  };
}

function onMsg(m) {
  switch (m.type) {
    case 'me': me = m; $('myName').textContent = m.name; $('myId').textContent = m.id; if (!inGame) show('home'); break;
    case 'friends': lastFriends = m; renderFriends(m); break;
    case 'info': toast(m.code, m.params); break;
    case 'challenge': showChallenge(m.from); break;
    case 'start':
      if (!opp || opp.id !== m.opponent.id) closeVoice();
      myColor = m.color; opp = m.opponent; inGame = true; selected = null; bannerFrom = null;
      $('colorLabel').textContent = t('youAre', { name: opp.name, color: t(myColor === 'w' ? 'white' : 'black') });
      $('banner').style.display = 'none';
      show('game'); break;
    case 'state': onState(m); break;
    case 'ended': inGame = false; opp = null; S = null; closeVoice(); show('home'); toast('sessionEnded'); if (lastFriends) renderFriends(lastFriends); break;
    case 'signal': onSignal(m.data); break;
  }
}

/* ================= الأصدقاء ================= */
$('myId').onclick = () => { navigator.clipboard && navigator.clipboard.writeText(me.id); toast('idCopied'); };
$('addBtn').onclick = () => { const id = $('friendId').value.trim(); if (id) { send({ type:'addFriend', id }); $('friendId').value = ''; } };

function renderFriends({ friends, incoming }) {
  $('reqCard').style.display = incoming.length ? 'flex' : 'none';
  $('reqList').replaceChildren(...incoming.map(r => h('div', { class:'item' },
    h('span', { class:'name' }, r.name),
    h('button', { class:'small', onclick:() => send({ type:'acceptFriend', id:r.id }) }, t('accept')),
    h('button', { class:'small ghost', onclick:() => send({ type:'declineFriend', id:r.id }) }, t('decline')))));
  $('noFriends').style.display = friends.length ? 'none' : 'block';
  $('friendList').replaceChildren(...friends.map(f => {
    const can = f.online && !f.busy && !inGame;
    return h('div', { class:'item' },
      h('span', { class:'dot ' + (f.busy ? 'busy' : f.online ? 'on' : '') }),
      h('span', { class:'name' }, f.name),
      h('span', { class:'muted' }, f.busy ? t('inGame') : f.online ? t('online') : t('offline')),
      h('button', { class:'small', ...(can ? {} : { disabled:'' }), onclick:() => send({ type:'challenge', id:f.id }) }, t('challenge')),
      h('button', { class:'small ghost', title:'✕', onclick:() => confirm(t('removeConfirm', { name:f.name })) && send({ type:'removeFriend', id:f.id }) }, '✕'));
  }));
}

function showChallenge(from) {
  bannerFrom = from;
  const b = $('banner');
  b.replaceChildren(
    h('div', { style:'margin-bottom:10px' }, t('challengeMsg', { name:from.name })),
    h('div', { class:'row' },
      h('button', { style:'flex:1', onclick:() => { send({ type:'acceptChallenge', id:from.id }); b.style.display = 'none'; } }, t('accept')),
      h('button', { class:'ghost', style:'flex:1', onclick:() => { send({ type:'declineChallenge', id:from.id }); b.style.display = 'none'; } }, t('decline'))));
  b.style.display = 'block';
}

/* ================= الرقعة ================= */
let prevOppOnline = null;
function onState(m) {
  S = m; selected = null; render();
  if (prevOppOnline === false && m.opponentOnline && voiceWanted) { closeVoice(); startVoice(); }
  if (prevOppOnline !== false && !m.opponentOnline) { const w = voiceWanted; closeVoice(); voiceWanted = w; }
  prevOppOnline = m.opponentOnline;
  maybeStartVoiceOffer();
}
function statusText() {
  if (S.status === 'draw') return t('draw');
  if (S.status !== 'playing') {
    const won = S.winner === myColor;
    let r = t(won ? 'won' : 'lost');
    if (S.status === 'checkmate') r = t('checkmate') + ' · ' + r;
    if (S.status === 'resigned') r = t(won ? 'oppResigned' : 'youResigned') + ' · ' + r;
    return r;
  }
  if (!S.opponentOnline) return t('oppOffline');
  if (S.turn === myColor) return t(S.check ? 'yourTurnCheck' : 'yourTurn');
  return t('turnOf', { name: opp ? opp.name : '' });
}
function render() {
  const grid = S.fen.split(' ')[0].split('/').map(r => {
    const row = [];
    for (const ch of r) /\\d/.test(ch) ? row.push(...Array(+ch).fill(null)) : row.push(ch);
    return row;
  });
  const files = 'abcdefgh', board = $('board'); board.innerHTML = '';
  const order = [];
  for (let r = 0; r < 8; r++) for (let f = 0; f < 8; f++) order.push([r, f]);
  if (myColor === 'b') order.reverse();
  for (const [r, f] of order) {
    const sq = files[f] + (8 - r), p = grid[r][f];
    const el = h('div', { class:'sq ' + ((r + f) % 2 ? 'd' : 'l'), 'data-sq':sq, onclick:() => onSquare(sq) });
    if (S.last && S.last.includes(sq)) el.classList.add('last');
    if (p) {
      const white = p === p.toUpperCase();
      el.append(h('span', { class:white ? 'w' : 'b' }, PIECES[p.toUpperCase()]));
      if (p.toUpperCase() === 'K' && S.check && white === (S.turn === 'w')) el.classList.add('chk');
    }
    board.append(el);
  }
  const over = S.status !== 'playing';
  $('status').textContent = statusText();
  $('resignBtn').style.display = over ? 'none' : '';
  $('rematchBtn').style.display = over ? '' : 'none';
}
function onSquare(sq) {
  if (!S || S.status !== 'playing' || S.turn !== myColor) return;
  if (selected && (S.legal[selected] || []).includes(sq)) { send({ type:'move', from:selected, to:sq }); selected = null; return; }
  selected = S.legal[sq] ? sq : null;
  document.querySelectorAll('.sq').forEach(el => el.classList.remove('sel', 'hint'));
  if (selected) {
    document.querySelector(\`[data-sq="\${sq}"]\`).classList.add('sel');
    S.legal[sq].forEach(x => document.querySelector(\`[data-sq="\${x}"]\`).classList.add('hint'));
  }
}
$('resignBtn').onclick = () => confirm(t('resignConfirm')) && send({ type:'resign' });
$('rematchBtn').onclick = () => send({ type:'rematch' });
$('leaveBtn').onclick = () => (!S || S.status !== 'playing' || confirm(t('leaveConfirm'))) && send({ type:'leave' });

/* ================= الصوت (WebRTC) ================= */
const RTC_CONFIG = { iceServers:[{ urls:'stun:stun.l.google.com:19302' }
  // For difficult networks add a TURN server:
  // , { urls:'turn:YOUR_HOST:3478', username:'user', credential:'pass' }
] };
let pc = null, localStream = null, pendingOffer = null, iceQueue = [], offered = false, voiceWanted = false;
const sig = (data) => send({ type:'signal', data });

async function startVoice() {
  if (pc) return;
  voiceWanted = true;
  try { localStream = await navigator.mediaDevices.getUserMedia({ audio:{ echoCancellation:true, noiseSuppression:true } }); }
  catch { voiceWanted = false; setVoice('micDenied'); return; }
  pc = new RTCPeerConnection(RTC_CONFIG);
  localStream.getTracks().forEach(tr => pc.addTrack(tr, localStream));
  pc.ontrack = (e) => { $('remoteAudio').srcObject = e.streams[0]; };
  pc.onicecandidate = (e) => e.candidate && sig({ candidate:e.candidate });
  pc.onconnectionstatechange = () => {
    if (!pc) return;
    if (pc.connectionState === 'connected') setVoice('voiceLive');
    if (pc.connectionState === 'failed') setVoice('voiceFail');
  };
  $('voiceBtn').textContent = t('voiceOff');
  setVoice('voiceWaiting');
  if (pendingOffer) { const o = pendingOffer; pendingOffer = null; await answer(o); } else maybeStartVoiceOffer();
}
async function maybeStartVoiceOffer() {
  if (!pc || offered || myColor !== 'w' || !S || !S.opponentOnline) return;
  offered = true;
  await pc.setLocalDescription(await pc.createOffer());
  sig({ sdp:pc.localDescription });
}
async function flushIce() { for (const c of iceQueue) await pc.addIceCandidate(c); iceQueue = []; }
async function answer(desc) {
  await pc.setRemoteDescription(desc); await flushIce();
  await pc.setLocalDescription(await pc.createAnswer());
  sig({ sdp:pc.localDescription });
}
async function onSignal(d) {
  try {
    if (d.sdp && d.sdp.type === 'offer') {
      if (!pc) { pendingOffer = d.sdp; setVoice('voiceFriendWants'); }
      else await answer(d.sdp);
    } else if (d.sdp && d.sdp.type === 'answer' && pc) { await pc.setRemoteDescription(d.sdp); await flushIce(); }
    else if (d.candidate) { pc && pc.remoteDescription ? await pc.addIceCandidate(d.candidate) : iceQueue.push(d.candidate); }
  } catch (err) { console.error(err); }
}
function closeVoice() {
  if (pc) pc.close(); pc = null; offered = false; pendingOffer = null; iceQueue = []; voiceWanted = false;
  if (localStream) localStream.getTracks().forEach(tr => tr.stop()); localStream = null;
  $('remoteAudio').srcObject = null; $('voiceBtn').textContent = t('voiceOn'); setVoice('voiceStopped');
}
$('voiceBtn').onclick = () => pc ? closeVoice() : startVoice();

/* ================= بداية ================= */
applyLang();
connect();
</script>
</body>
</html>
`;
