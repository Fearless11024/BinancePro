const express = require('express');
const { DatabaseSync } = require('node:sqlite');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const SECRET = process.env.JWT_SECRET || 'dev-secret-change-me';
const API = 'https://data-api.binance.vision/api/v3';
const COINS = ['BTC','ETH','BNB','SOL','XRP','DOGE','ADA','TRX','AVAX','LINK','DOT','LTC'];
const FEE = 0.001;

const db = new DatabaseSync('coindemo.db');
db.exec(`
CREATE TABLE IF NOT EXISTS users(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  hash TEXT NOT NULL,
  created TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS balances(
  user_id INTEGER NOT NULL,
  coin TEXT NOT NULL,
  amount REAL NOT NULL DEFAULT 0,
  PRIMARY KEY(user_id, coin)
);
CREATE TABLE IF NOT EXISTS trades(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  side TEXT NOT NULL,
  coin TEXT NOT NULL,
  qty REAL NOT NULL,
  price REAL NOT NULL,
  fee REAL NOT NULL,
  time TEXT DEFAULT CURRENT_TIMESTAMP
);
`);

const app = express();
app.use(express.json());
app.use(express.static('public'));

/* ---------- prices (cached 3 seconds) ---------- */
let cache = { t: 0, data: {} };
async function getPrices() {
  if (Date.now() - cache.t < 3000) return cache.data;
  const symbols = encodeURIComponent(JSON.stringify(COINS.map(c => c + 'USDT')));
  const r = await fetch(`${API}/ticker/price?symbols=${symbols}`);
  const arr = await r.json();
  const data = {};
  arr.forEach(x => data[x.symbol.replace('USDT', '')] = parseFloat(x.price));
  cache = { t: Date.now(), data };
  return data;
}

/* ---------- auth ---------- */
function auth(req, res, next) {
  const h = req.headers.authorization || '';
  try {
    req.user = jwt.verify(h.replace('Bearer ', ''), SECRET);
    next();
  } catch (e) {
    res.status(401).json({ error: 'Please log in again.' });
  }
}

app.post('/api/register', (req, res) => {
  const email = String(req.body.email || '').trim().toLowerCase();
  const password = String(req.body.password || '');
  if (!/^\S+@\S+\.\S+$/.test(email)) return res.status(400).json({ error: 'Enter a valid email.' });
  if (password.length < 8) return res.status(400).json({ error: 'Password must be at least 8 characters.' });
  try {
    const hash = bcrypt.hashSync(password, 10);
    const result = db.prepare('INSERT INTO users(email, hash) VALUES(?, ?)').run(email, hash);
    const id = Number(result.lastInsertRowid);
    db.prepare('INSERT INTO balances(user_id, coin, amount) VALUES(?, ?, ?)').run(id, 'USDT', 10000);
    res.json({ token: jwt.sign({ id, email }, SECRET, { expiresIn: '7d' }) });
  } catch (e) {
    res.status(400).json({ error: 'That email is already registered.' });
  }
});

app.post('/api/login', (req, res) => {
  const email = String(req.body.email || '').trim().toLowerCase();
  const u = db.prepare('SELECT * FROM users WHERE email = ?').get(email);
  if (!u || !bcrypt.compareSync(String(req.body.password || ''), u.hash))
    return res.status(401).json({ error: 'Wrong email or password.' });
  res.json({ token: jwt.sign({ id: Number(u.id), email }, SECRET, { expiresIn: '7d' }) });
});

/* ---------- market + wallet ---------- */
app.get('/api/prices', async (req, res) => {
  try { res.json(await getPrices()); }
  catch (e) { res.status(503).json({ error: 'Price feed unavailable.' }); }
});

app.get('/api/wallet', auth, (req, res) => {
  const rows = db.prepare('SELECT coin, amount FROM balances WHERE user_id = ? AND amount > 0').all(req.user.id);
  res.json(rows.map(r => ({ coin: r.coin, amount: r.amount })));
});

app.get('/api/history', auth, (req, res) => {
  const rows = db.prepare('SELECT * FROM trades WHERE user_id = ? ORDER BY id DESC LIMIT 50').all(req.user.id);
  res.json(rows.map(r => ({ ...r })));
});

/* ---------- spot trade ---------- */
app.post('/api/trade', auth, async (req, res) => {
  const { side, coin } = req.body;
  const qty = Number(req.body.qty);
  if (!['buy', 'sell'].includes(side) || !COINS.includes(coin) || !Number.isFinite(qty) || qty <= 0)
    return res.status(400).json({ error: 'Invalid order.' });

  let price;
  try { price = (await getPrices())[coin]; } catch (e) {}
  if (!price) return res.status(503).json({ error: 'Price unavailable, try again.' });

  const uid = req.user.id;
  const cost = qty * price, fee = cost * FEE;
  const getBal = c => {
    const row = db.prepare('SELECT amount FROM balances WHERE user_id=? AND coin=?').get(uid, c);
    return row ? row.amount : 0;
  };
  const setBal = (c, a) => db.prepare(
    'INSERT INTO balances(user_id, coin, amount) VALUES(?,?,?) ON CONFLICT(user_id, coin) DO UPDATE SET amount=excluded.amount'
  ).run(uid, c, a);

  try {
    db.exec('BEGIN');
    try {
      if (side === 'buy') {
        const usdt = getBal('USDT');
        if (usdt < cost + fee) throw new Error('Not enough USDT.');
        setBal('USDT', usdt - cost - fee);
        setBal(coin, getBal(coin) + qty);
      } else {
        const have = getBal(coin);
        if (have < qty) throw new Error('Not enough ' + coin + '.');
        setBal(coin, have - qty);
        setBal('USDT', getBal('USDT') + cost - fee);
      }
      db.prepare('INSERT INTO trades(user_id, side, coin, qty, price, fee) VALUES(?,?,?,?,?,?)')
        .run(uid, side, coin, qty, price, fee);
      db.exec('COMMIT');
    } catch (e) {
      db.exec('ROLLBACK');
      throw e;
    }
    res.json({ ok: true, price, cost, fee });
  } catch (e) {
    res.status(400).json({ error: e.message });
  }
});

app.listen(3000, () => console.log('CoinDemo running at http://localhost:3000'));