import { Redis } from '@upstash/redis';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';

const SECRET = process.env.JWT_SECRET || null;
const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const redis = REDIS_URL && REDIS_TOKEN ? new Redis({ url: REDIS_URL, token: REDIS_TOKEN }) : null;
const BINANCE = 'https://data-api.binance.vision/api/v3';
const GECKO = 'https://api.coingecko.com/api/v3';
const COINS = ['BTC','ETH','BNB','SOL','XRP','DOGE','ADA','TRX','AVAX','LINK','DOT','LTC','BCH','ATOM','UNI','NEAR','APT','ARB','OP','FIL','ETC','XLM','HBAR','ICP','AAVE','INJ','SUI','TON','SHIB','PEPE','FET','RUNE','MKR','ALGO','VET','SAND','MANA','AXS','GRT','CRV'];
const IDS = {
  BTC:'bitcoin',ETH:'ethereum',BNB:'binancecoin',SOL:'solana',XRP:'ripple',DOGE:'dogecoin',ADA:'cardano',TRX:'tron',
  AVAX:'avalanche-2',LINK:'chainlink',DOT:'polkadot',LTC:'litecoin',BCH:'bitcoin-cash',ATOM:'cosmos',UNI:'uniswap',
  NEAR:'near',APT:'aptos',ARB:'arbitrum',OP:'optimism',FIL:'filecoin',ETC:'ethereum-classic',XLM:'stellar',
  HBAR:'hedera-hashgraph',ICP:'internet-computer',AAVE:'aave',INJ:'injective-protocol',SUI:'sui',TON:'the-open-network',
  SHIB:'shiba-inu',PEPE:'pepe',FET:'fetch-ai',RUNE:'thorchain',MKR:'maker',ALGO:'algorand',VET:'vechain',
  SAND:'the-sandbox',MANA:'decentraland',AXS:'axie-infinity',GRT:'the-graph',CRV:'curve-dao-token'
};
const INTERVALS = ['1m','15m','1h','4h','1d'];
const APY = { USDT: 5, BTC: 1.5, ETH: 2.5, BNB: 3, SOL: 4, XRP: 2 };
const LEVS = [2, 5, 10, 20, 50];
const FEE = 0.001, START = 10000;
const YEAR = 365 * 24 * 3600 * 1000;
const SNAP_MS = 5 * 60 * 1000;
const LOCK_MS = 10 * 60 * 1000, MAX_FAILS = 5;

const reply = (data, status = 200, extra = {}) =>
  new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json', ...extra } });
const cached = (data, secs) => reply(data, 200, {
  'Cache-Control': `public, s-maxage=${secs}, stale-while-revalidate=60`
});
const keyOf = email => 'u_' + Buffer.from(email).toString('hex');
const now = () => new Date().toISOString().replace('T', ' ').slice(0, 19);
const uid = () => Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

async function getJSON(url, ms = 5000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, { signal: ctrl.signal, headers: { Accept: 'application/json' } });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return await r.json();
  } finally { clearTimeout(t); }
}

// Start the main source. If it is slow or fails, start the backup. First success wins.
function race(primary, backup, delay = 1500) {
  return new Promise((resolve, reject) => {
    let fails = 0, done = false, started = false;
    const ok = v => { if (!done) { done = true; resolve(v); } };
    const bad = e => { if (++fails === 2 && !done) reject(e); };
    const startBackup = () => { if (!started) { started = true; backup().then(ok, bad); } };
    const timer = setTimeout(startBackup, delay);
    primary().then(ok, e => { clearTimeout(timer); bad(e); startBackup(); });
  });
}

/* ---------- market data ---------- */
async function fromBinance() {
  const symbols = encodeURIComponent(JSON.stringify(COINS.map(c => c + 'USDT')));
  let arr;
  try { arr = await getJSON(`${BINANCE}/ticker/24hr?symbols=${symbols}`, 4000); }
  catch (e) {
    if (!String(e.message).includes('HTTP 400')) throw e;
    const all = await Promise.all(COINS.map(c => getJSON(`${BINANCE}/ticker/24hr?symbol=${c}USDT`, 4000).catch(() => null)));
    arr = all.filter(Boolean);
  }
  if (!arr.length) throw new Error('empty');
  return arr.map(t => ({
    symbol: t.symbol, lastPrice: +t.lastPrice, priceChangePercent: +t.priceChangePercent,
    highPrice: +t.highPrice, lowPrice: +t.lowPrice, quoteVolume: +t.quoteVolume
  }));
}

async function fromGecko() {
  const ids = COINS.map(c => IDS[c]).join(',');
  const arr = await getJSON(`${GECKO}/coins/markets?vs_currency=usd&ids=${ids}&per_page=100&price_change_percentage=24h`, 8000);
  const byId = {};
  Object.entries(IDS).forEach(([c, id]) => (byId[id] = c));
  const out = arr.filter(x => byId[x.id] && x.current_price).map(x => ({
    symbol: byId[x.id] + 'USDT', lastPrice: x.current_price,
    priceChangePercent: x.price_change_percentage_24h || 0,
    highPrice: x.high_24h || x.current_price, lowPrice: x.low_24h || x.current_price,
    quoteVolume: x.total_volume || 0
  }));
  if (!out.length) throw new Error('empty');
  return out;
}

let mcache = { t: 0, data: null };
async function getMarket() {
  if (mcache.data && Date.now() - mcache.t < 8000) return mcache.data;
  try {
    const data = await race(
      () => fromBinance().then(list => ({ source: 'binance', list })),
      () => fromGecko().then(list => ({ source: 'coingecko', list }))
    );
    mcache = { t: Date.now(), data };
  } catch (e) { if (!mcache.data) throw e; }
  return mcache.data;
}

async function getPrices() {
  const m = await getMarket();
  const p = {};
  m.list.forEach(t => (p[t.symbol.replace('USDT', '')] = t.lastPrice));
  return p;
}
const safePrices = async () => { try { return await getPrices(); } catch (e) { return {}; } };

const kcache = {};
async function getKlines(coin, ivl) {
  const key = coin + ivl;
  const c = kcache[key];
  if (c && Date.now() - c.t < 20000) return c.data;
  const viaBinance = async () =>
    (await getJSON(`${BINANCE}/klines?symbol=${coin}USDT&interval=${ivl}&limit=90`, 4000)).map(a => [+a[1], +a[2], +a[3], +a[4]]);
  const viaGecko = async () => {
    const days = { '1m': 1, '15m': 1, '1h': 7, '4h': 14, '1d': 30 }[ivl] || 1;
    const raw = await getJSON(`${GECKO}/coins/${IDS[coin]}/ohlc?vs_currency=usd&days=${days}`, 8000);
    return raw.slice(-90).map(a => [a[1], a[2], a[3], a[4]]);
  };
  const data = await race(viaBinance, viaGecko);
  kcache[key] = { t: Date.now(), data };
  return data;
}

async function getBook(coin) {
  const [b, t] = await Promise.all([
    getJSON(`${BINANCE}/depth?symbol=${coin}USDT&limit=8`, 4000),
    getJSON(`${BINANCE}/trades?symbol=${coin}USDT&limit=12`, 4000)
  ]);
  return { asks: b.asks, bids: b.bids, trades: t.map(x => ({ price: x.price, qty: x.qty, sell: x.isBuyerMaker })) };
}

/* ---------- account maths ---------- */
const bal = (u, c) => u.bal[c] || 0;
const accrued = e => e.amt * (e.apy / 100) * (Date.now() - e.t) / YEAR;
const pnlOf = (p, price) => (p.side === 'long' ? price - p.entry : p.entry - price) * p.size;
const log = (u, e) => { u.trades.unshift({ time: now(), ...e }); u.trades = u.trades.slice(0, 300); };

// Total account value in USDT (returns NaN if a needed price is missing)
function equity(u, prices) {
  const val = (c, a) => {
    if (!a) return 0;
    if (c === 'USDT') return a;
    if (!prices[c]) throw new Error('no price');
    return a * prices[c];
  };
  try {
    let t = 0;
    Object.entries(u.bal).forEach(([c, a]) => (t += val(c, a)));
    u.earn.forEach(e => (t += val(e.coin, e.amt + accrued(e))));
    u.orders.forEach(o => (t += o.side === 'buy' ? o.reserved : val(o.coin, o.reserved)));
    u.positions.forEach(p => {
      if (!prices[p.coin]) throw new Error('no price');
      t += p.margin + pnlOf(p, prices[p.coin]);
    });
    return t;
  } catch (e) { return NaN; }
}

// Fills limit orders, liquidates futures, triggers alerts. Runs whenever the user's data is loaded.
function settle(u, prices) {
  const events = [];
  for (const o of u.orders.slice()) {
    const p = prices[o.coin];
    if (!p || !(o.side === 'buy' ? p <= o.limit : p >= o.limit)) continue;
    const price = o.side === 'buy' ? Math.min(o.limit, p) : Math.max(o.limit, p);
    const cost = o.qty * price, fee = cost * FEE;
    if (o.side === 'buy') {
      u.bal.USDT = bal(u, 'USDT') + o.reserved - cost - fee;
      u.bal[o.coin] = bal(u, o.coin) + o.qty;
    } else {
      u.bal.USDT = bal(u, 'USDT') + cost - fee;
    }
    u.orders = u.orders.filter(x => x.id !== o.id);
    log(u, { type: 'limit', side: o.side, coin: o.coin, qty: o.qty, price, fee });
    events.push({ kind: 'order', side: o.side, coin: o.coin, qty: o.qty, price });
  }
  for (const pos of u.positions.slice()) {
    const p = prices[pos.coin];
    if (!p || pnlOf(pos, p) > -pos.margin * 0.9) continue;
    u.positions = u.positions.filter(x => x.id !== pos.id);
    log(u, { type: 'futures', side: 'liquidated', coin: pos.coin, qty: pos.size, price: p, fee: 0,
      note: `${pos.side} ${pos.lev}x, lost ${pos.margin.toFixed(2)} USDT` });
    events.push({ kind: 'liq', coin: pos.coin, side: pos.side, price: p });
  }
  for (const a of u.alerts) {
    const p = prices[a.coin];
    if (a.done || !p || !(a.dir === 'above' ? p >= a.price : p <= a.price)) continue;
    a.done = true; a.hit = { t: now(), price: p };
    events.push({ kind: 'alert', coin: a.coin, dir: a.dir, target: a.price, price: p });
  }
  return events;
}

async function handle(req) {
  if (!SECRET) return reply({ error: 'Server not configured: set JWT_SECRET.' }, 500);
  if (!redis) return reply({ error: 'Server not configured: set the Upstash Redis variables.' }, 500);

  const url = new URL(req.url);
  const route = url.pathname.replace(/^\/api\/?/, '');
  const post = req.method === 'POST';
  const body = post ? await req.json().catch(() => ({})) : {};
  // Small helpers that keep accounts, lockouts and the leaderboard in Upstash Redis
  const users = {
    get: k => redis.get('u:' + k),
    setJSON: (k, v) => redis.set('u:' + k, v),
    delete: k => redis.del('u:' + k)
  };
  const limits = {
    get: k => redis.get('l:' + k),
    setJSON: (k, v) => redis.set('l:' + k, v, { ex: LOCK_MS / 1000 }),
    delete: k => redis.del('l:' + k)
  };
  const board = {
    setJSON: (k, v) => redis.hset('board', { [k]: v }),
    delete: k => redis.hdel('board', k),
    all: async () => Object.values((await redis.hgetall('board')) || {})
  };

  const lockKey = e => 'l_' + keyOf(e);
  const isLocked = async e => {
    const r = await limits.get(lockKey(e), { type: 'json' });
    return !!r && r.count >= MAX_FAILS && Date.now() - r.first < LOCK_MS;
  };
  const addFail = async e => {
    const r = await limits.get(lockKey(e), { type: 'json' });
    const fresh = !r || Date.now() - r.first > LOCK_MS;
    await limits.setJSON(lockKey(e), { count: fresh ? 1 : r.count + 1, first: fresh ? Date.now() : r.first });
  };

  /* ---------- public routes ---------- */
  if (route === 'market') {
    try { return cached(await getMarket(), 8); }
    catch (e) { return reply({ error: 'Price feed unavailable.' }, 503); }
  }
  if (route === 'klines') {
    const coin = url.searchParams.get('coin'), ivl = url.searchParams.get('ivl');
    if (!COINS.includes(coin) || !INTERVALS.includes(ivl)) return reply({ error: 'Bad request.' }, 400);
    try { return cached(await getKlines(coin, ivl), 20); }
    catch (e) { return reply({ error: 'Chart unavailable.' }, 503); }
  }
  if (route === 'book') {
    const coin = url.searchParams.get('coin');
    if (!COINS.includes(coin)) return reply({ error: 'Bad request.' }, 400);
    try { return cached(await getBook(coin), 3); }
    catch (e) { return reply({ error: 'Order book unavailable.' }, 503); }
  }
  if (route === 'leaderboard') {
    try {
      const rows = (await board.all()).filter(Boolean);
      const top = rows
        .map(r => ({ name: String(r.name || 'Trader').split(' ')[0].slice(0, 20), equity: r.equity, pct: (r.equity - START) / START * 100 }))
        .sort((a, b) => b.pct - a.pct).slice(0, 20);
      return cached({ top, total: rows.length }, 20);
    } catch (e) { return reply({ top: [], total: 0 }); }
  }

  if (route === 'register' && post) {
    const email = String(body.email || '').trim().toLowerCase();
    const password = String(body.password || '');
    let name = String(body.name || '').trim().slice(0, 30);
    if (!/^\S+@\S+\.\S+$/.test(email)) return reply({ error: 'Enter a valid email.' }, 400);
    if (password.length < 8) return reply({ error: 'Password must be at least 8 characters.' }, 400);
    if (!name) name = email.split('@')[0];
    if (await users.get(keyOf(email), { type: 'json' })) return reply({ error: 'That email is already registered.' }, 400);
    const user = {
      email, name, created: new Date().toISOString(), hash: bcrypt.hashSync(password, 10),
      bal: { USDT: START }, trades: [], orders: [], positions: [], earn: [], alerts: [], snaps: []
    };
    await users.setJSON(keyOf(email), user);
    return reply({ token: jwt.sign({ email }, SECRET, { expiresIn: '7d' }), name });
  }

  if (route === 'login' && post) {
    const email = String(body.email || '').trim().toLowerCase();
    if (await isLocked(email)) return reply({ error: 'Too many failed attempts. Try again in 10 minutes.' }, 429);
    const user = await users.get(keyOf(email), { type: 'json' });
    if (!user || !bcrypt.compareSync(String(body.password || ''), user.hash)) {
      await addFail(email);
      return reply({ error: 'Wrong email or password.' }, 401);
    }
    await limits.delete(lockKey(email));
    return reply({ token: jwt.sign({ email }, SECRET, { expiresIn: '7d' }), name: user.name || email.split('@')[0] });
  }

  /* ---------- logged-in routes ---------- */
  let me = null;
  try { me = jwt.verify((req.headers.get('authorization') || '').replace('Bearer ', ''), SECRET); } catch (e) {}
  if (!me) return reply({ error: 'Please log in again.' }, 401);
  const user = await users.get(keyOf(me.email), { type: 'json' });
  if (!user) return reply({ error: 'Please log in again.' }, 401);
  ['trades', 'orders', 'positions', 'earn', 'alerts', 'snaps'].forEach(k => { if (!user[k]) user[k] = []; });
  if (!user.bal) user.bal = { USDT: START };
  const save = () => users.setJSON(keyOf(me.email), user);

  // Record a portfolio snapshot and update the leaderboard
  const snapshot = async (prices, force) => {
    const last = user.snaps[user.snaps.length - 1];
    if (!force && last && Date.now() - last.t < SNAP_MS) return false;
    const v = equity(user, prices);
    if (!Number.isFinite(v)) return false;
    user.snaps.push({ t: Date.now(), v });
    user.snaps = user.snaps.slice(-288);
    if (user.hideBoard) await board.delete(keyOf(user.email));
    else await board.setJSON(keyOf(user.email), { name: user.name, equity: v, updated: Date.now() });
    return true;
  };

  if (route === 'state') {
    const prices = await safePrices();
    const events = settle(user, prices);
    const last = user.snaps[user.snaps.length - 1];
    const due = !last || Date.now() - last.t > 15 * 60 * 1000;
    const snapped = events.length || due ? await snapshot(prices, !!events.length) : false;
    if (events.length || snapped) await save();
    return reply({
      me: { email: user.email, name: user.name || user.email.split('@')[0], created: user.created || null, hideBoard: !!user.hideBoard },
      wallet: Object.entries(user.bal).filter(([, a]) => a > 1e-12).map(([coin, amount]) => ({ coin, amount })),
      history: user.trades.slice(0, 300),
      orders: user.orders, positions: user.positions, alerts: user.alerts, snaps: user.snaps,
      earn: user.earn.map(e => ({ ...e, accrued: accrued(e) })),
      apy: APY, events
    });
  }

  if (!post) return reply({ error: 'Not found' }, 404);

  if (route === 'profile') {
    if (body.name !== undefined) {
      const name = String(body.name).trim().slice(0, 30);
      if (!name) return reply({ error: 'Name cannot be empty.' }, 400);
      user.name = name;
    }
    if (typeof body.hideBoard === 'boolean') {
      user.hideBoard = body.hideBoard;
      if (user.hideBoard) await board.delete(keyOf(user.email));
      else await snapshot(await safePrices(), true);
    }
    await save();
    return reply({ name: user.name, hideBoard: !!user.hideBoard });
  }

  if (route === 'password' || route === 'delete') {
    if (await isLocked(me.email)) return reply({ error: 'Too many failed attempts. Try again in 10 minutes.' }, 429);
    const given = route === 'password' ? body.current : body.password;
    if (!bcrypt.compareSync(String(given || ''), user.hash)) {
      await addFail(me.email);
      return reply({ error: route === 'password' ? 'Your current password is wrong.' : 'Wrong password.' }, 400);
    }
    if (route === 'password') {
      const next = String(body.next || '');
      if (next.length < 8) return reply({ error: 'New password must be at least 8 characters.' }, 400);
      user.hash = bcrypt.hashSync(next, 10);
      await save();
      return reply({ ok: true });
    }
    await users.delete(keyOf(me.email));
    await board.delete(keyOf(me.email));
    return reply({ ok: true });
  }

  if (route === 'reset') {
    Object.assign(user, { bal: { USDT: START }, trades: [], orders: [], positions: [], earn: [], alerts: [], snaps: [] });
    await snapshot(await safePrices(), true);
    await save();
    return reply({ ok: true });
  }

  /* ----- spot market order ----- */
  if (route === 'trade') {
    const { side, coin } = body, qty = Number(body.qty);
    if (!['buy', 'sell'].includes(side) || !COINS.includes(coin) || !Number.isFinite(qty) || qty <= 0)
      return reply({ error: 'Invalid order.' }, 400);
    const prices = await safePrices();
    const price = prices[coin];
    if (!price) return reply({ error: 'Price unavailable, try again.' }, 503);
    const cost = qty * price, fee = cost * FEE;
    if (side === 'buy') {
      if (bal(user, 'USDT') < cost + fee) return reply({ error: 'Not enough USDT.' }, 400);
      user.bal.USDT = bal(user, 'USDT') - cost - fee;
      user.bal[coin] = bal(user, coin) + qty;
    } else {
      if (bal(user, coin) < qty) return reply({ error: 'Not enough ' + coin + '.' }, 400);
      user.bal[coin] = bal(user, coin) - qty;
      user.bal.USDT = bal(user, 'USDT') + cost - fee;
    }
    log(user, { type: 'spot', side, coin, qty, price, fee });
    await snapshot(prices, true);
    await save();
    return reply({ ok: true, price, cost, fee });
  }

  /* ----- limit orders ----- */
  if (route === 'order') {
    const { side, coin } = body, qty = Number(body.qty), limit = Number(body.limit);
    if (!['buy', 'sell'].includes(side) || !COINS.includes(coin) || !(qty > 0) || !(limit > 0) || !Number.isFinite(qty + limit))
      return reply({ error: 'Invalid order.' }, 400);
    if (user.orders.length >= 20) return reply({ error: 'You can have at most 20 open orders.' }, 400);
    const o = { id: uid(), side, coin, qty, limit, created: now(), reserved: 0 };
    if (side === 'buy') {
      o.reserved = qty * limit * (1 + FEE);
      if (bal(user, 'USDT') < o.reserved) return reply({ error: 'Not enough USDT.' }, 400);
      user.bal.USDT = bal(user, 'USDT') - o.reserved;
    } else {
      if (bal(user, coin) < qty) return reply({ error: 'Not enough ' + coin + '.' }, 400);
      user.bal[coin] = bal(user, coin) - qty;
      o.reserved = qty;
    }
    user.orders.push(o);
    const events = settle(user, await safePrices());
    await save();
    return reply({ ok: true, filled: events.length > 0, events });
  }

  if (route === 'order/cancel') {
    const o = user.orders.find(x => x.id === body.id);
    if (!o) return reply({ error: 'Order not found.' }, 404);
    if (o.side === 'buy') user.bal.USDT = bal(user, 'USDT') + o.reserved;
    else user.bal[o.coin] = bal(user, o.coin) + o.reserved;
    user.orders = user.orders.filter(x => x.id !== o.id);
    await save();
    return reply({ ok: true });
  }

  /* ----- futures ----- */
  if (route === 'futures/open') {
    const { coin, side } = body, lev = Number(body.lev), margin = Number(body.margin);
    if (!COINS.includes(coin) || !['long', 'short'].includes(side) || !LEVS.includes(lev) || !(margin >= 10))
      return reply({ error: 'Invalid position. Minimum margin is 10 USDT.' }, 400);
    if (user.positions.length >= 10) return reply({ error: 'You can have at most 10 open positions.' }, 400);
    if (bal(user, 'USDT') < margin) return reply({ error: 'Not enough USDT.' }, 400);
    const price = (await safePrices())[coin];
    if (!price) return reply({ error: 'Price unavailable, try again.' }, 503);
    user.bal.USDT = bal(user, 'USDT') - margin;
    const pos = { id: uid(), coin, side, lev, entry: price, margin, size: margin * lev / price, opened: now() };
    user.positions.push(pos);
    log(user, { type: 'futures', side, coin, qty: pos.size, price, fee: 0, note: `${lev}x, margin ${margin} USDT` });
    await save();
    return reply({ ok: true });
  }

  if (route === 'futures/close') {
    const pos = user.positions.find(x => x.id === body.id);
    if (!pos) return reply({ error: 'Position not found.' }, 404);
    const prices = await safePrices();
    const price = prices[pos.coin];
    if (!price) return reply({ error: 'Price unavailable, try again.' }, 503);
    const pnl = pnlOf(pos, price);
    user.bal.USDT = bal(user, 'USDT') + Math.max(0, pos.margin + pnl);
    user.positions = user.positions.filter(x => x.id !== pos.id);
    log(user, { type: 'futures', side: 'close ' + pos.side, coin: pos.coin, qty: pos.size, price, fee: 0,
      note: `PnL ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)} USDT` });
    await snapshot(prices, true);
    await save();
    return reply({ ok: true, pnl });
  }

  /* ----- earn ----- */
  if (route === 'earn/subscribe') {
    const { coin } = body, amt = Number(body.amt);
    if (!APY[coin] || !(amt > 0) || !Number.isFinite(amt)) return reply({ error: 'Invalid amount.' }, 400);
    if (bal(user, coin) < amt) return reply({ error: 'Not enough ' + coin + '.' }, 400);
    user.bal[coin] = bal(user, coin) - amt;
    user.earn.push({ id: uid(), coin, amt, apy: APY[coin], t: Date.now() });
    log(user, { type: 'earn', side: 'subscribe', coin, qty: amt, price: 0, fee: 0, note: `${APY[coin]}% APY` });
    await save();
    return reply({ ok: true });
  }

  if (route === 'earn/redeem') {
    const e = user.earn.find(x => x.id === body.id);
    if (!e) return reply({ error: 'Subscription not found.' }, 404);
    const total = e.amt + accrued(e);
    user.bal[e.coin] = bal(user, e.coin) + total;
    user.earn = user.earn.filter(x => x.id !== e.id);
    log(user, { type: 'earn', side: 'redeem', coin: e.coin, qty: total, price: 0, fee: 0, note: `interest ${(total - e.amt).toFixed(8)}` });
    await save();
    return reply({ ok: true, total });
  }

  /* ----- price alerts ----- */
  if (route === 'alert/add') {
    const { coin, dir } = body, price = Number(body.price);
    if (!COINS.includes(coin) || !['above', 'below'].includes(dir) || !(price > 0) || !Number.isFinite(price))
      return reply({ error: 'Invalid alert.' }, 400);
    if (user.alerts.length >= 20) return reply({ error: 'You can have at most 20 alerts.' }, 400);
    user.alerts.push({ id: uid(), coin, dir, price, done: false, created: now() });
    await save();
    return reply({ ok: true });
  }

  if (route === 'alert/delete') {
    user.alerts = user.alerts.filter(x => x.id !== body.id);
    await save();
    return reply({ ok: true });
  }

  return reply({ error: 'Not found' }, 404);
}

export default { fetch: handle };