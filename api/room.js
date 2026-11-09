// 試合表の同期API。Upstash Redis（Vercel Marketplace から接続）に保存する。
const BASE = process.env.KV_REST_API_URL || process.env.UPSTASH_REDIS_REST_URL;
const TOKEN = process.env.KV_REST_API_TOKEN || process.env.UPSTASH_REDIS_REST_TOKEN;
const TTL = 60 * 60 * 24 * 3; // 最後の更新から3日で自動削除
const MAX_STATE = 96 * 1024;
const MAX_JOINS = 64;
const ID_RE = /^[a-z0-9]{6,16}$/;
const KEY_RE = /^[a-z0-9]{8,24}$/;

// 更新を原子的に行う: 操作キーの照合 → 版の照合 → 保存
const SAVE_LUA = `
local k = redis.call('GET', KEYS[3])
if k and k ~= ARGV[3] then return -2 end
local rev = tonumber(redis.call('GET', KEYS[1]) or '0')
if rev ~= tonumber(ARGV[1]) then return -1 end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[4])
redis.call('SET', KEYS[3], ARGV[3], 'EX', ARGV[4])
rev = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ARGV[4])
redis.call('EXPIRE', KEYS[4], ARGV[4])
return rev
`;

const keys = (id) => ({
  rev: `{dbl:${id}}:rev`,
  state: `{dbl:${id}}:state`,
  key: `{dbl:${id}}:key`,
  joins: `{dbl:${id}}:joins`,
});

async function redis(commands) {
  const r = await fetch(`${BASE}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(commands),
  });
  if (!r.ok) throw new Error(`storage ${r.status}`);
  const out = await r.json();
  for (const item of out) if (item.error) throw new Error(item.error);
  return out.map((item) => item.result);
}

function send(res, status, body) {
  res.setHeader('Cache-Control', 'no-store');
  res.status(status).json({ ...body, now: Date.now() });
}

async function handleGet(req, res) {
  if (req.query.ping) return send(res, 200, { ok: true, storage: Boolean(BASE && TOKEN) });
  const id = String(req.query.id || '');
  if (!ID_RE.test(id)) return send(res, 400, { error: 'bad_id' });
  const k = keys(id);
  const known = Number(req.query.rev);
  const [revRaw] = await redis([['GET', k.rev]]);
  const rev = Number(revRaw || 0);
  if (rev === known) return send(res, 200, { same: true, rev });
  const [stateRaw, joinsRaw] = await redis([['GET', k.state], ['LRANGE', k.joins, 0, -1]]);
  const joins = (joinsRaw || []).map((j) => { try { return JSON.parse(j); } catch { return null; } }).filter(Boolean);
  return send(res, 200, { rev, state: stateRaw ? JSON.parse(stateRaw) : null, joins });
}

async function handleJoin(id, join, res) {
  const k = keys(id);
  const playerId = String(join.id || '');
  if (!/^[a-z0-9]{4,20}$/.test(playerId)) return send(res, 400, { error: 'bad_join' });
  const [exists, count] = await redis([['EXISTS', k.state], ['LLEN', k.joins]]);
  if (!exists) return send(res, 404, { error: 'not_found' });
  if (count >= MAX_JOINS) return send(res, 429, { error: 'too_many' });
  const name = String(join.name || '').replace(/[\u0000-\u001f]/g, '').trim().slice(0, 12);
  const [, rev] = await redis([
    ['RPUSH', k.joins, JSON.stringify({ id: playerId, name })],
    ['INCR', k.rev],
    ['EXPIRE', k.joins, TTL],
    ['EXPIRE', k.rev, TTL],
  ]);
  return send(res, 200, { rev });
}

async function handlePost(req, res) {
  const body = typeof req.body === 'string' ? JSON.parse(req.body || '{}') : req.body || {};
  const id = String(body.id || '');
  if (!ID_RE.test(id)) return send(res, 400, { error: 'bad_id' });
  if (body.join) return handleJoin(id, body.join, res);

  const editKey = String(body.key || '');
  if (!KEY_RE.test(editKey)) return send(res, 403, { error: 'forbidden' });
  const state = JSON.stringify(body.state || null);
  if (!body.state || state.length > MAX_STATE) return send(res, 413, { error: 'too_large' });

  const k = keys(id);
  const [result] = await redis([[
    'EVAL', SAVE_LUA, 4, k.rev, k.state, k.key, k.joins,
    String(Number(body.base) || 0), state, editKey, String(TTL),
  ]]);
  if (result === -2) return send(res, 403, { error: 'forbidden' });
  if (result === -1) return send(res, 409, { error: 'conflict' });
  return send(res, 200, { rev: result });
}

export default async function handler(req, res) {
  try {
    if (!BASE || !TOKEN) {
      if (req.method === 'GET' && req.query.ping) return send(res, 200, { ok: true, storage: false });
      return send(res, 503, { error: 'storage_not_configured' });
    }
    if (req.method === 'GET') return await handleGet(req, res);
    if (req.method === 'POST') return await handlePost(req, res);
    return send(res, 405, { error: 'method_not_allowed' });
  } catch (err) {
    console.error(err);
    return send(res, 500, { error: 'server_error' });
  }
}
