// Subscription model transport for the unmodified, checksummed 2.1.281 runtime.
// This preload exits before Claude Code's entrypoint and never runs its agent loop.
import http from 'node:http';
import { once } from 'node:events';
import { createHash, timingSafeEqual } from 'node:crypto';
import { writeFileSync, renameSync } from 'node:fs';

const VERSION = '2.1.281';
const PROTOCOL = 1;
const MAX_BODY = 64 * 1024 * 1024;
const MAX_HELD = 256 * 1024 * 1024;
const MAX_ACTIVE = 64;
const MAX_FRAME = 8 * 1024 * 1024;
const MAX_RESPONSE = 128 * 1024 * 1024;
const forbidden = [
  'ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_BASE_URL',
  'ANTHROPIC_CUSTOM_HEADERS', 'ANTHROPIC_UNIX_SOCKET', 'ANTHROPIC_BETAS',
  'CLAUDE_CODE_OAUTH_TOKEN', 'CLAUDE_CODE_OAUTH_TOKEN_FILE_DESCRIPTOR', 'CCR_OAUTH_TOKEN_FILE',
  'CLAUDE_CODE_USE_BEDROCK', 'CLAUDE_CODE_USE_VERTEX', 'CLAUDE_CODE_USE_FOUNDRY',
];
const hash = text => createHash('sha256').update(text).digest('hex');
let core, model, billing, secret, identity, accountUuid, verifiedToken, verification;
let heldBytes = 0;
let lastActivity = Date.now();
const active = new Map();

function subscriptionToken() {
  const auth = core.un();
  if (!core.Ec() || !auth?.accessToken || !auth.scopes?.includes('user:inference')) {
    throw Error('Claude subscription login required. Run claude auth login for the selected account.');
  }
  return auth.accessToken;
}

async function verifyIdentity() {
  await core.Cie();
  const token = subscriptionToken();
  if (verifiedToken === token) return;
  if (verification) {
    await verification;
    return verifyIdentity();
  }
  verification = (async () => {
    const profile = await core.PMe(token);
    const account = profile?.account?.uuid;
    const organization = profile?.organization?.uuid;
    if (!account || !organization) {
      throw Error('Cannot verify the selected Claude subscription account; sign in again or retry.');
    }
    identity = hash(`claude-subscription\0${account}\0${organization}`);
    accountUuid = account;
    verifiedToken = token;
  })();
  try { await verification; } finally { verification = undefined; }
}

function capabilities(name) {
  if (typeof name !== 'string' || name.length > 200) throw Error('Invalid model');
  const resolved = core.kt(name);
  if (!core.Kr(resolved)) throw Error('The selected Claude model is unavailable for this account.');
  const spec = model.tY([{ value: resolved, label: resolved, description: '' }])[0];
  const limits = core.T4(resolved);
  return {
    model: core.BO(resolved), context_window: core.cg(resolved),
    default_output_tokens: limits.default, max_output_tokens: limits.upperLimit,
    thinking: core.uAr(resolved), adaptive_thinking: !!spec.supportsAdaptiveThinking,
    thinking_required: core.fKe(resolved), fast: !!spec.supportsFastMode,
    efforts: spec.supportedEffortLevels ?? [],
    betas: core.C8(resolved).map(beta => beta.header),
  };
}

// The attribution fingerprint is a wire-format field, not an agent decision.
function fingerprint(messages) {
  const content = messages.find(message => message.role === 'user')?.content;
  const text = typeof content === 'string' ? content : content?.find(block => block.type === 'text')?.text ?? '';
  const sample = [4, 7, 20].map(index => text[index] || '0').join('');
  return hash(`59cf53e54c78${sample}${VERSION}`).slice(0, 3);
}

function authorized(request) {
  const supplied = Buffer.from(request.headers.authorization ?? '');
  const expected = Buffer.from(`Bearer ${secret}`);
  return !request.headers.origin && supplied.length === expected.length && timingSafeEqual(supplied, expected);
}

async function readBody(request) {
  const chunks = [];
  let size = 0;
  try {
    for await (const chunk of request) {
      size += chunk.length;
      heldBytes += chunk.length;
      if (size > MAX_BODY || heldBytes > MAX_HELD) throw Error('Connector request memory limit exceeded');
      chunks.push(chunk);
    }
    return { value: JSON.parse(Buffer.concat(chunks).toString('utf8')), size };
  } catch (error) {
    heldBytes -= size;
    throw error;
  }
}

async function emit(response, value) {
  const frame = JSON.stringify(value) + '\n';
  const bytes = Buffer.byteLength(frame);
  response.borgBytes = (response.borgBytes ?? 0) + bytes;
  if (bytes > MAX_FRAME || response.borgBytes > MAX_RESPONSE) throw Error('Connector response limit exceeded');
  if (response.destroyed) throw Error('Borg client disconnected');
  if (!response.write(frame)) {
    await new Promise((resolve, reject) => {
      const cleanup = () => {
        response.off('drain', drained);
        response.off('close', closed);
        response.off('error', failed);
      };
      const drained = () => { cleanup(); resolve(); };
      const closed = () => { cleanup(); reject(Error('Borg client disconnected')); };
      const failed = error => { cleanup(); reject(error); };
      response.once('drain', drained);
      response.once('close', closed);
      response.once('error', failed);
    });
  }
}

function safeError(error) {
  // Upstream error objects can carry credentials or the submitted prompt.
  // Only the service's declared message and status cross this boundary.
  const status = Number.isInteger(error.status) ? error.status : null;
  const declared = error.error?.error?.message ?? error.error?.message;
  let message = typeof declared === 'string' ? declared : status ? `Claude model request failed (HTTP ${status})` : 'Claude connector request failed';
  if (error.borgMessage) message = error.borgMessage;
  return { status, message: message.replace(/sk-ant-[A-Za-z0-9_-]+/g, '[redacted]').slice(0, 2000) };
}

async function infer(request, response, value) {
  const { id, session_id, parent_agent_id, account_identity, body } = value;
  if (typeof id !== 'string' || !/^[a-zA-Z0-9_:.-]{1,128}$/.test(id) ||
      typeof session_id !== 'string' || !/^[a-zA-Z0-9_:.-]{1,128}$/.test(session_id) ||
      (parent_agent_id != null && !/^[a-zA-Z0-9_:.-]{1,128}$/.test(parent_agent_id)) ||
      !body || !Array.isArray(body.messages) || !Array.isArray(body.system ?? [])) {
    throw Error('Invalid inference request');
  }
  if (active.has(id)) throw Error('Duplicate inference request');
  const controller = new AbortController();
  active.set(id, controller);
  const disconnected = () => controller.abort();
  response.once('close', disconnected);
  let receivedEvent = false;
  response.writeHead(200, { 'content-type': 'application/x-ndjson', 'cache-control': 'no-store' });
  try {
    await verifyIdentity();
    if (identity !== account_identity) {
      const error = Error();
      error.borgMessage = 'Claude account changed. Reconnect before sending model state.';
      throw error;
    }
    const spec = capabilities(body.model);
    const agentContext = parent_agent_id
      ? { agentType: 'subagent', agentId: session_id, parentAgentId: parent_agent_id, isMainSession: false }
      : { agentType: 'main', agentId: session_id };
    const wire = {
      ...body, model: spec.model, stream: true,
      betas: [...new Set([...spec.betas, ...(body.betas ?? [])])],
      system: [{ type: 'text', text: billing.LEr(fingerprint(body.messages), agentContext, undefined, undefined, undefined, { ignoreEnvOptOut: true }) }, ...(body.system ?? [])],
      metadata: { user_id: JSON.stringify({ device_id: core.zO(), account_uuid: accountUuid, session_id }) },
    };
    const clientForCall = async () => {
      const client = await model.kG({ maxRetries: 0, model: spec.model, source: 'borg_model_connector', agentContext });
      if (client.apiKey || !client.authToken || new URL(client.baseURL).origin !== 'https://api.anthropic.com') {
        const error = Error();
        error.borgMessage = 'Claude subscription transport refused a different credential or endpoint.';
        throw error;
      }
      if (client.authToken !== verifiedToken) {
        await verifyIdentity();
        if (identity !== account_identity || client.authToken !== verifiedToken) throw Error('Claude account changed');
      }
      return client;
    };
    await emit(response, { type: 'started', id, pid: process.pid, model: spec.model, account_identity: identity, betas: wire.betas });
    for (let attempt = 0; attempt < 2; attempt++) {
      const client = await clientForCall();
      try {
        const { data: stream, response: upstream } = await client.beta.messages.create(wire, {
          signal: controller.signal,
          headers: { 'X-Claude-Code-Session-Id': session_id, 'x-client-request-id': id },
        }).withResponse();
        const grace = ['5h', '7d'].map(window => {
          const value = Number(upstream.headers.get(`anthropic-ratelimit-unified-grace-${window}-utilization`) ?? 0);
          return Number.isFinite(value) && value > 0 ? value : 0;
        });
        const overage = upstream.headers.get('anthropic-ratelimit-unified-overage-status');
        if (upstream.headers.has('anthropic-ratelimit-unified-grace-status') &&
            grace.some(value => value > 0) &&
            upstream.headers.get('anthropic-ratelimit-unified-overage-in-use') !== 'true' &&
            overage !== 'allowed' && overage !== 'allowed_warning') {
          await emit(response, { type: 'grace', id, five_hour: grace[0], weekly: grace[1] });
        }
        for await (const event of stream) {
          receivedEvent = true;
          await emit(response, { type: 'event', id, event });
        }
        break;
      } catch (error) {
        if (attempt !== 0 || receivedEvent || controller.signal.aborted || error.status !== 401) throw error;
        if (!await core._y(client.authToken)) throw error;
        await verifyIdentity();
        if (identity !== account_identity) throw Error('Claude account changed during recovery');
      }
    }
    await emit(response, { type: controller.signal.aborted ? 'cancelled' : 'done', id });
  } catch (error) {
    if (!response.destroyed) {
      await emit(response, { type: controller.signal.aborted ? 'cancelled' : 'error', id, ...safeError(error) }).catch(() => {});
    }
  } finally {
    controller.abort();
    active.delete(id);
    lastActivity = Date.now();
    response.off('close', disconnected);
    response.end();
  }
}

async function serve(request, response) {
  lastActivity = Date.now();
  if (!authorized(request)) { response.writeHead(403).end(); return; }
  if (request.method !== 'POST' || !['/info', '/infer', '/cancel'].includes(request.url)) {
    response.writeHead(404).end(); return;
  }
  if (active.size >= MAX_ACTIVE && request.url === '/infer') { response.writeHead(429).end(); return; }
  let size = 0;
  try {
    const parsed = await readBody(request);
    size = parsed.size;
    if (request.url === '/infer') {
      await infer(request, response, parsed.value);
    } else {
      if (request.url === '/cancel') {
        active.get(parsed.value.id)?.abort();
        response.writeHead(200).end('{}');
      } else {
        await verifyIdentity();
        const result = { protocol: PROTOCOL, version: VERSION, pid: process.pid, account_identity: identity, auth: 'subscription_oauth', agent_loop: false, active_requests: active.size };
        if (parsed.value.model) result.capabilities = capabilities(parsed.value.model);
        response.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' }).end(JSON.stringify(result));
      }
    }
  } catch (error) {
    if (!response.headersSent) response.writeHead(400, { 'content-type': 'application/json' }).end(JSON.stringify(safeError(error)));
    else response.destroy();
  } finally { heldBytes -= size; }
}

try {
  for (const name of forbidden) if (process.env[name]) throw Error(`${name} conflicts with the selected Claude subscription login`);
  let bootstrap = '';
  for await (const chunk of process.stdin) {
    bootstrap += chunk;
    if (bootstrap.length > 4096) throw Error('Invalid connector bootstrap');
  }
  const options = JSON.parse(bootstrap);
  secret = options.secret;
  if (!/^[a-f0-9]{64}$/.test(secret) || options.protocol !== PROTOCOL ||
      !/^[a-f0-9]{64}$/.test(options.revision) || typeof options.endpoint_path !== 'string') throw Error('Invalid connector bootstrap');
  [core, model, billing] = await Promise.all([
    import('/$bunfs/root/chunk-5khn4tvf.js'),
    import('/$bunfs/root/chunk-adsaemws.js'),
    import('/$bunfs/root/chunk-649gsb4b.js'),
  ]);
  await core.LKe();
  await verifyIdentity();
  const server = http.createServer({ requestTimeout: 30000, headersTimeout: 10000, maxHeaderSize: 8192 }, (request, response) => {
    serve(request, response).catch(() => response.destroy());
  });
  server.maxConnections = 128;
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const endpoint = { port: server.address().port, pid: process.pid, secret, revision: options.revision };
  const temporary = `${options.endpoint_path}.${process.pid}.tmp`;
  writeFileSync(temporary, JSON.stringify(endpoint), { mode: 0o600, flag: 'wx' });
  renameSync(temporary, options.endpoint_path);
  // A launcher can exit after the endpoint is published. Its stdout pipe is
  // no longer part of the helper's lifetime or its local model transport.
  process.stdout.on('error', () => {});
  process.stdout.write(JSON.stringify({ type: 'ready', protocol: PROTOCOL, version: VERSION, pid: process.pid, port: server.address().port, account_identity: identity, auth: 'subscription_oauth', agent_loop: false }) + '\n');
  // A detached helper is shared by independent Borg clients. Idle helpers retire.
  const idle = setInterval(() => {
    if (!active.size && Date.now() - lastActivity > 300000) server.close(() => process.exit(0));
  }, 30000);
  const stop = () => {
    clearInterval(idle);
    for (const controller of active.values()) controller.abort();
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 5000).unref();
  };
  process.on('SIGTERM', stop);
  process.on('SIGINT', stop);
  // Keep the preload alive: returning would start the upstream CLI.
  await new Promise(() => {});
} catch (error) {
  process.stdout.write(JSON.stringify({ type: 'fatal', message: String(error.message).replace(/sk-ant-[A-Za-z0-9_-]+/g, '[redacted]').slice(0, 500) }) + '\n');
  process.exit(1);
}
