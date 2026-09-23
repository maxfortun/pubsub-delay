import { describe, it, before, after, beforeEach } from 'node:test';
import assert from 'node:assert';
import { createServer, Server, IncomingMessage, ServerResponse } from 'http';
import { AddressInfo } from 'net';
import { gzipSync, gunzipSync } from 'zlib';
import { HttpTransform, NoopTransform, createTransform, HttpTransformRequest } from '../../src/transform/index.js';
import { Message } from '../../src/broker/types.js';

type Handler = (req: HttpTransformRequest, res: ServerResponse) => void;

function readJson(req: IncomingMessage): Promise<HttpTransformRequest> {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => resolve(JSON.parse(data)));
    req.on('error', reject);
  });
}

function reply(res: ServerResponse, status: number, body?: unknown): void {
  res.writeHead(status, body === undefined ? {} : { 'content-type': 'application/json' });
  res.end(body === undefined ? undefined : typeof body === 'string' ? body : JSON.stringify(body));
}

const message = (): Message => ({
  key: 'k1',
  headers: { Authorization: 'Bearer good', DELAY_DURATION: 'PT1S', DELAY_DESTINATION: 'out' },
  body: Buffer.from('hello'),
});

describe('HttpTransform', () => {
  let server: Server;
  let baseUrl: string;
  let handler: Handler;
  const requests: HttpTransformRequest[] = [];

  before(async () => {
    server = createServer(async (req, res) => {
      const body = await readJson(req);
      requests.push(body);
      handler(body, res);
    });
    // Port 0: let the OS pick a free port
    await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
    baseUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  });

  after(() => new Promise<void>((r) => server.close(() => r())));

  beforeEach(() => {
    requests.length = 0;
  });

  it('passes through a stage without a URL and makes no request', async () => {
    handler = (_req, res) => reply(res, 500);
    const t = new HttpTransform({ pre: `${baseUrl}/pre` }, 1000);
    const msg = message();
    const result = await t.apply(msg, { stage: 'post', topic: 'bucket', destination: 'out' });
    assert.deepStrictEqual(result, { action: 'forward', message: msg });
    assert.strictEqual(requests.length, 0);
  });

  it('sends stage, topic, destination, key, headers and base64 body', async () => {
    handler = (_req, res) => reply(res, 204);
    const t = new HttpTransform({ post: `${baseUrl}/post` }, 1000);
    await t.apply(message(), { stage: 'post', topic: 'delay-ingest-PT1S', destination: 'out' });
    assert.deepStrictEqual(requests[0], {
      stage: 'post',
      topic: 'delay-ingest-PT1S',
      destination: 'out',
      key: 'k1',
      headers: message().headers,
      body: Buffer.from('hello').toString('base64'),
    });
  });

  it('forwards the message unchanged on 204', async () => {
    handler = (_req, res) => reply(res, 204);
    const t = new HttpTransform({ pre: `${baseUrl}/pre` }, 1000);
    const msg = message();
    const result = await t.apply(msg, { stage: 'pre', topic: 'delay-ingest' });
    assert.deepStrictEqual(result, { action: 'forward', message: msg });
  });

  it('replaces the message with the 200 reply and keeps omitted fields', async () => {
    handler = (req, res) => {
      const { Authorization: _, ...headers } = req.headers;
      reply(res, 200, { headers: { ...headers, X_CHECKED: 'yes' } });
    };
    const t = new HttpTransform({ pre: `${baseUrl}/pre` }, 1000);
    const result = await t.apply(message(), { stage: 'pre', topic: 'delay-ingest' });
    assert.strictEqual(result.action, 'forward');
    if (result.action !== 'forward') return;
    assert.strictEqual(result.message.key, 'k1');
    assert.strictEqual(result.message.body.toString(), 'hello');
    assert.strictEqual(result.message.headers.Authorization, undefined);
    assert.strictEqual(result.message.headers.X_CHECKED, 'yes');
  });

  it('round-trips binary bodies (compress on pre, decompress on post)', async () => {
    handler = (req, res) => {
      const raw = Buffer.from(req.body, 'base64');
      const out = req.stage === 'pre' ? gzipSync(raw) : gunzipSync(raw);
      reply(res, 200, { body: out.toString('base64') });
    };
    const t = new HttpTransform({ pre: `${baseUrl}/pre`, post: `${baseUrl}/post` }, 1000);
    const pre = await t.apply(message(), { stage: 'pre', topic: 'delay-ingest' });
    assert.ok(pre.action === 'forward');
    assert.notStrictEqual(pre.message.body.toString(), 'hello');
    assert.strictEqual(gunzipSync(pre.message.body).toString(), 'hello');
    const post = await t.apply(pre.message, { stage: 'post', topic: 'bucket', destination: 'out' });
    assert.ok(post.action === 'forward');
    assert.strictEqual(post.message.body.toString(), 'hello');
  });

  it('rejects on 401 and 403 with the status and response text', async () => {
    for (const status of [401, 403]) {
      handler = (_req, res) => reply(res, status, 'invalid token');
      const t = new HttpTransform({ pre: `${baseUrl}/pre` }, 1000);
      const result = await t.apply(message(), { stage: 'pre', topic: 'delay-ingest' });
      assert.strictEqual(result.action, 'reject');
      if (result.action === 'reject') {
        assert.match(result.reason, new RegExp(`${status}: invalid token`));
      }
    }
  });

  it('throws on 5xx, 408 and 429 so the message is retried', async () => {
    for (const status of [500, 503, 408, 429]) {
      handler = (_req, res) => reply(res, status);
      const t = new HttpTransform({ pre: `${baseUrl}/pre` }, 1000);
      await assert.rejects(t.apply(message(), { stage: 'pre', topic: 'delay-ingest' }), new RegExp(String(status)));
    }
  });

  it('throws when the plugin does not answer within the timeout', async () => {
    handler = (_req, res) => setTimeout(() => reply(res, 204), 500);
    const t = new HttpTransform({ pre: `${baseUrl}/pre` }, 50);
    await assert.rejects(t.apply(message(), { stage: 'pre', topic: 'delay-ingest' }));
  });

  it('throws on a malformed reply', async () => {
    const t = new HttpTransform({ pre: `${baseUrl}/pre` }, 1000);
    for (const body of ['not json', { headers: { a: 1 } }, { body: 5 }, { headers: ['x'] }]) {
      handler = (_req, res) => reply(res, 200, body);
      await assert.rejects(t.apply(message(), { stage: 'pre', topic: 'delay-ingest' }));
    }
  });

  it('throws when the plugin is unreachable', async () => {
    const t = new HttpTransform({ pre: 'http://127.0.0.1:1/pre' }, 1000);
    await assert.rejects(t.apply(message(), { stage: 'pre', topic: 'delay-ingest' }));
  });
});

describe('createTransform', () => {
  const base = { retryBackoffMs: 0, http: { timeoutMs: 1000 } };

  it('defaults to a no-op that forwards the same message', async () => {
    const t = createTransform({ ...base, plugin: 'none' });
    assert.ok(t instanceof NoopTransform);
    const msg = message();
    assert.deepStrictEqual(await t.apply(msg, { stage: 'pre', topic: 'x' }), { action: 'forward', message: msg });
  });

  it('builds an http transform when at least one URL is set', () => {
    const t = createTransform({ ...base, plugin: 'http', http: { timeoutMs: 1000, postUrl: 'http://x/post' } });
    assert.ok(t instanceof HttpTransform);
  });

  it('refuses http without any URL', () => {
    assert.throws(() => createTransform({ ...base, plugin: 'http' }), /TRANSFORM_PRE_URL/);
  });

  it('refuses an unknown plugin', () => {
    assert.throws(() => createTransform({ ...base, plugin: 'nope' as never }), /Unknown transform plugin/);
  });
});
