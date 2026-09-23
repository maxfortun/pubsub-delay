import { Message } from '../broker/types.js';
import { Transform, TransformContext, TransformResult, TransformStage } from './types.js';

// Wire format for both request and response bodies. The body is base64 so binary
// payloads (compressed, encrypted) survive JSON.
export interface HttpTransformMessage {
  key?: string;
  headers: Record<string, string>;
  body: string;
}

export interface HttpTransformRequest extends HttpTransformMessage {
  stage: TransformStage;
  topic: string;
  destination?: string;
}

// Client errors that are worth retrying rather than treating as a rejection
const RETRYABLE_4XX = new Set([408, 429]);

// POSTs the message to a per-stage URL.
//   200 + JSON message  -> forward the returned message (omitted fields are kept)
//   204                 -> forward unchanged
//   4xx                 -> reject (drop), e.g. 401/403 from an auth check
//   5xx, 408, 429, timeout, network or malformed reply -> retry later
// A stage without a URL is a pass-through.
export class HttpTransform implements Transform {
  readonly name = 'http';
  private urls: Partial<Record<TransformStage, string>>;
  private timeoutMs: number;

  constructor(urls: Partial<Record<TransformStage, string>>, timeoutMs: number) {
    this.urls = urls;
    this.timeoutMs = timeoutMs;
  }

  async apply(message: Message, context: TransformContext): Promise<TransformResult> {
    const url = this.urls[context.stage];
    if (!url) return { action: 'forward', message };

    const request: HttpTransformRequest = {
      stage: context.stage,
      topic: context.topic,
      ...(context.destination !== undefined && { destination: context.destination }),
      ...(message.key !== undefined && { key: message.key }),
      headers: message.headers,
      body: message.body.toString('base64'),
    };

    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(request),
      signal: AbortSignal.timeout(this.timeoutMs),
    });

    if (res.status === 204) return { action: 'forward', message };

    if (res.ok) {
      return { action: 'forward', message: this.decode(await res.json(), message) };
    }

    const detail = (await res.text()).slice(0, 200);
    if (res.status >= 400 && res.status < 500 && !RETRYABLE_4XX.has(res.status)) {
      return { action: 'reject', reason: `${context.stage}-transform returned ${res.status}${detail ? `: ${detail}` : ''}` };
    }
    throw new Error(`${context.stage}-transform returned ${res.status}${detail ? `: ${detail}` : ''}`);
  }

  private decode(reply: unknown, original: Message): Message {
    if (!reply || typeof reply !== 'object') throw new Error('Transform reply is not a JSON object');
    const { key, headers, body } = reply as Partial<HttpTransformMessage>;

    if (key !== undefined && typeof key !== 'string') throw new Error('Transform reply key must be a string');
    if (body !== undefined && typeof body !== 'string') throw new Error('Transform reply body must be a base64 string');
    if (headers !== undefined) {
      if (!headers || typeof headers !== 'object' || Array.isArray(headers)) {
        throw new Error('Transform reply headers must be an object');
      }
      for (const [k, v] of Object.entries(headers)) {
        if (typeof v !== 'string') throw new Error(`Transform reply header ${k} must be a string`);
      }
    }

    return {
      key: key ?? original.key,
      headers: headers ?? original.headers,
      body: body !== undefined ? Buffer.from(body, 'base64') : original.body,
    };
  }
}
