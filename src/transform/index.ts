import { Message } from '../broker/types.js';
import { transformDuration, transformTotal } from '../metrics.js';
import { HttpTransform } from './http.js';
import { NoopTransform } from './noop.js';
import { Transform, TransformConfig, TransformContext, TransformResult } from './types.js';

export * from './types.js';
export { HttpTransform } from './http.js';
export type { HttpTransformMessage, HttpTransformRequest } from './http.js';
export { NoopTransform } from './noop.js';

export function createTransform(config: TransformConfig): Transform {
  switch (config.plugin) {
    case 'none':
      return new NoopTransform();
    case 'http':
      if (!config.http.preUrl && !config.http.postUrl) {
        throw new Error('TRANSFORM_PLUGIN=http requires TRANSFORM_PRE_URL and/or TRANSFORM_POST_URL');
      }
      return new HttpTransform({ pre: config.http.preUrl, post: config.http.postUrl }, config.http.timeoutMs);
    default:
      throw new Error(`Unknown transform plugin: ${config.plugin}`);
  }
}

// Runs a transform and records its outcome. Errors are rethrown for the caller to retry.
export async function applyTransform(
  transform: Transform,
  message: Message,
  context: TransformContext
): Promise<TransformResult> {
  const labels = { plugin: transform.name, stage: context.stage };
  const startedAt = Date.now();
  try {
    const result = await transform.apply(message, context);
    transformTotal.inc({ ...labels, outcome: result.action === 'forward' ? 'forwarded' : 'rejected' });
    return result;
  } catch (error) {
    transformTotal.inc({ ...labels, outcome: 'failed' });
    throw error;
  } finally {
    transformDuration.observe(labels, Date.now() - startedAt);
  }
}
