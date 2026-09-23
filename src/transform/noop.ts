import { Message } from '../broker/types.js';
import { Transform, TransformContext, TransformResult } from './types.js';

export class NoopTransform implements Transform {
  readonly name = 'none';

  async apply(message: Message, _context: TransformContext): Promise<TransformResult> {
    return { action: 'forward', message };
  }
}
