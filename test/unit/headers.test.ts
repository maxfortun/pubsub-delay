import { describe, it } from 'node:test';
import assert from 'node:assert';
import { getHeader, withoutHeader } from '../../src/headers.js';

describe('getHeader', () => {
  it('returns an exact match', () => {
    assert.strictEqual(getHeader({ delay_key: 'a' }, 'delay_key'), 'a');
  });

  it('matches regardless of case', () => {
    assert.strictEqual(getHeader({ DELAY_KEY: 'a' }, 'delay_key'), 'a');
    assert.strictEqual(getHeader({ delay_key: 'a' }, 'Delay_Key'), 'a');
  });

  it('returns undefined when absent', () => {
    assert.strictEqual(getHeader({ other: 'a' }, 'delay_key'), undefined);
  });
});

describe('withoutHeader', () => {
  it('removes every casing of the name and keeps the rest', () => {
    const out = withoutHeader({ delay_enqueued_at: '1', DELAY_ENQUEUED_AT: '2', keep: 'x' }, 'delay_enqueued_at');
    assert.deepStrictEqual(out, { keep: 'x' });
  });
});
