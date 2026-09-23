export { SchedulerStrategy, SchedulerStats, StrategyConfig } from './types.js';
export { BoundedPoolStrategy } from './bounded-pool.js';
export { TimeWheelStrategy } from './time-wheel.js';

import { SchedulerStrategy, StrategyConfig } from './types.js';
import { BoundedPoolStrategy } from './bounded-pool.js';
import { TimeWheelStrategy } from './time-wheel.js';

export type StrategyType = 'bounded-pool' | 'time-wheel';

export function createStrategy(type: StrategyType, config: StrategyConfig): SchedulerStrategy {
  switch (type) {
    case 'bounded-pool':
      return new BoundedPoolStrategy(config);
    case 'time-wheel':
      return new TimeWheelStrategy(config);
    default:
      throw new Error(`Unknown strategy type: ${type}`);
  }
}
