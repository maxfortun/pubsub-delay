import { Broker, BrokerConfig } from './types.js';
import { KafkaBroker } from './kafka.js';
import { ActiveMQBroker } from './activemq.js';

export * from './types.js';

export function createBroker(config: BrokerConfig): Broker {
  switch (config.type) {
    case 'kafka':
      return new KafkaBroker(config.kafka);
    case 'activemq':
      return new ActiveMQBroker(config.activemq);
    default:
      throw new Error(`Unknown broker type: ${config.type}`);
  }
}
