declare module 'stompit' {
  export interface ConnectHeaders {
    host: string;
    login: string;
    passcode: string;
    'heart-beat': string;
  }

  export interface ServerConfig {
    host: string;
    port: number;
    connectHeaders: ConnectHeaders;
    // Send heartbeats this many ms before the negotiated interval
    heartbeatOutputMargin?: number;
    // Tolerate incoming heartbeats this many ms late
    heartbeatDelayMargin?: number;
  }

  export class Client {
    subscribe(headers: Client.SubscribeHeaders, callback: (error: Error | null, message: Client.Message) => void): Client.Subscription;
    send(headers: Client.SendHeaders, options?: Client.SendOptions): NodeJS.WritableStream;
    ack(message: Client.Message, headers?: Record<string, string>, options?: Client.SendOptions): void;
    on(event: 'error', listener: (error: Error) => void): this;
    setMaxListeners(n: number): this;
    disconnect(): void;
  }

  export namespace Client {
    interface SubscribeHeaders {
      destination: string;
      ack: 'auto' | 'client' | 'client-individual';
      [key: string]: string;
    }

    interface SendHeaders {
      destination: string;
      'content-type'?: string;
      [key: string]: string | undefined;
    }

    // Setting onReceipt adds a receipt header; onError fires if the connection fails first
    interface SendOptions {
      onReceipt?: () => void;
      onError?: (error: Error) => void;
    }

    interface Message extends NodeJS.ReadableStream {
      headers: Record<string, string | number>;
    }

    interface Subscription {
      unsubscribe(headers?: Record<string, string>): void;
    }
  }

  export class ConnectFailover {
    constructor(servers: ServerConfig[]);
    connect(callback: (error: Error | null, client: Client) => void): void;
    on(event: 'error', listener: (error: Error) => void): this;
  }
}
