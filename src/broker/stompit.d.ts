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
  }

  export class Client {
    subscribe(headers: Client.SubscribeHeaders, callback: (error: Error | null, message: Client.Message) => void): Client.Subscription;
    send(headers: Client.SendHeaders): NodeJS.WritableStream;
    ack(message: Client.Message): void;
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

    interface Message extends NodeJS.ReadableStream {
      headers: Record<string, string | number>;
    }

    interface Subscription {
      unsubscribe(): void;
    }
  }

  export class ConnectFailover {
    constructor(servers: ServerConfig[]);
    connect(callback: (error: Error | null, client: Client) => void): void;
  }
}
