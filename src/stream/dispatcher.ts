import { Consumer } from "./consumer";
import { Source } from "./source";
import { Stream } from "./stream";

export class Dispatcher<VALUE, NAME extends string>
  implements Iterable<Consumer<VALUE, NAME>>, AsyncDisposable, Disposable
{
  private _consumers = new Map<string, Consumer<VALUE, any>>();
  private _consumerAttached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerAttached`>;
  private _consumerDetached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerDetached`>;
  private _cleared?: Stream<undefined, never, `${NAME}Cleared`>;
  private _disposed?: Stream<undefined, never, `${NAME}Disposed`>;

  constructor(
    public readonly name: NAME,
    private options: Dispatcher.Options<VALUE> = {},
  ) {}
  [Symbol.iterator]() {
    return this._consumers.values();
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  dispatch<const T extends VALUE>(value: T): Consumer.PushProgress<T, string>[] {
    const consumers = this._consumers;

    const progresses: Consumer.PushProgress<T, string>[] = [];

    for (const consumer of consumers.values()) {
      progresses.push(consumer.push(value) as never);
    }
    return progresses;
  }
  hasConsumer(name: string): boolean {
    return this._consumers.has(name);
  }
  getConsumer<NAME extends string = string>(name?: NAME): Consumer<VALUE, NAME> {
    if (!name) {
      while (true) {
        name = `consumer${globalThis.crypto.getRandomValues(new Uint32Array(1))[0]}` as NAME;
        if (!this._consumers.has(name)) break;
      }
    }
    let consumer = this._consumers.get(name);
    if (consumer) return consumer;

    consumer = new Consumer<VALUE, NAME>(name, {
      source: this.options?.source,
      onTerminate: () => {
        this._consumers.delete(name);
        this._consumerDetached?.push(consumer!);
      },
    });

    this._consumers.set(name, consumer);
    this._consumerAttached?.push(consumer);
    return consumer;
  }

  async clear() {
    const promises = [];
    for (const consumer of this) {
      promises.push(consumer.return());
    }

    await Promise.all(promises);
    this._cleared?.push(undefined);
  }
  async dispose() {
    await Promise.all([
      this.clear(),
      this._consumerAttached?.dispose(),
      this._consumerDetached?.dispose(),
      this._cleared?.dispose(),
    ]);
    this._consumerAttached = this._consumerDetached = this._cleared = undefined;

    this._disposed?.push(undefined);
    await this._disposed?.dispose();
    this._disposed = undefined;
  }

  get consumersCount() {
    return this._consumers.size;
  }
  get consumerAttached() {
    if (!this._consumerAttached) {
      this._consumerAttached = new Stream(`${this.name}ConsumerAttached`);
    }
    return this._consumerAttached;
  }
  get consumerDetached() {
    if (!this._consumerDetached) {
      this._consumerDetached = new Stream(`${this.name}ConsumerDetached`);
    }
    return this._consumerDetached;
  }
  get cleared() {
    if (!this._cleared) {
      this._cleared = new Stream(`${this.name}Cleared`);
    }
    return this._cleared;
  }
  get disposed() {
    if (!this._disposed) {
      this._disposed = new Stream(`${this.name}Disposed`);
    }
    return this._disposed;
  }
}
export namespace Dispatcher {
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
  };
}
