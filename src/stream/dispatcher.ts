import { Consumer } from "./consumer";
import { Queue } from "./queue";
import { Source } from "./source";
import { Stream } from "./stream";

const NAME = "dispatcher";
export class Dispatcher<VALUE, NAME extends string = Dispatcher.Name>
  implements Iterable<Consumer<VALUE, NAME>>, AsyncDisposable, Disposable
{
  readonly name: NAME;
  private _options?: Dispatcher.Options<VALUE>;
  private _consumers = new Map<string, Consumer<VALUE, any>>();
  private _consumerAttached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerAttached`>;
  private _consumerDetached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerDetached`>;
  private _cleared?: Stream<undefined, never, `${NAME}Cleared`>;
  private _disposed?: Stream<undefined, never, `${NAME}Disposed`>;

  constructor(name: NAME, options?: Dispatcher.Options<VALUE>);
  constructor(options?: Dispatcher.Options<VALUE>);
  constructor(nameOrOptions?: NAME | Dispatcher.Options<VALUE>, options?: Dispatcher.Options<VALUE>) {
    if (typeof nameOrOptions === "string") {
      this.name = nameOrOptions;
      this._options = { ...options };
    } else {
      this.name = NAME as NAME;
      this._options = { ...nameOrOptions };
    }
  }
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
  getConsumer<
    NAME extends string,
    BUFFER extends Queue<VALUE, any>,
    PENDINGS extends Queue<(value: VALUE | Queue.Empty) => void, any>,
  >(name: NAME, options?: Consumer.Options<VALUE, BUFFER, PENDINGS, Source<VALUE, any, any>>): Consumer<VALUE, NAME>;
  getConsumer<BUFFER extends Queue<VALUE, any>, PENDINGS extends Queue<(value: VALUE | Queue.Empty) => void, any>>(
    options?: Consumer.Options<VALUE, BUFFER, PENDINGS, Source<VALUE, any, any>>,
  ): Consumer<VALUE, string>;
  getConsumer<
    NAME extends string,
    BUFFER extends Queue<VALUE, any>,
    PENDINGS extends Queue<(value: VALUE | Queue.Empty) => void, any>,
  >(
    nameOrOptions?: NAME | Consumer.Options<VALUE, BUFFER, PENDINGS, Source<VALUE, any, any>>,
    _options?: Consumer.Options<VALUE, BUFFER, PENDINGS, Source<VALUE, any, any>>,
  ): Consumer<VALUE, NAME> {
    let name: NAME | undefined;
    let options: Consumer.Options<VALUE, BUFFER, PENDINGS, Source<VALUE, any, any>> | undefined;

    if (typeof nameOrOptions === "string") {
      name = nameOrOptions;
      options = _options;
    } else {
      _options = nameOrOptions;
    }

    if (!name) {
      while (true) {
        name = `consumer${globalThis.crypto.getRandomValues(new Uint32Array(1))[0]}` as NAME;
        if (!this._consumers.has(name)) break;
      }
    }
    let consumer = this._consumers.get(name);
    if (consumer) return consumer;

    consumer = new Consumer<VALUE, NAME>(name, {
      source: this._options?.source,
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
  export type Name = typeof NAME;
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
  };
}
