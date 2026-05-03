import { Consumer } from "./consumer";
import { Source } from "./source";
import { Stream } from "./stream";

export class Dispatcher<VALUE, NAME extends string> implements Iterable<Consumer<VALUE, NAME>> {
  private _consumers = new Map<string, Consumer<VALUE, any>>();
  private _consumerAttached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerAttached`>;
  private _consumerDetached?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerDetached`>;
  private _cleared?: Stream<void, never, `${NAME}Cleared`>;

  constructor(
    public readonly name: NAME,
    private options: Dispatcher.Options<VALUE> = {},
  ) {}
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
        this.detachConsumer(consumer!);
      },
    });

    this._consumers.set(name, consumer);
    this._consumerAttached?.push(consumer);
    return consumer;
  }
  attachConsumer<CONSUMER extends Consumer<VALUE, any>>(consumer: CONSUMER): boolean {
    const found = this._consumers.get(consumer.name);
    if (found === consumer) return true;

    if (found) return false;

    this._consumers.set(consumer.name, consumer);
    consumer.terminated.next().then(() => {
      this.detachConsumer(consumer);
    });
    return true;
  }
  detachConsumer(consumer: Consumer<VALUE, any>): boolean {
    if (this._consumers.delete(consumer.name)) {
      this._consumerDetached?.push(consumer!);
      return true;
    }
    return false;
  }
  clear() {
    for (const consumer of this) {
      consumer.return();
    }
    this._cleared?.push(undefined);
  }
  [Symbol.iterator]() {
    return this._consumers.values();
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
}
export namespace Dispatcher {
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
  };
}
