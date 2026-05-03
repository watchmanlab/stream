import { Consumer } from "./consumer";
import { Source } from "./source";
import { Stream } from "./stream";

export class Dispatcher<VALUE, NAME extends string> implements Iterable<Consumer<VALUE, NAME>> {
  private _consumers = new Map<string, Consumer<VALUE, any>>();
  private _consumerCreated?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerCreated`>;
  private _consumerDeleted?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerDeleted`>;
  private _terminated?: Stream<void, never, `${NAME}Terminated`>;

  constructor(
    public readonly name: NAME,
    private options: Dispatcher.Options<VALUE> = {},
  ) {}
  dispatch<const T extends VALUE>(value: T) {
    const consumers = this._consumers;

    const progresses: Consumer.PushProgress<T, NAME>[] = [];

    for (const consumer of consumers.values()) {
      progresses.push(consumer.push(value) as never);
    }
    return progresses;
  }

  hasConsumer(name: string): boolean {
    return this._consumers.has(name);
  }
  getConsumer<NAME extends string>(name?: NAME): Consumer<VALUE, NAME> {
    if (!name) {
      while (true) {
        name = `c${(globalThis.crypto.getRandomValues(new Uint32Array(1))[0] % 900000) + 100000}` as NAME;
        if (!this._consumers.has(name)) break;
      }
    }
    let consumer = this._consumers.get(name);
    if (consumer) return consumer;

    consumer = new Consumer<VALUE, NAME>(name, {
      source: this.options?.source,
      onTerminate: () => {
        this.deleteConsumer(consumer!);
      },
    });

    this._consumers.set(name, consumer);
    this._consumerCreated?.push(consumer);
    return consumer;
  }
  addConsumer<CONSUMER extends Consumer<VALUE, any>>(consumer: CONSUMER): CONSUMER {
    this._consumers.set(consumer.name, consumer);
    consumer.terminated.next().then(() => {
      this.deleteConsumer(consumer);
    });
    return consumer;
  }
  deleteConsumer(consumer: Consumer<VALUE, any>): boolean {
    if (this._consumers.delete(consumer.name)) {
      this._consumerDeleted?.push(consumer!);
      return true;
    }
    return false;
  }
  terminate() {
    for (const consumer of this) {
      consumer.return();
    }
    this._terminated?.push(undefined);
  }
  [Symbol.iterator]() {
    return this._consumers.values();
  }
  get consumersCount() {
    return this._consumers.size;
  }
  get consumerCreated() {
    if (!this._consumerCreated) {
      this._consumerCreated = new Stream(`${this.name}ConsumerCreated`);
    }
    return this._consumerCreated;
  }
  get consumerDeleted() {
    if (!this._consumerDeleted) {
      this._consumerDeleted = new Stream(`${this.name}ConsumerDeleted`);
    }
    return this._consumerDeleted;
  }
  get terminated() {
    if (!this._terminated) {
      this._terminated = new Stream(`${this.name}Terminated`);
    }
    return this._terminated;
  }
}
export namespace Dispatcher {
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
  };
}
