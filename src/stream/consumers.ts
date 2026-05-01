import { Consumer } from "./consumer";
import { Queue } from "./queue";
import { Source } from "./source";
import { Stream } from "./stream";

export class Consumers<VALUE, NAME extends string> implements Iterable<Consumer<VALUE, NAME>> {
  private _list = new Map<string, Consumer<VALUE, any>>();
  private _created?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerCreated`>;
  private _removed?: Stream<Consumer<VALUE, any>, never, `${NAME}ConsumerRemoved`>;
  private _terminated?: Stream<void, never, `${NAME}ListTerminated`>;

  constructor(
    public readonly name: NAME,
    private options: Consumers.Options<VALUE> = {},
  ) {}
  push<const T extends VALUE>(value: T) {
    const consumers = this._list;

    const progresses: Consumer.PushProgress<T, NAME>[] = [];

    for (const consumer of consumers.values()) {
      progresses.push(consumer.push(value) as never);
    }
    return progresses;
  }
  getConsumer<NAME extends string>(name: NAME, queue?: Queue<VALUE, NAME>): Consumer<VALUE, NAME> {
    let consumer = this._list.get(name);
    if (consumer) return consumer;

    consumer = new Consumer<VALUE, NAME>(name, {
      queue,
      source: this.options?.source,
      onTerminate: () => {
        this._list.delete(name);
        this._removed?.push(consumer!);
      },
    });
    this._list.set(name, consumer);
    this._created?.push(consumer);
    return consumer;
  }

  terminate() {
    for (const consumer of this._list.values()) {
      consumer.terminate();
    }
    this._terminated?.push(undefined);
  }
  [Symbol.iterator]() {
    return this._list.values();
  }
  get count() {
    return this._list.size;
  }
  get created() {
    if (!this._created) {
      this._created = new Stream(`${this.name}ConsumerCreated`);
    }
    return this._created;
  }
  get removed() {
    if (!this._removed) {
      this._removed = new Stream(`${this.name}ConsumerRemoved`);
    }
    return this._removed;
  }
  get terminated() {
    if (!this._terminated) {
      this._terminated = new Stream(`${this.name}ListTerminated`);
    }
    return this._terminated;
  }
}
export namespace Consumers {
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
  };
}
