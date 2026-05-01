import { Consumer } from "./consumer";
import { Queue } from "./queue";
import { Source } from "./source";
import { Stream } from "./stream";
export interface Consumers<VALUE, NAME extends string> {
  [index: number]: Consumer<VALUE, any>;
}
export class Consumers<VALUE, NAME extends string> implements Iterable<Consumer<VALUE, NAME>> {
  private _list: Consumer<VALUE, any>[] = [];
  private _created?: Stream<Consumer<VALUE, any>, never, `${NAME}Created`>;
  private _removed?: Stream<Consumer<VALUE, any>, never, `${NAME}Removed`>;
  private _cleared?: Stream<void, never, `${NAME}Cleared`>;

  constructor(
    public readonly name: NAME,
    private options: Consumers.Options<VALUE> = {},
  ) {
    return new Proxy(this, {
      get(target, prop: any, receiver) {
        if (!isNaN(prop)) {
          return target._list[prop];
        }
        return Reflect.get(target, prop, receiver);
      },
      set(target, prop: any, value) {
        if (!isNaN(prop)) {
          target._list[prop] = value;
          return true;
        }
        return Reflect.set(target, prop, value);
      },
    });
  }
  push<const T extends VALUE>(value: T) {
    const consumers = this._list;
    const length = consumers.length;
    const progresses: Consumer.PushProgress<T, NAME>[] = [];
    for (let i = 0; i < length; i++) {
      progresses.push(consumers[i].push(value) as never);
    }
    return progresses;
  }
  create<NAME extends string>(name: NAME, queue?: Queue<VALUE>): Consumer<VALUE, NAME> {
    const consumer = new Consumer<VALUE, NAME>(name, queue ?? new Queue(), {
      source: this.options?.source,
      onTerminate: () => {
        this.remove(consumer);
      },
    });

    this._list.push(consumer);
    this._created?.push(consumer);
    return consumer;
  }
  remove(consumer: Consumer<VALUE, any>): void {
    if (!this._list.length) return;
    const index = this._list.indexOf(consumer);
    if (index === -1) return;
    this._list.splice(index, 1);
    this._removed?.push(consumer);
  }
  clear() {
    this._list.length = 0;
    this._cleared?.push(undefined);
  }
  terminate() {}
  [Symbol.iterator]() {
    return this._list[Symbol.iterator]();
  }
  get count() {
    return this._list.length;
  }
  get created() {
    if (!this._created) {
      this._created = new Stream(`${this.name}Created`);
    }
    return this._created;
  }
  get removed() {
    if (!this._removed) {
      this._removed = new Stream(`${this.name}Removed`);
    }
    return this._removed;
  }
  get cleared() {
    if (!this._cleared) {
      this._cleared = new Stream(`${this.name}Cleared`);
    }
    return this._cleared;
  }
}
export namespace Consumers {
  export type Options<VALUE> = {
    source?: Source<VALUE, any, any>;
  };
}
