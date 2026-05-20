import { Channel } from "./channel.ts";

export class Channels<VALUE> implements Iterable<Channel<VALUE>>, Disposable {
  private _collection = new Set<Channel<VALUE>>();

  constructor(private options?: Channels.Options<VALUE>) {}
  [Symbol.iterator](): SetIterator<Channel<VALUE>> {
    return this._collection.values();
  }
  [Symbol.dispose](): void {
    this.clear();
  }
  get(options: Channel.Options<VALUE>): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      ...options,
      return: () => {
        this._collection.delete(channel);
        this.options?.detach?.(channel);
        options?.return?.();
      },
      ready: () => {
        this.options?.ready?.();
        options?.ready?.();
      },
    });
    this._collection.add(channel);
    this.options?.attach?.(channel);
    return channel;
  }
  clear(): void {
    const array: Channel<VALUE>[] = [];
    for (const channel of this) {
      channel.return();
      array.push(channel);
    }

    this.options?.clear?.(array);
  }
  get count() {
    return this._collection.size;
  }
}

export namespace Channels {
  export type Options<VALUE> = {
    attach?: (channel: Channel<VALUE>) => void;
    detach?: (channel: Channel<VALUE>) => void;
    clear?: (channels: Channel<VALUE>[]) => void;
    ready?: () => void;
  };
}
