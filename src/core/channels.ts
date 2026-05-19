import { Stream } from "./stream.ts";
import { Channel } from "./channel.ts";

export class Channels<VALUE> implements Iterable<Channel<VALUE>>, Disposable {
  private _collection = new Set<Channel<VALUE>>();

  constructor(
    private stream: Stream.AnyStream,
    private options?: Channels.Options<VALUE>,
  ) {}
  [Symbol.iterator]() {
    return this._collection.values();
  }
  [Symbol.dispose]() {
    this.clear();
  }
  get(options: Channel.Options<VALUE>): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      ...options,
      onReturn: () => {
        this._collection.delete(channel);
        this.options?.detached?.(channel);
        options?.onReturn?.();
      },
      onReady: () => {
        this.stream.source?.next();
        options?.onReady?.();
      },
    });
    this._collection.add(channel);
    this.options?.attached?.(channel);
    return channel;
  }
  clear(): void {
    const array: Channel<VALUE>[] = [];
    for (const channel of this) {
      channel.return();
      array.push(channel);
    }

    this.options?.cleared?.(array);
  }

  get count() {
    return this._collection.size;
  }
}

export namespace Channels {
  export type Options<VALUE> = {
    attached?: (channel: Channel<VALUE>) => void;
    detached?: (channel: Channel<VALUE>) => void;
    cleared?: (channels: Channel<VALUE>[]) => void;
  };
}
