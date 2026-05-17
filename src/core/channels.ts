import { Stream } from "./stream.ts";
import { Channel } from "./channel.ts";

export class Channels<VALUE> implements Iterable<Channel<VALUE>>, Disposable {
  private _collection = new Set<Channel<VALUE>>();
  private _attached?: Stream<Channel<VALUE>, `ChannelAttached`>;
  private _detached?: Stream<Channel<VALUE>, `ChannelDetached`>;
  private _cleared?: Stream<void, `ChannelsCleared`>;
  private _disposed?: Stream<void, `ChannelsDisposed`>;

  constructor(private stream: Stream.AnyStream) {}
  [Symbol.iterator]() {
    return this._collection.values();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  get(options?: Channel.Options<VALUE>): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      next: options?.next,
      return: () => {
        this._collection.delete(channel);
        this._detached?.push(channel);
        options?.return?.();
      },
      ready: () => {
        this.stream.source?.next();
        options?.ready?.();
      },
    });
    this._collection.add(channel);
    this._attached?.push(channel);
    return channel;
  }
  clear(): void {
    for (const channel of this) {
      channel.return();
    }

    this._cleared?.push();

    this._cleared?.dispose();
  }
  dispose(): void {
    this.clear();
    this._attached?.dispose();
    this._detached?.dispose();

    this._disposed?.push();
    this._disposed?.dispose();

    this._cleared = this._attached = this._detached = this._disposed = undefined;
  }
  get count() {
    return this._collection.size;
  }
  get attached() {
    if (!this._attached) this._attached = new Stream(`ChannelAttached`);
    return this._attached;
  }
  get detached() {
    if (!this._detached) this._detached = new Stream(`ChannelDetached`);
    return this._detached;
  }
  get cleared() {
    if (!this._cleared) this._cleared = new Stream(`ChannelsCleared`);
    return this._cleared;
  }
  get disposed() {
    if (!this._disposed) this._disposed = new Stream(`ChannelsDisposed`);
    return this._disposed;
  }
}
