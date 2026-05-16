import { Stream } from "./stream.ts";
import { Channel } from "./channel.ts";

export class Channels<VALUE> implements Iterable<Channel<VALUE>>, AsyncDisposable, Disposable {
  private _collection = new Set<Channel<VALUE>>();
  private _attached?: Stream<Channel<VALUE>, `ChannelAttached`>;
  private _detached?: Stream<Channel<VALUE>, `ChannelDetached`>;
  private _cleared?: Stream<void, `ChannelsCleared`>;
  private _disposed?: Stream<void, `ChannelsDisposed`>;

  constructor(private stream: Stream.AnyStream) {}
  [Symbol.iterator]() {
    return this._collection.values();
  }
  async [Symbol.asyncDispose]() {
    await this.dispose();
  }
  [Symbol.dispose]() {
    this.dispose();
  }
  get(options?: Channel.Options<VALUE>) {
    const channel = new Channel<VALUE>({
      next: options?.next,
      return: () => {
        this._collection.delete(channel);
        this._detached?.push(channel);
        options?.return?.();
      },
      ready: () => {
        this.stream.source?.pull();
        options?.ready?.();
      },
    });
    this._collection.add(channel);
    this._attached?.push(channel);
    return channel;
  }
  async clear() {
    const promises = [];
    for (const channel of this) {
      promises.push(channel.return());
    }

    await Promise.all(promises);

    this._cleared?.push();

    await this._cleared?.dispose();
  }
  async dispose() {
    await Promise.all([this.clear(), this._attached?.dispose(), this._detached?.dispose()]);

    this._disposed?.push();
    await this._disposed?.dispose();

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
