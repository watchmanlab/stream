import { Stream } from "./stream.ts";
import { Channel } from "./channel.ts";

export class Channels<VALUE, NAME extends string> implements Iterable<Channel<VALUE, string>> {
  private _collection = new Set<Channel<VALUE, any>>();
  private _attached?: Stream<Channel<VALUE, string>, `${NAME}ChannelAttached`>;
  private _detached?: Stream<Channel<VALUE, string>, `${NAME}ChannelDetached`>;
  private _cleared?: Stream<void, `${NAME}ChannelsCleared`>;
  private _disposed?: Stream<void, `${NAME}ChannelsDisposed`>;

  constructor(public readonly stream: Stream.AnyStream) {}

  [Symbol.iterator]() {
    return this._collection.values();
  }
  get(options?: Channel.Options<VALUE>) {
    const channel = new Channel<VALUE, NAME>(this.stream, {
      onNext: options?.onNext,
      onDone: () => {
        this._collection.delete(channel);
        this._detached?.push(channel);
        options?.onDone?.();
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
    if (!this._attached) this._attached = new Stream(`${this.stream.name}ChannelAttached`);
    return this._attached;
  }
  get detached() {
    if (!this._detached) this._detached = new Stream(`${this.stream.name}ChannelDetached`);
    return this._detached;
  }
  get cleared() {
    if (!this._cleared) this._cleared = new Stream(`${this.stream.name}ChannelsCleared`);
    return this._cleared;
  }
  get disposed() {
    if (!this._disposed) this._disposed = new Stream(`${this.stream.name}ChannelsDisposed`);
    return this._disposed;
  }
}

export namespace Channels {
  export type ChannelName<NAME extends string> = `${NAME}Channel${string}`;
}
