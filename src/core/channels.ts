import { Stream } from "./stream.ts";
import { Channel } from "./channel.ts";

export class Channels<VALUE, NAME extends string> implements Iterable<Channel<VALUE, string>> {
  private _collection = new Map<string, Channel<VALUE, any>>();
  private _attached?: Stream<Channel<VALUE, string>, never, `${NAME}ChannelAttached`>;
  private _detached?: Stream<Channel<VALUE, string>, never, `${NAME}ChannelDetached`>;
  private _cleared?: Stream<void, never, `${NAME}ChannelsCleared`>;
  private _disposed?: Stream<void, never, `${NAME}ChannelsDisposed`>;

  constructor(private stream: Stream.AnyStream) {}

  [Symbol.iterator]() {
    return this._collection.values();
  }
  get() {
    let name: Channels.ChannelName<NAME>;

    while (true) {
      name = `${this.stream.name}Channel${globalThis.crypto.getRandomValues(new Uint32Array(1))[0]}`;
      if (!this._collection.has(name)) break;
    }

    const channel = new Channel<VALUE, Channels.ChannelName<NAME>>(name, {
      onNext: () => this.stream.source?.next(),
      onDone: () => {
        this._collection.delete(name);
        this._detached?.push(channel);
      },
    });

    this._collection.set(name, channel);
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
  get array() {
    return [...this];
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
