import { Channel } from "./channel";

export class Stream<VALUE> {
  private _source?: Channel<VALUE>;
  private _channels = new Set<Channel<VALUE>>();
  private _pulling = false;
  private _status: Channel.Status = "active";
  constructor(source?: Channel<VALUE>) {
    this._source = source;
  }

  push(value: VALUE) {
    for (const channel of this._channels) {
      channel.push(value);
    }
  }
  getChannel(): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      pull: () => {
        if (this._pulling || !this._source) return;

        const next = this._source.next();

        if (next instanceof Promise) {
          this._pulling = true;

          next.then((value) => {
            this._pulling = false;
            this._handleNext(value);
          });
        } else {
          this._handleNext(next);
        }
      },
      done: () => {
        this._channels.delete(channel);
      },
    });

    this._channels.add(channel);
    return channel;
  }

  private _handleNext(next: VALUE | Channel.Done) {
    if (next === Channel.DONE) {
      this._source = undefined;
      this.complete();
    } else {
      this.push(next);
    }
  }
  complete() {
    for (const channel of this._channels) {
      channel.complete();
    }
  }
  abort() {
    for (const channel of this._channels) {
      channel.abort();
    }
  }
  get channels() {
    return this._channels.values();
  }
  get source() {
    return this._source;
  }
}

export namespace Channels {}
