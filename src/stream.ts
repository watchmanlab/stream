import { Channel } from "./channel";

export class Stream<VALUE = void, NAME extends string = Stream.Name> {
  readonly name: NAME;
  private _options: Stream.Options<VALUE, NAME>;
  private _channels: Channel<VALUE>[] = [];
  private _pulling = false;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this._options = { ...options };
    this.name = this._options.name ?? ("root" as NAME);

    if (this._options.scoop) {
      if (this._options.scoop instanceof Channel) {
        this._handleScoop(this._options.scoop);
      } else if (this._options.scoop.any) {
        new Set(this._options.scoop.any).forEach((other) => {
          this._handleScoop(other);
        });
      } else {
        const scoops = new Set(this._options.scoop.all);
        let completCount = scoops.size,
          abortCount = scoops.size;

        scoops.forEach((other) => {
          this._handleScoop(
            other,
            () => !completCount-- && this.complete(),
            () => !abortCount-- && this.abort(),
          );
        });
      }
    }
  }
  private _handleScoop(channel: Channel<VALUE>, onComplete?: () => void, onAbort?: () => void) {
    const completedChannel = channel.completed.getChannel();
    try {
      const next = completedChannel.next();
      if (next instanceof Promise) {
        next.then(() => (onComplete ? onComplete() : this.complete())).catch(() => this.abort());
      } else {
        onComplete ? onComplete() : this.complete();
      }
    } catch (done) {
      this.abort();
    }
    const abortedChannel = channel.aborted.getChannel();
    try {
      const next = abortedChannel.next();
      if (next instanceof Promise) {
        next.then(() => (onAbort ? onAbort() : this.abort())).catch(() => this.abort());
      } else {
        onAbort ? onAbort() : this.abort();
      }
    } catch (done) {
      this.abort();
    }
  }

  push(value: VALUE) {
    const len = this._channels.length;
    for (let i = 0; i < len; i++) {
      this._channels[i]!.push(value);
    }
  }
  getChannel(): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      pull: () => {
        if (this._pulling || !this._options.source) return;

        try {
          const next = this._options.source.next();

          if (next instanceof Promise) {
            this._pulling = true;
            next
              .then((value) => {
                this._pulling = false;
                this.push(value);
              })
              .catch((done) => {
                this._handleError(done);
              });
          } else {
            this.push(next);
          }
        } catch (done) {
          this._handleError(done);
        }
      },
      done: () => {
        const index = this._channels.indexOf(channel);
        if (index !== -1) {
          (this._channels as any)[index] = this._channels[this._channels.length - 1];
          this._channels.pop();
        }
      },
    });

    this._channels.push(channel);
    return channel;
  }

  private _handleError(error: any) {
    this._options.source = undefined;
    if (error === Channel.ABORTED) {
      this.abort();
    } else {
      this.complete();
    }
  }
  // pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
  //   transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  // ): OUTPUT_STREAM;
  // pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
  //   name: OUTPUT_NAME,
  //   transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  // ): OUTPUT_STREAM;
  // pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
  //   nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  //   transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  // ): OUTPUT_STREAM {
  //   return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  // }
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
    return this._options.source;
  }
}

export namespace Stream {
  export const NAME = "root";
  export type Name = typeof NAME;
  export type AnyStream = Stream<any, any>;
  export type Scoop =
    | Channel.AnyChannel
    | { any: [other: Channel.AnyChannel, ...others: Channel.AnyChannel[]]; all?: never }
    | { all: [other: Channel.AnyChannel, ...others: Channel.AnyChannel[]]; any?: never };

  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    scoop?: Scoop;
    source?: Channel<VALUE>;
  };
  // export type ExtractValue<T extends AnyStream | Transformer.AnyTransformer> =
  //   T extends Stream<infer VALUE, any>
  //     ? VALUE
  //     : Transformer.ExtractValue<T> extends never
  //       ? never
  //       : Transformer.ExtractValue<T>;

  // export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

  // export type Transform<
  //   INPUT_STREAM extends AnyStream,
  //   OUTPUT_NAME extends string,
  //   OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, OUTPUT_NAME> | INPUT_STREAM,
  // > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;
}

function bench() {
  const MAX = 18_000_000;
  const start = performance.now();

  const stream = new Stream<number>();
  const channel = stream.getChannel();

  (async () => {
    try {
      let next = channel.next();
      next = next instanceof Promise ? await next : next;

      while (true) {
        if (next === MAX) console.log(next, Math.round(performance.now() - start));
        next = channel.next();
        next = next instanceof Promise ? await next : next;
      }
    } catch (error) {}
  })();

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}

// bench(); //948ms

function test() {
  const stream = new Stream<number>();
  const channel = stream.getChannel();

  (async () => {
    try {
      let next = channel.next();
      next = next instanceof Promise ? await next : next;

      while (true) {
        console.log(next);

        next = channel.next();
        next = next instanceof Promise ? await next : next;
      }
    } catch (error) {}
  })();

  stream.push(1);
  stream.push(2);
}

// test();

function optimizedBench() {
  const MAX = 18_000_000;
  const stream = new Stream<number>();
  const channel = stream.getChannel();

  // 1. Flood the stream with data first
  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }

  // 2. Start the timer right before consumption starts
  const start = performance.now();

  (async () => {
    try {
      let next = channel.next();
      next = next instanceof Promise ? await next : next;

      while (true) {
        if (next === MAX) {
          // This will print an even lower, purely synchronous runtime score
          console.log("Synchronous Processing Done:", next, Math.round(performance.now() - start), "ms");
        }
        next = channel.next();
        next = next instanceof Promise ? await next : next;
      }
    } catch (error) {}
  })();
}

// optimizedBench();

function multiConsumerBench() {
  const MAX = 30_000_000;
  const stream = new Stream<number>();

  // 1. Create multiple independent consumer channels
  const channelA = stream.getChannel();
  const channelB = stream.getChannel();
  const channelC = stream.getChannel();

  // 2. Flood the stream (fires your internal for-of/for-each dispatch loop)
  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }

  // 3. Start the timer for the consumer drainage phase
  const start = performance.now();
  let completedConsumers = 0;

  function consume(channel: any, name: string) {
    (async () => {
      try {
        let next = channel.next();
        next = next instanceof Promise ? await next : next;

        while (true) {
          if (next === MAX) {
            completedConsumers++;
            if (completedConsumers === 3) {
              console.log("All 3 Consumers Done!", Math.round(performance.now() - start), "ms");
            }
          }
          next = channel.next();
          next = next instanceof Promise ? await next : next;
        }
      } catch (error) {}
    })();
  }

  // 4. Drain all channels concurrently
  consume(channelA, "A");
  consume(channelB, "B");
  consume(channelC, "C");
}

// multiConsumerBench();
