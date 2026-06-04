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
        this._options.scoop.completed.getChannel().handleNext({ onDone: () => this.complete() });
        this._options.scoop.aborted.getChannel().handleNext({ onDone: () => this.abort() });
      } else if (this._options.scoop.any) {
        new Set(this._options.scoop.any).forEach((other) => {
          other.completed.getChannel().handleNext({ onDone: () => this.complete() });
          other.aborted.getChannel().handleNext({ onDone: () => this.abort() });
        });
      } else {
        const scoops = new Set(this._options.scoop.all);
        let completCount = scoops.size,
          abortCount = scoops.size;

        scoops.forEach((other) => {
          other.completed.getChannel().handleNext({ onDone: () => !completCount-- && this.complete() });
          other.aborted.getChannel().handleNext({ onDone: () => !abortCount-- && this.abort() });
        });
      }
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

        const next = this._options.source.next();

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

  private _handleNext(next: VALUE | Channel.Done) {
    if (next === Channel.DONE) {
      this._options.source = undefined;
      this.complete();
    } else {
      this.push(next);
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
    let next = channel.next();
    next = next instanceof Promise ? await next : next;

    while (next !== Channel.DONE) {
      if (next === MAX) console.log(next, Math.round(performance.now() - start));
      next = channel.next();
      next = next instanceof Promise ? await next : next;
    }
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
    let next = channel.next();
    next = next instanceof Promise ? await next : next;

    while (next !== Channel.DONE) {
      console.log(next);

      next = channel.next();
      next = next instanceof Promise ? await next : next;
    }
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
    let next = channel.next();
    next = next instanceof Promise ? await next : next;

    while (next !== Channel.DONE) {
      if (next === MAX) {
        // This will print an even lower, purely synchronous runtime score
        console.log("Synchronous Processing Done:", next, Math.round(performance.now() - start), "ms");
      }
      next = channel.next();
      next = next instanceof Promise ? await next : next;
    }
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
      let next = channel.next();
      next = next instanceof Promise ? await next : next;

      while (next !== Channel.DONE) {
        if (next === MAX) {
          completedConsumers++;
          if (completedConsumers === 3) {
            console.log("All 3 Consumers Done!", Math.round(performance.now() - start), "ms");
          }
        }
        next = channel.next();
        next = next instanceof Promise ? await next : next;
      }
    })();
  }

  // 4. Drain all channels concurrently
  consume(channelA, "A");
  consume(channelB, "B");
  consume(channelC, "C");
}

// multiConsumerBench();
