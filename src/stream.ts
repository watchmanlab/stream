import { Channel } from "./channel";

export class Stream<VALUE = void, NAME extends string = Stream.Name> {
  readonly name: NAME;
  private _options: Stream.Options<VALUE, NAME>;
  private _channels: Channel<VALUE>[] = [];
  private _pulling = false;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this._options = { ...options };
    this.name = this._options.name ?? ("root" as NAME);

    if (this._options.scope) {
      if (this._options.scope instanceof Channel) {
        this._options.scope.terminated.getChannel().once({ next: (reason) => this.terminate(reason) });
      } else if (this._options.scope.any) {
        const others = new Set(this._options.scope.any).values().map((other) =>
          other.terminated.getChannel().once({
            next: (reason) => {
              others.forEach((other) => other.terminate(Channel.COMPLETED));
              this.terminate(reason);
            },
          }),
        );
      } else {
        const others = new Set(this._options.scope.all);
        let count = others.size;
        others.forEach((other) => {
          other.terminated.getChannel().once({ next: (reason) => !count-- && this.terminate(reason) });
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
  getChannel(options?: Channel.Options<Channel<VALUE>>): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      pull: (self) => {
        if (this._pulling || !this._options.source) return;

        this._options.source.once({
          next: (value) => {
            this._pulling = false;
            this.push(value);
          },
          terminate: (reason) => {
            this._options.source = undefined;
            this.terminate(reason);
          },
        });
        options?.pull?.(self);
      },
      terminate: (reason, self) => {
        const index = this._channels.indexOf(channel);
        if (index !== -1) {
          (this._channels as any)[index] = this._channels[this._channels.length - 1];
          this._channels.pop();
        }
        options?.terminate?.(reason, self);
      },
    });

    this._channels.push(channel);
    return channel;
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
  terminate(reason: Channel.Aborted | Channel.Completed) {
    for (const channel of this._channels) {
      channel.terminate(reason);
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
  export interface Closable {
    close(drain: boolean): void;
  }
  export type Scoop =
    | Channel.AnyChannel
    | { any: [other: Channel.AnyChannel, ...others: Channel.AnyChannel[]]; all?: never }
    | { all: [other: Channel.AnyChannel, ...others: Channel.AnyChannel[]]; any?: never };

  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    scope?: Scoop;
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
  const MAX = 10_000_000;
  const stream = new Stream<number>();
  const channel = stream.getChannel();

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }

  const start = performance.now();

  (async () => {
    try {
      let next = channel.next();
      next = next instanceof Promise ? await next : next;

      while (true) {
        if (next === MAX) {
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
  const MAX = 20_000_000;
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

  function consume(channel: any) {
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
  consume(channelA);
  consume(channelB);
  consume(channelC);
}

// multiConsumerBench();
