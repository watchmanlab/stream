import { Channel } from "./channel0";
import { Transformer } from "./transformer0";

export class Stream<VALUE = void, NAME extends string = Stream.Name> {
  readonly name: NAME;
  private _options: Stream.Options<VALUE, NAME>;
  private _channels: Channel<VALUE>[] = [];
  private _source?: Channel<VALUE>;
  private _pulling = false;
  private _executor: Channel.Executor<VALUE>;
  private _returned?: Stream<void, `${NAME}Returned`>;

  constructor(options?: Stream.Options<VALUE, NAME>) {
    this._options = { ...options };
    this.name = this._options.name ?? ("root" as NAME);
    this._executor = {
      next: (value: VALUE) => this.push(value),
      ready: (value) => {
        this._pulling = false;
        this.push(value);
      },
      return: () => {
        this._pulling = false;
        this.return();
      },
    };

    if (this._options.scope) {
      if (this._options.scope instanceof Stream) {
        this._options.scope.returned.getChannel({ next: () => this.return() });
      } else if (this._options.scope.any) {
        const others = new Set(this._options.scope.any).values().map((other) =>
          other.returned.getChannel({
            next: () => {
              others.forEach((other) => other.return());
              this.return();
            },
          }),
        );
      } else {
        const others = new Set(this._options.scope.all);
        let count = others.size;
        others.forEach((other) => {
          other.returned.getChannel({ next: () => !count-- && this.return() });
        });
      }
    }

    if (this._options.source) {
      this._source = this._options.source.getChannel({
        next: (value) => {
          this._pulling = false;
          this.push(value);
        },
      });
    }
  }

  private _swapPush() {
    switch (this._channels.length) {
      case 0:
        this.push = () => {};
        break;
      case 1:
        const channel = this._channels[0]!;
        this.push = (value: VALUE) => channel.push(value);
        break;
      default:
        this.push = (value: VALUE) => {
          const len = this._channels.length;
          for (let i = 0; i < len; i++) {
            this._channels[i]!.push(value);
          }
        };
    }
  }
  push(value: VALUE): void {}

  getChannel(options: Channel.Options<VALUE>): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      ...options,
      pull: (executor) => {
        if (this._pulling) return;
        this._pulling = true;

        if (this._source) {
          this._source.next();
        }

        options?.pull?.({
          ...executor,
          ready: (value) => {
            this._pulling = false;
            executor.ready(value);
          },
          return: () => {
            this._pulling = false;
            executor.return();
          },
        });
        this._options.pull?.(this._executor);
      },
      return: (self) => {
        const index = this._channels.indexOf(channel);
        if (index !== -1) {
          (this._channels as any)[index] = this._channels[this._channels.length - 1];
          this._channels.pop();
        }
        this._swapPush();
        options?.return?.(self);
      },
    });

    this._channels.push(channel);
    this._swapPush();
    return channel;
  }
  return() {
    while (this._channels.length > 0) {
      this._channels[0]!.return();
    }
    this._returned?.push();
  }
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
    name: OUTPUT_NAME,
    transform: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM;
  pipe<OUTPUT_NAME extends string, OUTPUT_STREAM extends Transformer<this, any, OUTPUT_NAME> | this>(
    nameOrTransform: OUTPUT_NAME | Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
    transform?: Stream.Transform<this, OUTPUT_NAME, OUTPUT_STREAM>,
  ): OUTPUT_STREAM {
    return typeof nameOrTransform === "string" ? transform!(this, nameOrTransform) : nameOrTransform(this);
  }

  get returned() {
    if (!this._returned) this._returned = new Stream({ name: `${this.name}Returned` });
    return this._returned;
  }
  get channels() {
    return this._channels.values();
  }
  get source() {
    return this._options.source;
  }

  static fromIterable<VALUE>(iterable: Iterable<VALUE>) {
    const iterator = iterable[Symbol.iterator]();

    return new Stream<VALUE>({
      pull: (executor) => {
        const next = iterator.next();
        if (next.done) {
          executor.return();
        } else {
          executor.ready(next.value);
        }
      },
    });
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
    | AnyStream
    | { any: [other: AnyStream, ...others: AnyStream[]]; all?: never }
    | { all: [other: AnyStream, ...others: AnyStream[]]; any?: never };

  export type Options<VALUE, NAME extends string> = {
    name?: NAME;
    scope?: Scoop;
    source?: Stream<VALUE, any>;
    pull?: (executor: Channel.Executor<VALUE>) => void;
  };
  export type ExtractValue<T extends AnyStream | Transformer.AnyTransformer> =
    T extends Stream<infer VALUE, any>
      ? VALUE
      : Transformer.ExtractValue<T> extends never
        ? never
        : Transformer.ExtractValue<T>;

  export type ExtractName<T> = T extends { [k in "name"]: any } ? T["name"] : never;

  export type Transform<
    INPUT_STREAM extends AnyStream,
    OUTPUT_NAME extends string,
    OUTPUT_STREAM extends Transformer<INPUT_STREAM, any, OUTPUT_NAME> | INPUT_STREAM,
  > = (inputStream: INPUT_STREAM, name?: OUTPUT_NAME) => OUTPUT_STREAM;
}

function test() {
  const stream = new Stream<number>();
  stream
    .getChannel({
      next: (value, self) => {
        console.log(value);
        self.next();
      },
    })
    .next();

  stream.push(1);
  stream.push(2);
}
// test();

function optimizedBench() {
  const MAX = 70_000_000;
  const start = performance.now();
  const stream = new Stream<number>();

  stream
    .getChannel({
      next: (value, self) => {
        if (value === MAX) console.log(value, "ddd", Math.round(performance.now() - start));
        self.next();
      },
    })
    .next();

  for (let i = 0; i <= MAX; i++) {
    stream.push(i);
  }
}
// optimizedBench();

function fromIterable() {
  const MAX = 1_000_000;
  const array = new Array(MAX);
  for (let i = 0; i <= MAX; i++) {
    array.push(i);
  }

  const start = performance.now();

  const stream = Stream.fromIterable(array);

  stream
    .getChannel({
      next(value, self) {
        if (value === MAX) console.log(value, Math.round(performance.now() - start));
        self.next();
      },
    })
    .next();
}

fromIterable();
