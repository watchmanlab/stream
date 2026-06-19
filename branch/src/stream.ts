import { Channel } from "./channel";
import { Transformer } from "./transformer";

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
        this._options.scope.terminated.getChannel().handleNext({ next: (reason) => this.terminate(reason) });
      } else if (this._options.scope.any) {
        const others: Channel<Channel.Terminated>[] = [];
        new Set(this._options.scope.any).forEach((other) =>
          others.push(
            other.terminated.getChannel().handleNext({
              next: (reason) => {
                others.forEach((other) => other.terminate(Channel.COMPLETED));
                others.length = 0;
                this.terminate(reason);
              },
            }),
          ),
        );
      } else {
        const others = new Set(this._options.scope.all);
        let count = others.size;
        others.forEach((other) => {
          other.terminated.getChannel().handleNext({ next: (reason) => !count-- && this.terminate(reason) });
        });
      }
    }
  }
  private _push = (value: VALUE) => {};
  private _swapPush() {
    switch (this._channels.length) {
      case 0:
        this._push = (value: VALUE) => {};
        break;
      case 1:
        this._push = (value: VALUE) => this._channels[0]!.push(value);
        break;
      default:
        this._push = (value: VALUE) => {
          for (let length = this._channels.length, i = length - 1; i >= 0; i--) {
            this._channels[i]!.push(value);
          }
        };
    }
  }
  push(value: VALUE) {
    this._push(value);
  }
  getChannel(options?: Channel.Options<Channel<VALUE>>): Channel<VALUE> {
    const channel = new Channel<VALUE>({
      pull: (self) => {
        if (this._pulling) {
          options?.pull?.(self);
          return;
        }

        if (this._options.source) {
          this._pulling = true;

          this._options.source.handleNext({
            next: (value) => {
              this._pulling = false;
              this.push(value);
            },
            terminate: (reason) => {
              this._pulling = false;
              this._options.source = undefined;
              this.terminate(reason);
            },
          });
        }

        options?.pull?.(self);
      },
      terminate: (reason, self) => {
        const index = this._channels.indexOf(channel);
        if (index !== -1) {
          (this._channels as any)[index] = this._channels[this._channels.length - 1];
          this._channels.pop();
          this._swapPush();
        }

        // const idx = (self as any)._streamIndex;
        // if (idx === undefined || idx === -1) return;

        // const lastIndex = this._channels.length - 1;
        // const lastChannel = this._channels.pop()!;

        // if (idx < lastIndex) {
        //   this._channels[idx] = lastChannel;
        //   (lastChannel as any)._streamIndex = idx;
        // }

        // (self as any)._streamIndex = -1;
        // this._swapPush();
        options?.terminate?.(reason, self);
      },
    });

    // (channel as any)._streamIndex = this._channels.length;
    this._channels.push(channel);
    this._swapPush();
    return channel;
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
  terminate(reason: Channel.Aborted | Channel.Completed) {
    for (let i = this._channels.length - 1; i >= 0; i--) {
      this._channels[i]!.terminate(reason);
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
