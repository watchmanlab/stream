import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, TerminateReason, Transform } from "../core/types";

export class Pump<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$pump",
> extends Transformer<INPUT, VALUE, NAME> {
  declare protected _options: Pump.Options<INPUT, VALUE, NAME>;
  declare protected _metaStreams: Pump.MetaStreams<VALUE, NAME>;
  private _inputConsumer?: Consumer<VALUE>;
  constructor(input: INPUT, options?: Pump.Options<INPUT, VALUE, NAME>) {
    const { name, terminate, ...rest } = options ?? {};

    const consumers = [
      options?.$start?.consume((self) => (this.start(), self.next())).next(),
      options?.$stop?.consume((self, reason) => (this.stop(reason), self.next())).next(),
    ];
    super(input, {
      ...rest,
      name: name ?? ("$pump" as NAME),
      terminate: (self, reason) => {
        this.stop(reason);
        this.start = this.stop = () => this;
        consumers.forEach((consumer) => consumer?.terminate(reason));
        terminate?.(self, reason);
      },
    });

    if (options?.autoStart !== false) this.start();
  }

  start(): this {
    if (this._inputConsumer) return this;

    this._inputConsumer = this._input.consume((self, value) => {
      this.push(value);
      self.next();
    });
    this._options.start?.(this);
    this._metaStreams.$start?.push();
    this._inputConsumer.next();
    return this;
  }

  stop(reason: TerminateReason): this {
    if (!this._inputConsumer) return this;

    this._inputConsumer?.terminate(reason);
    this._inputConsumer = undefined;
    this._options.stop?.(this, reason);
    this._metaStreams.$stop?.push(reason);
    return this;
  }

  get $start(): Stream<void, `${NAME}Start`> {
    this._metaStreams.$start ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Start`, source: this._metaStreams.$start });
  }
  get $stop(): Stream<TerminateReason, `${NAME}Stop`> {
    this._metaStreams.$stop ??= new Stream({ $terminate: this.$terminate });
    return new Stream({ name: `${this.name}Stop`, source: this._metaStreams.$stop });
  }
}

export function pump<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$pump",
>(options?: Pump.Options<INPUT, VALUE, NAME>): Transform<INPUT, Pump<INPUT, VALUE, NAME>> {
  return (input) => new Pump(input, options);
}

export namespace Pump {
  export type Options<
    INPUT extends AnyStream,
    VALUE extends ExtractValue<INPUT>,
    NAME extends NonEmptyString,
  > = Stream.Options<VALUE, NAME> & {
    autoStart?: boolean;
    start?: (self: Pump<INPUT, VALUE, NAME>) => void;
    stop?: (self: Pump<INPUT, VALUE, NAME>, reason: TerminateReason) => void;
    $start?: Stream<void, any>;
    $stop?: Stream<TerminateReason, any>;
  };

  export type MetaStreams<VALUE, NAME extends NonEmptyString> = Stream.MetaStreams<VALUE, NAME> & {
    $start?: Stream<void, any>;
    $stop?: Stream<TerminateReason, any>;
  };
}
