import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyStream, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class Pump<
  INPUT extends AnyStream,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "$pump",
> extends Transformer<INPUT, VALUE, NAME> {
  declare protected _options: Pump.Options<INPUT, VALUE, NAME>;
  private _inputConsumer?: Consumer<VALUE>;
  constructor(input: INPUT, options?: Pump.Options<INPUT, VALUE, NAME>) {
    const consumers = [
      options?.$start?.consume((self) => (this.start(), self.next())).next(),
      options?.$stop?.consume((self, reason) => (this.stop(reason), self.next())).next(),
    ];
    super(input, {
      ...options,
      name: options?.name ?? ("$pump" as NAME),
      terminate: (self, reason) => {
        this.stop(reason);
        consumers.forEach((consumer) => consumer?.terminate(reason));
        options?.terminate?.(self, reason);
      },
    });

    if (options?.autoStart !== false) this.start();
  }

  start() {
    if (this._inputConsumer) return;

    this._inputConsumer = this._input.consume((self, value) => {
      this.push(value);
      self.next();
    });
    this._options.start?.(this);
    this._options.$start?.push();
    this._inputConsumer.next();
  }

  stop(reason: "abort" | "complete") {
    if (!this._inputConsumer) return;

    this._inputConsumer?.terminate(reason);
    this._inputConsumer = undefined;
    this._options.stop?.(this, reason);
    this._options.$stop?.push(reason);
  }

  get $start() {
    this._options.$start ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}Start`, source: this._options.$start });
  }
  get $stop() {
    this._options.$stop ??= new Stream({ scope: [this] });
    return new Stream({ name: `${this.name}Stop`, source: this._options.$stop });
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
    stop?: (self: Pump<INPUT, VALUE, NAME>, reason: "abort" | "complete") => void;
    $start?: Stream<void, any>;
    $stop?: Stream<"abort" | "complete", any>;
  };
}
