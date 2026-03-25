import { Stream } from "../../streams/stream";

const NAME = "pump";

class Pump<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
> extends Stream<VALUE, NAME> {
  protected _pumping = false;
  protected __source: Stream<VALUE, any>;
  protected __sourceGenerator?: AsyncGenerator<VALUE, void>;
  protected _options: pump.Options = { autoStart: true };
  protected _events?: Stream<
    pump.Event<Stream.Transformer<Pump<INPUT_STREAM, INPUT_NAME, VALUE, NAME>, INPUT_NAME, INPUT_STREAM>>,
    `${NAME}-events`
  >;
  constructor(options: Stream.TransformOptions<INPUT_STREAM, NAME> & { options?: pump.Options }) {
    super(options.name ?? (NAME as NAME), options.inputStream);

    this.__source = options.inputStream;
    this.options = options.options ?? {};

    if (this._options.autoStart) {
      this.start();
    } else if (this._options.startSignalActivated) {
      this._options.startSignal?.next().then(() => this.start());
    }
  }
  get pumping() {
    return this._pumping;
  }
  get options() {
    return { ...this._options };
  }
  set options(options: pump.Options) {
    this._options = { autoStart: true, ...options };
    this._events?.push({ type: "options-changed", self: this as never });
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
  async start() {
    if (this._pumping) return;
    this._pumping = true;
    this._events?.push({ type: "start", self: this as never });

    if (this._options.stopSignalActivated) this._options.stopSignal?.next().then(() => this.stop());

    this.__sourceGenerator = this.__source[Symbol.asyncIterator]();

    let result = await this.__sourceGenerator.next();

    while (!result.done && this._pumping) {
      result = await this.__sourceGenerator.next();
    }
  }
  async stop() {
    if (!this._pumping) return;
    if (this._options.startSignalActivated) this._options.startSignal?.next().then(() => this.start());
    this._pumping = false;
    this._events?.push({ type: "stop", self: this as never });
    await this.__sourceGenerator?.return();
    this.__sourceGenerator = undefined;
  }
}
export function pump<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
>(): Stream.Transform<INPUT_STREAM, NAME, Pump<INPUT_STREAM, INPUT_NAME, VALUE, NAME>>;

export function pump<
  INPUT_STREAM extends Stream<any, any>,
  INPUT_NAME extends string = Stream.ExtractName<INPUT_STREAM>,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
>(options?: pump.Options): Stream.Transform<INPUT_STREAM, NAME, Pump<INPUT_STREAM, INPUT_NAME, VALUE, NAME>> {
  return (opts) => new Pump({ ...opts, options });
}

export namespace pump {
  export type Name = typeof NAME;

  export type Options = {
    autoStart?: boolean;
    stopSignal?: Stream<any, any>;
    startSignal?: Stream<any, any>;
    stopSignalActivated?: boolean;
    startSignalActivated?: boolean;
  };

  export type Event<SELF extends Stream<any, any>> =
    | { type: "start"; self: SELF }
    | { type: "stop"; self: SELF }
    | { type: "options-changed"; self: SELF };
}
