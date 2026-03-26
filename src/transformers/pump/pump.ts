import { Stream } from "../../streams/index.ts";

const NAME = "pump";

class Pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
> extends Stream<VALUE, NAME> {
  protected _pumping = false;
  protected __source: Stream<VALUE, any>;
  protected __sourceGenerator?: AsyncGenerator<VALUE, void>;
  protected _options: pump.Options = { autoStart: true };
  protected _events?: Stream<pump.Event, `${NAME}Events`>;
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
    this._events?.push({ type: "options-changed" });
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}Events` as never);
    return this._events;
  }
  async start() {
    if (this._pumping) return;
    this._pumping = true;
    this._events?.push({ type: "start" });

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
    this._events?.push({ type: "stop" });
    await this.__sourceGenerator?.return();
    this.__sourceGenerator = undefined;
  }
}
export function pump<
  INPUT_STREAM extends Stream<any, any>,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
>(): Stream.Transform<INPUT_STREAM, NAME, Pump<INPUT_STREAM, VALUE, NAME>>;

export function pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
>(options?: pump.Options): Stream.Transform<INPUT_STREAM, NAME, Pump<INPUT_STREAM, VALUE, NAME>> {
  return (opts) => new Pump({ ...opts, options });
}

export namespace pump {
  export type Name = typeof NAME;

  export type Options = {
    autoStart?: boolean;
    stopSignal?: Stream.AnyStream;
    startSignal?: Stream.AnyStream;
    stopSignalActivated?: boolean;
    startSignalActivated?: boolean;
  };

  export type Event = { type: "start" } | { type: "stop" } | { type: "options-changed" };
}
