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
  protected _options: pump.Options = { autoStart: true, startSignalActivated: true, stopSignalActivated: true };
  protected _startEvents?: Stream<Stream.Traversable<this, INPUT_STREAM>, `${NAME}StartEvents`>;
  protected _stopEvents?: Stream<Stream.Traversable<this, INPUT_STREAM>, `${NAME}StopEvents`>;
  protected _optionsChangeEvents?: Stream<Stream.Traversable<this, INPUT_STREAM>, `${NAME}OptionsChangeEvents`>;
  protected _traversable: Stream.Traversable<this, INPUT_STREAM>;
  constructor(name: NAME, inputStream: INPUT_STREAM, options?: pump.Options) {
    super(name, inputStream);

    this._traversable = Stream.traversable(this, inputStream);

    this.__source = inputStream;
    this._options = { ...this._options, ...options };

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
    this._options = { ...this._options, ...options };
    this._optionsChangeEvents?.push(this._traversable);
  }
  get startEvents() {
    if (!this._startEvents) this._startEvents = new Stream(`${this._name}StartEvents` as never);
    return this._startEvents;
  }
  get stopEvents() {
    if (!this._stopEvents) this._stopEvents = new Stream(`${this._name}StopEvents` as never);
    return this._stopEvents;
  }
  get optionsChangeEvents() {
    if (!this._optionsChangeEvents) this._optionsChangeEvents = new Stream(`${this._name}OptionsChangeEvents` as never);
    return this._optionsChangeEvents;
  }

  async start() {
    if (this._pumping) return;
    this._pumping = true;
    this._startEvents?.push(this._traversable);

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
    this._stopEvents?.push(this._traversable);
    await this.__sourceGenerator?.return();
    this.__sourceGenerator = undefined;
  }
}

export function pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
>(): Stream.Transform<INPUT_STREAM, Stream.Traversable<Pump<INPUT_STREAM, VALUE, pump.Name>, INPUT_STREAM>>;

export function pump<INPUT_STREAM extends Stream.AnyStream, VALUE = Stream.ExtractValue<INPUT_STREAM>>(
  options: pump.Options,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Pump<INPUT_STREAM, VALUE, pump.Name>, INPUT_STREAM>>;

export function pump<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
>(name: NAME): Stream.Transform<INPUT_STREAM, Stream.Traversable<Pump<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>>;

export function pump<
  NAME extends string,
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
>(
  name: NAME,
  options: pump.Options,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Pump<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>>;

export function pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
>(
  nameOrOptions?: NAME | pump.Options,
  options?: pump.Options,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<Pump<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>> {
  return (inputStream) =>
    Stream.traversable(
      typeof nameOrOptions === "string"
        ? new Pump(nameOrOptions, inputStream, options)
        : new Pump(NAME as NAME, inputStream, nameOrOptions),
      inputStream,
    );
}

export namespace pump {
  export type Name = typeof NAME;

  export type Options = {
    autoStart: boolean;
    stopSignal?: Stream.AnyStream;
    startSignal?: Stream.AnyStream;
    stopSignalActivated: boolean;
    startSignalActivated: boolean;
  };
}
