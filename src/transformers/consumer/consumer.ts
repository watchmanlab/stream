import { Stream } from "../../streams/stream";

const NAME = "consumer";

class Consumer<VALUE, NAME extends string = consumer.Name, ERROR = unknown> extends Stream<VALUE, NAME, ERROR> {
  protected _running = false;
  protected __source: Stream<VALUE, any, any>;
  protected __sourceGenerator?: AsyncGenerator<VALUE, void>;
  protected _options: consumer.Options<VALUE, ERROR> = { autoStart: true };
  protected _events?: Stream<consumer.Event<this>, `${NAME}-events`, never>;
  constructor(source: Stream<VALUE, any, any>, name = NAME as NAME, options?: consumer.Options<VALUE, ERROR>) {
    super(name, source);

    this.__source = source;
    this.options = options ?? {};

    if (this._options.autoStart) {
      this.start();
    } else if (this._options.startSignalActivated) {
      this._options.startSignal?.next().then(() => this.start());
    }
  }
  get options() {
    return { ...this._options };
  }
  set options(options: consumer.Options<VALUE, ERROR>) {
    this._options = { autoStart: true, ...options };
    this._events?.push({ type: "options-changed", self: this });
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
  async start() {
    if (this._running) return;
    this._running = true;
    this._events?.push({ type: "start", self: this });

    if (this._options.stopSignalActivated) this._options.stopSignal?.next().then(() => this.stop());

    this.__sourceGenerator = this.__source[Symbol.asyncIterator]();

    let result = await this.__sourceGenerator.next();

    while (!result.done && this._running) {
      try {
        const maybeError = await this?._options.callback?.(result.value, this.stop.bind(this));

        result = await this.__sourceGenerator.next(maybeError);
      } catch (error) {
        result = await this.__sourceGenerator.next(new Stream.Error(error));
      }
    }
  }
  async stop() {
    if (!this._running) return;
    if (this._options.startSignalActivated) this._options.startSignal?.next().then(() => this.start());
    this._running = false;
    this._events?.push({ type: "stop", self: this });
    await this.__sourceGenerator?.return();
    this.__sourceGenerator = undefined;
  }
}
export function consumer<VALUE, NAME extends string = consumer.Name, ERROR = unknown>(): Stream.Transformer<
  NAME,
  Stream<VALUE, any, any>,
  Consumer<VALUE, NAME, ERROR>
>;
export function consumer<VALUE, NAME extends string = consumer.Name, ERROR = unknown>(
  callback: consumer.Callback<VALUE, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any, any>, Consumer<VALUE, NAME, ERROR>>;
export function consumer<VALUE, NAME extends string = consumer.Name, ERROR = unknown>(
  options: consumer.Options<VALUE, ERROR>,
): Stream.Transformer<NAME, Stream<VALUE, any, any>, Consumer<VALUE, NAME, ERROR>>;
export function consumer<VALUE, NAME extends string = consumer.Name, ERROR = unknown>(
  callback: consumer.Callback<VALUE, ERROR>,
  options: Omit<consumer.Options<VALUE, ERROR>, "callback">,
): Stream.Transformer<NAME, Stream<VALUE, any, any>, Consumer<VALUE, NAME, ERROR>>;
export function consumer<VALUE, NAME extends string = consumer.Name, ERROR = unknown>(
  callbackOrOptions?: consumer.Callback<VALUE, ERROR> | consumer.Options<VALUE, ERROR>,
  options?: Omit<consumer.Options<VALUE, ERROR>, "callback">,
): Stream.Transformer<NAME, Stream<VALUE, any, any>, Consumer<VALUE, NAME, ERROR>> {
  const _options =
    typeof callbackOrOptions === "function" ? { ...options, callback: callbackOrOptions } : { ...callbackOrOptions };
  return (_, source, name) => new Consumer(source, name, _options);
}

export namespace consumer {
  export type Name = typeof NAME;
  export type Callback<VALUE, ERROR> = (
    value: VALUE,
    stop: () => void,
  ) => Stream.MaybeError<ERROR> | Promise<Stream.MaybeError<ERROR>>;
  export type Options<VALUE, ERROR> = {
    callback?: Callback<VALUE, ERROR>;
    autoStart?: boolean;
    stopSignal?: Stream<any, any, any>;
    startSignal?: Stream<any, any, any>;
    stopSignalActivated?: boolean;
    startSignalActivated?: boolean;
  };

  export type Event<CONSUMER extends Consumer<any, any, any>> =
    | { type: "start"; self: CONSUMER }
    | { type: "stop"; self: CONSUMER }
    | { type: "options-changed"; self: CONSUMER };
}
