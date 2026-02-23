import { Stream } from "../../streams/stream";

const NAME = "consumer";

class Consumer<VALUE, NAME extends string = consumer.Name> extends Stream<VALUE, NAME> {
  protected _running = false;
  protected __source: Stream<VALUE, any>;
  protected __sourceGenerator?: AsyncGenerator<VALUE, void>;
  protected _options: consumer.Options<VALUE> = { autoStart: true };
  protected _events?: Stream<consumer.Event<this>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, options?: consumer.Options<VALUE>) {
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
  set options(options: consumer.Options<VALUE>) {
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
        result = await this.__sourceGenerator.next(new Stream.Error(error, this, result.value));
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
export function consumer<VALUE, NAME extends string = consumer.Name>(): Stream.Transformer<
  NAME,
  Stream<VALUE, any>,
  Consumer<VALUE, NAME>
>;
export function consumer<VALUE, NAME extends string = consumer.Name>(
  callback: consumer.Callback<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Consumer<VALUE, NAME>>;
export function consumer<VALUE, NAME extends string = consumer.Name>(
  options: consumer.Options<VALUE>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Consumer<VALUE, NAME>>;
export function consumer<VALUE, NAME extends string = consumer.Name>(
  callback: consumer.Callback<VALUE>,
  options: Omit<consumer.Options<VALUE>, "callback">,
): Stream.Transformer<NAME, Stream<VALUE, any>, Consumer<VALUE, NAME>>;
export function consumer<VALUE, NAME extends string = consumer.Name>(
  callbackOrOptions?: consumer.Callback<VALUE> | consumer.Options<VALUE>,
  options?: Omit<consumer.Options<VALUE>, "callback">,
): Stream.Transformer<NAME, Stream<VALUE, any>, Consumer<VALUE, NAME>> {
  const _options =
    typeof callbackOrOptions === "function" ? { ...options, callback: callbackOrOptions } : { ...callbackOrOptions };
  return (_, source, name) => new Consumer(source, name, _options);
}

export namespace consumer {
  export type Name = typeof NAME;
  export type Callback<VALUE> = (value: VALUE, stop: () => void) => Stream.MaybeError | Promise<Stream.MaybeError>;
  export type Options<VALUE> = {
    callback?: Callback<VALUE>;
    autoStart?: boolean;
    stopSignal?: Stream<any, any>;
    startSignal?: Stream<any, any>;
    stopSignalActivated?: boolean;
    startSignalActivated?: boolean;
  };

  export type Event<CONSUMER extends Consumer<any, any>> =
    | { type: "start"; self: CONSUMER }
    | { type: "stop"; self: CONSUMER }
    | { type: "options-changed"; self: CONSUMER };
}
