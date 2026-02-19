import { Stream } from "../../stream";

const NAME = "consumer";
type Name = typeof NAME;
class Consumer<VALUE, NAME extends string = Name> extends Stream<VALUE, NAME> {
  protected _running = true;
  protected _source: Stream<VALUE, any>;
  protected _sourceGenerator?: AsyncGenerator<VALUE, void>;
  protected _options: consumer.Options<VALUE, NAME> = { autoStart: true };
  protected _events?: Stream<consumer.Event<VALUE, NAME>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, options?: consumer.Options<VALUE, NAME>) {
    super(name, source);
    this._source = source;
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
  set options(options: consumer.Options<VALUE, NAME>) {
    this._options = { autoStart: true, ...options };
    this._events?.push({ type: "options-changed", this: this });
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
  async start() {
    if (this._running) return;
    this._running = true;
    this._events?.push({ type: "start", this: this });

    if (this._options.stopSignalActivated) this._options.stopSignal?.next().then(() => this.stop());

    this._sourceGenerator = this._source[Symbol.asyncIterator]();

    for await (const value of this._sourceGenerator) {
      if (!this._running) break;
      this?._options.callback?.(value, this.stop.bind(this));
    }
  }
  async stop() {
    if (!this._running) return;
    if (this._options.startSignalActivated) this._options.startSignal?.next().then(() => this.start());
    this._running = false;
    this._events?.push({ type: "stop", this: this });
    await this._sourceGenerator?.return();
    this._sourceGenerator = undefined;
  }
}

export function consumer<VALUE, NAME extends string = Name>(
  options?: consumer.Options<VALUE, NAME>,
): Stream.Transformer<NAME, Stream<VALUE, any>, Consumer<VALUE, NAME>> {
  return (_, source, name) => new Consumer(source, name, options);
}

export namespace consumer {
  export type Callback<VALUE> = (value: VALUE, stop: () => void) => void;
  export type Options<VALUE, NAME extends string> = {
    callback?: Callback<VALUE>;
    autoStart?: boolean;
    stopSignal?: Stream<any, `${NAME}-stop-signal`>;
    startSignal?: Stream<any, `${NAME}-start-signal`>;
    stopSignalActivated?: boolean;
    startSignalActivated?: boolean;
  };

  export type Event<VALUE, NAME extends string> =
    | { type: "start"; this: Stream<VALUE, NAME> }
    | { type: "stop"; this: Stream<VALUE, NAME> }
    | { type: "options-changed"; this: Stream<VALUE, NAME> };
}
