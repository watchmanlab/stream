import { Stream } from "../../streams/stream";

const NAME = "pump";

class Pump<VALUE, NAME extends string = pump.Name> extends Stream<VALUE, NAME> {
  protected _pumping = false;
  protected __source: Stream<VALUE, any>;
  protected __sourceGenerator?: AsyncGenerator<VALUE, void>;
  protected _options: pump.Options = { autoStart: true };
  protected _events?: Stream<pump.Event<this>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, options?: pump.Options) {
    super(name, source);

    this.__source = source;
    this.options = options ?? {};

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
    this._events?.push({ type: "options-changed", self: this });
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}-events` as never);
    return this._events;
  }
  async start() {
    if (this._pumping) return;
    this._pumping = true;
    this._events?.push({ type: "start", self: this });

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
    this._events?.push({ type: "stop", self: this });
    await this.__sourceGenerator?.return();
    this.__sourceGenerator = undefined;
  }
}
export function pump<VALUE, NAME extends string = pump.Name>(): Stream.Transform<
  NAME,
  Stream<VALUE, any>,
  Pump<VALUE, NAME>
>;

export function pump<VALUE, NAME extends string = pump.Name>(
  options?: pump.Options,
): Stream.Transform<NAME, Stream<VALUE, any>, Pump<VALUE, NAME>> {
  return (_, source, name) => new Pump(source, name, options);
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

  export type Event<CONSUMER extends Pump<any, any>> =
    | { type: "start"; self: CONSUMER }
    | { type: "stop"; self: CONSUMER }
    | { type: "options-changed"; self: CONSUMER };
}
