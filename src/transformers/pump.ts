import { Consumer, Stream, Transformer } from "../core/index.ts";

const NAME = "pump";

class Pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  private _consumer?: Consumer<Stream.Batch<VALUE>>;
  private _options: pump.Options;
  private _started?: Stream<void, `Started`>;
  private _stoped?: Stream<void, `Stoped`>;
  private _optionsChanged?: Stream<{ old: pump.Options; new: pump.Options }, `OptionsChanged`>;
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, options?: pump.Options) {
    super(name, inputStream);

    this._options = { ...pump.defaultOptions, ...options };

    if (this._options.autoStart) {
      this.start();
    } else {
      this.startOnSignal();
    }
  }
  private startOnSignal() {
    const signal = this._options.startSignal;
    signal?.consumers.get({
      next: () => {
        if (signal === this._options.startSignal) this.start();
      },
    });
  }
  private stopOnSignal() {
    const signal = this._options.stopSignal;
    signal?.consumers.get({
      next: () => {
        if (signal === this._options.stopSignal) this.stop();
      },
    });
  }
  start(): void {
    if (this._consumer) return;

    this._started?.push();

    this.stopOnSignal();

    this._consumer = this.inputStream.consumers.get({
      next: (batch, consumer) => {
        this.batch(batch);
        consumer.next();
      },
    });
    this._consumer?.next();
  }

  stop(): void {
    if (!this._consumer) return;

    this._consumer?.return();
    this._consumer = undefined;

    this._stoped?.push();
    this.startOnSignal();
  }

  get isPumping() {
    return this._consumer !== undefined;
  }
  get options() {
    return { ...this._options };
  }
  set options(options: pump.Options) {
    const old = this.options;
    this._options = { ...this._options, ...options };
    if (this.isPumping) {
      if (options.stopSignal) this.stopOnSignal();
    } else {
      if (options.startSignal) this.startOnSignal();
    }
    this._optionsChanged?.push({ old, new: options });
  }
  get started() {
    if (!this._started) this._started = new Stream(`Started`);
    return this._started;
  }
  get stoped() {
    if (!this._stoped) this._stoped = new Stream(`Stoped`);
    return this._stoped;
  }
  get optionsChanged() {
    if (!this._optionsChanged) this._optionsChanged = new Stream(`OptionsChanged`);
    return this._optionsChanged;
  }
}

export function pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
>(options?: pump.Options): Stream.Transform<INPUT_STREAM, NAME, Pump<INPUT_STREAM, VALUE, NAME>> {
  return (inputStream, name) => new Pump(name, inputStream, options);
}

export namespace pump {
  export type Name = typeof NAME;

  export type Options = {
    autoStart?: boolean;
    stopSignal?: Stream.AnyStream;
    startSignal?: Stream.AnyStream;
  };
  export const defaultOptions = { autoStart: true };
}
