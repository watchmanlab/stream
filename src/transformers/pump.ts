import { Consumer, Stream, Transformer } from "../core/index.ts";

const NAME = "pump";

class Pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
> extends Transformer<INPUT_STREAM, VALUE, never, NAME> {
  private _consumer?: Consumer<VALUE, any>;
  private _options: pump.Options;
  private _started?: Stream<void, never, `${NAME}Started`>;
  private _stoped?: Stream<void, never, `${NAME}Stoped`>;
  private _optionsChanged?: Stream<this, never, `${NAME}OptionsChanged`>;

  constructor(name: NAME, inputStream: INPUT_STREAM, options?: pump.Options) {
    super(name, inputStream);

    this._options = { ...pump.defaultOptions, ...options };

    if (this._options.autoStart) this.start();

    this._options.startSignal?.next().then(() => this.start());
  }

  private startOnSignal(signal?: Stream.AnyStream) {
    signal?.next().then(() => {
      if (signal === this._options.startSignal) this.start();
    });
  }
  private stopOnSignal(signal?: Stream.AnyStream) {
    signal?.next().then(() => {
      if (signal === this._options.startSignal) this.stop();
    });
  }
  start() {
    if (this._consumer) return;

    this._started?.push();

    this.stopOnSignal(this._options.stopSignal);

    (async () => {
      this._consumer = this.inputStream.getConsumer();
      for await (const batch of this._consumer) {
        this.batch(batch);
      }
    })();
  }

  //TODO: i think sourceData may return only values and expose error stream for transformers to push to it
  async stop() {
    if (!this._consumer) return;

    this.startOnSignal(this._options.stopSignal);

    this._stoped?.push();
    await this._consumer?.dispose();
    this._consumer = undefined;
  }
  override async dispose(): Promise<void> {
    await Promise.all([
      super.dispose(),
      this._consumer?.dispose(),
      this._options.startSignal?.dispose(),
      this._options.stopSignal?.dispose(),
      this._started?.dispose(),
      this._stoped?.dispose(),
      this._optionsChanged?.dispose(),
    ]);
    this._consumer =
      this._options.startSignal =
      this._options.stopSignal =
      this._started =
      this._stoped =
      this._optionsChanged =
        undefined;
  }
  get pumping() {
    return this._consumer !== undefined;
  }
  get options() {
    return { ...this._options };
  }
  set options(options: pump.Options) {
    this._options = { ...this._options, ...options };
    this._optionsChanged?.push(this);
  }
  get started() {
    if (!this._started) this._started = new Stream(`${this.name}Started`);
    return this._started;
  }
  get stoped() {
    if (!this._stoped) this._stoped = new Stream(`${this.name}Stoped`);
    return this._stoped;
  }
  get optionsChanged() {
    if (!this._optionsChanged) this._optionsChanged = new Stream(`${this.name}OptionsChanged`);
    return this._optionsChanged;
  }
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
