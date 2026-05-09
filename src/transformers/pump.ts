import { Consumer, Source, Stream, Transformer } from "../core/index.ts";
import { each } from "./each.ts";
import { effect } from "./effect.ts";
import { map } from "./map.ts";

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
    signal?.next().then(() => {
      if (signal === this._options.startSignal) this.start();
    });
  }
  private stopOnSignal() {
    const signal = this._options.stopSignal;
    signal?.next().then(() => {
      if (signal === this._options.stopSignal) this.stop();
    });
  }
  start() {
    if (this._consumer) return;

    this._started?.push();

    this.stopOnSignal();

    (async () => {
      this._consumer = this.inputStream.getConsumer();
      for await (const batch of this._consumer) {
        this.batch(batch);
      }
    })();
  }

  async stop() {
    if (!this._consumer) return;

    await this._consumer?.dispose();
    this._consumer = undefined;

    this._stoped?.push();
    this.startOnSignal();
  }
  override async dispose(): Promise<void> {
    await Promise.all([
      this.stop(),
      this._options.startSignal?.dispose(),
      this._options.stopSignal?.dispose(),
      this._started?.dispose(),
      this._stoped?.dispose(),
      this._optionsChanged?.dispose(),
    ]);

    this._options.startSignal =
      this._options.stopSignal =
      this._started =
      this._stoped =
      this._optionsChanged =
        undefined;

    await super.dispose();
  }
  get isPumping() {
    return this._consumer !== undefined;
  }
  get options() {
    return { ...this._options };
  }
  set options(options: pump.Options) {
    this._options = { ...this._options, ...options };
    if (this.isPumping) {
      if (options.stopSignal) this.stopOnSignal();
    } else {
      if (options.startSignal) this.startOnSignal();
    }
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

export function pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE = Stream.ExtractValue<INPUT_STREAM>,
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

function bench() {
  const MAX = 1_000_000;
  const start = performance.now();

  const stream = new Stream<number>();
  const mapped = stream
    .pipe(
      map((v) => {
        if (v === 4) return new Source.Error("kechmahaja " + v);
        return { value: v, doubled: v * 2 };
      }),
    )
    .pipe(
      effect((value) => {
        if (value.value === MAX) {
          console.log("each ", value.value, Math.round(performance.now() - start));
        }
      }),
    )
    .pipe(pump());

  mapped.traversal.effect.map.source?.error.pipe(pump());

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
  // setTimeout(() => {
  //   stream.push(0);
  // });
}

bench(); //128ms
