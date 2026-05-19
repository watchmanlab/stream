import { Channel, Stream, Transformer } from "../core/index.ts";
import { each } from "./each.ts";
import { tap } from "./tap.ts";
import { filter } from "./filter.ts";
import { flatMap } from "./flat-map.ts";
import { flat } from "./flat.ts";
import { map } from "./map.ts";

const NAME = "pump";

class Pump<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = pump.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  private _channel?: Channel<Stream.Batch<VALUE>>;
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
    signal?.channels.get({
      onNext: () => {
        if (signal === this._options.startSignal) this.start();
      },
    });
  }
  private stopOnSignal() {
    const signal = this._options.stopSignal;
    signal?.channels.get({
      onNext: () => {
        if (signal === this._options.stopSignal) this.stop();
      },
    });
  }
  start() {
    if (this._channel) return;

    this._started?.push();

    this.stopOnSignal();

    this._channel = this.inputStream.channels.get({
      onNext: (batch) => {
        this.batch(batch);
        this._channel?.next();
      },
    });
    this._channel?.next();
  }

  stop() {
    if (!this._channel) return;

    this._channel?.return();
    this._channel = undefined;

    this._stoped?.push();
    this.startOnSignal();
  }
  override dispose(): void {
    this.stop();
    this._options.startSignal?.dispose();
    this._options.stopSignal?.dispose();
    this._started?.dispose();
    this._stoped?.dispose();
    this._optionsChanged?.dispose();

    this._options.startSignal =
      this._options.stopSignal =
      this._started =
      this._stoped =
      this._optionsChanged =
        undefined;

    super.dispose();
  }
  get isPumping() {
    return this._channel !== undefined;
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

function bench() {
  const MAX = 1_000_000;
  const start = performance.now();

  const stream = new Stream<number>();
  const mapped = stream
    .pipe(
      flatMap({ count: 0, v: 0 }, (v, ctx) => {
        ctx.count++;
        ctx.v = v;
        if (v % 2 !== 0) return [];
        return v;
      }),
    )
    .pipe(
      each((value) => {
        if (value === MAX) {
          console.log("each ", value, Math.round(performance.now() - start));
        }
      }),
    )
    .pipe(pump());

  for (let i = 1; i <= MAX; i++) {
    stream.push(i);
  }
}

bench(); //40ms

function test() {
  new Stream([[1, 2, 3, 4]])
    .pipe(flat(-1))
    .pipe(each((v) => console.log(v)))
    //.         ^?
    .pipe(pump())
    .traversal.each.flat.disposed.pipe(each(console.log))
    .pipe(pump());
}

test();
