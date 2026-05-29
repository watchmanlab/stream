import type { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class Cache<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = cache.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  readonly buffer: VALUE[] = [];
  private _options: Required<cache.Options>;

  constructor(name = cache.NAME as NAME, input: INPUT, options?: cache.Options) {
    let signal: Mitto | undefined;
    super(name ?? (cache.NAME as NAME), input, {
      source: () => {
        this.emitBatch(this.buffer);
        this.buffer.length = 0;
        signal?.emit();
        signal = input.listen((value) => this.emit(value));
        return () => {
          signal?.emit();
          signal = this.save();
        };
      },
      aborted: () => {
        signal?.emit();
        signal = undefined;
        this.buffer.length = 0;
      },
    });

    this._options = {
      ...cache.DEFAULT_OPTIONS,
      ...Object.fromEntries(Object.entries(options ?? {}).filter(([key, val]) => val != null)),
    };

    signal = this.save();
  }
  private save(): Mitto {
    return this.input.listen((value) => {
      if (this.buffer.length > this._options.size) {
        //
      }

      this.buffer.push(value);
    });
  }

  override get options() {
    return this._options;
  }
}

export namespace cache {
  export const NAME = "cache";
  export type Name = typeof NAME;
  export type Options = {
    size?: number;
    drop?: "newest" | "older";
    ttl?: number | null;
  };
  export const DEFAULT_OPTIONS: Required<Options> = { size: 1000, drop: "newest", ttl: null };
}
