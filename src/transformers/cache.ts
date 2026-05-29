import type { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class Cache<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = cache.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  private _buffer = new Queue<VALUE>();
  constructor({ name, input, drop, size, ttl }: cache.Options<INPUT, NAME>) {
    let signal = input.listen((value) => this._buffer.enqueue(value));
    super({
      name: name ?? (cache.NAME as NAME),
      input,
      source: () => {
        while (this._buffer.size) {
          const value = this._buffer.dequeue();
          if (value === Queue.EMPTY) break;
          this.emit(value);
        }
        signal.emit();
        signal = input.listen((value) => this.emit(value));
        return () => {
          signal.emit();
          signal = input.listen((value) => this._buffer.enqueue(value));
        };
      },
      aborted: () => {
        signal.emit();
        this._buffer.clear();
      },
    });
  }
}

export namespace cache {
  export const NAME = "cache";
  export type Name = typeof NAME;
  export type Options<INPUT extends Mitto.AnyMitto, NAME extends string> = {
    name?: NAME;
    input: INPUT;
    size?: number;
    drop?: "newest" | "older";
    ttl?: number;
  };
}
