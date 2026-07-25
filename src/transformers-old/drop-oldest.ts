import { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class DropOldest<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = dropOldest.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  private _buffer = new Queue<VALUE>();

  private _dropped?: Mitto<VALUE, `${NAME}Dropped`>;

  constructor(
    name = dropOldest.NAME as NAME,
    input: INPUT,
    public readonly size: number,
  ) {
    let signal: Mitto | undefined;
    super(name ?? (dropOldest.NAME as NAME), input, {
      source: () => {
        this.emitBatch([...this._buffer]);
        this._buffer.clear();
        signal?.emit();
        signal = input.consume((value) => this.emit(value));
        return () => {
          signal?.emit();
          signal = this.save();
        };
      },
      aborted: () => {
        signal?.emit();
        signal = undefined;
        this._buffer.clear();
      },
    });

    signal = this.save();
  }
  private save(): Mitto {
    return this.input.consume((value) => {
      if (this._buffer.size >= this.size) {
        this._buffer.dequeue();
        this._dropped?.emit(value);
      }
      this._buffer.enqueue(value);
    });
  }
  get dropped(): Mitto<VALUE, `${NAME}Dropped`> {
    if (!this._dropped) this._dropped = new Mitto({ name: `${this.name}Dropped` });
    return this._dropped;
  }
  get buffer(): Queue.Iterator<VALUE> {
    return this._buffer.values();
  }
}

export function dropOldest<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = dropOldest.Name,
>(size: number): Mitto.Transform<INPUT, NAME, DropOldest<INPUT, VALUE, NAME>> {
  return (input, name) => new DropOldest(name, input, size);
}

export namespace dropOldest {
  export const NAME = "dropOldest";
  export type Name = typeof NAME;
}
