import { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class DropNewest<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = dropNewest.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  readonly buffer = new Queue<VALUE>();

  private _dropped?: Mitto<VALUE, `${NAME}Dropped`>;

  constructor(
    name = dropNewest.NAME as NAME,
    input: INPUT,
    public readonly size: number,
  ) {
    let signal: Mitto | undefined;
    super(name ?? (dropNewest.NAME as NAME), input, {
      source: () => {
        this.emitBatch([...this.buffer]);
        this.buffer.clear();
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
        this.buffer.clear();
      },
    });

    signal = this.save();
  }
  private save(): Mitto {
    return this.input.listen((value) => {
      if (this.buffer.size >= this.size) {
        this._dropped?.emit(value);
        return;
      }
      this.buffer.enqueue(value);
    });
  }

  get dropped() {
    if (!this._dropped) this._dropped = new Mitto({ name: `${this.name}Dropped` });
    return this._dropped;
  }
}

export function dropNewest<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = dropNewest.Name,
>(size: number): Mitto.Transform<INPUT, NAME, DropNewest<INPUT, VALUE, NAME>> {
  return (input, name) => new DropNewest(name, input, size);
}

export namespace dropNewest {
  export const NAME = "dropNewest";
  export type Name = typeof NAME;
}
