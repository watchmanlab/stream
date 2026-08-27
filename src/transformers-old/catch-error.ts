import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class CatchError<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = unwrap.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    name = unwrap.NAME as NAME,
    input: INPUT,
    public readonly handler: unwrap.Handler<VALUE>,
  ) {
    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          try {
            this.emit(value);
          } catch (error) {
            this.emit(handler(error));
          }
        });
        return () => signal.emit();
      },
    });
  }
}

export function unwrap<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = unwrap.Name,
>(handler: unwrap.Handler<VALUE>): Mitto.Transform<INPUT, NAME, CatchError<INPUT, VALUE, NAME>> {
  return (input, name) => new CatchError(name, input, handler);
}
export namespace unwrap {
  export const NAME = "unwrap";
  export type Name = typeof NAME;
  export type Handler<VALUE> = (error: any) => VALUE;
}
