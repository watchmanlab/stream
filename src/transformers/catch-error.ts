import type { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class CatchError<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = catchError.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(name = catchError.NAME as NAME, input: INPUT, handler: catchError.Handler<VALUE>) {
    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
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

export function catchError<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = catchError.Name,
>(handler: catchError.Handler<VALUE>): Mitto.Transform<INPUT, NAME, CatchError<INPUT, VALUE, NAME>> {
  return (input, name) => new CatchError(name, input, handler);
}
export namespace catchError {
  export const NAME = "catchError";
  export type Name = typeof NAME;
  export type Handler<VALUE> = (error: any) => VALUE;
}
