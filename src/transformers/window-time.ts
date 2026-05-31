import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class WindowTime<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = windowTime.Name,
> extends Transformer<INPUT, Mitto<VALUE>, NAME> {
  constructor(name = windowTime.NAME as NAME, input: INPUT, ms: number) {
    let window: Mitto<VALUE> | null = null;
    let timer: any = null;

    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          if (!window) {
            window = new Mitto<VALUE>();
            this.emit(window);

            timer = setTimeout(() => {
              window?.abort();
              window = null;
              timer = null;
            }, ms);
          }

          window.emit(value);
        });

        return () => {
          signal.emit();
          clearTimeout(timer);
          window?.abort();
        };
      },
      aborted: () => {
        clearTimeout(timer);
        window?.abort();
      },
    });
  }
}

export function windowTime<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = windowTime.Name,
>(ms: number): Mitto.Transform<INPUT, NAME, WindowTime<INPUT, VALUE, NAME>> {
  return (input, name) => new WindowTime(name, input, ms);
}
export namespace windowTime {
  export const NAME = "windowTime";
  export type Name = typeof NAME;
}
