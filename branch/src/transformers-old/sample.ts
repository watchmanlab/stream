import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class Sample<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = sample.Name,
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(
    name = sample.NAME as NAME,
    input: INPUT,
    public readonly notifier: Mitto.AnyMitto,
  ) {
    let latest: VALUE | Mitto.Empty;

    super(name, input, {
      source: () => {
        const s1 = input.listen((v) => (latest = v));
        const s2 = notifier.listen(() => {
          if (latest !== Mitto.EMPTY) this.emit(latest);
        });
        return () => {
          s1.emit();
          s2.emit();
        };
      },
    });
  }
}

export function sample<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = sample.Name,
>(notifier: Mitto.AnyMitto): Mitto.Transform<INPUT, NAME, Sample<INPUT, VALUE, NAME>> {
  return (input, name) => new Sample(name, input, notifier);
}
export namespace sample {
  export const NAME = "sample";
  export type Name = typeof NAME;
}
