import { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class WindowCount<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = windowCount.Name,
> extends Transformer<INPUT, Mitto<VALUE>, NAME> {
  constructor(name = windowCount.NAME as NAME, input: INPUT, size: number, startWindowEvery = size) {
    const windows = new Queue<{ mitto: Mitto<VALUE>; count: number }>();
    let count = 0;

    super(name, input, {
      source: () => {
        const signal = input.listen((value) => {
          if (count % startWindowEvery === 0) {
            const window = new Mitto<VALUE>();
            windows.enqueue({ mitto: window, count: 0 });
            this.emit(window);
          }

          for (const window of windows) {
            window.mitto.emit(value);
            window.count++;
            if (window.count >= size) {
              window.mitto.abort();
              windows.dequeue();
            }
          }

          count++;
        });

        return () => signal.emit();
      },
      aborted: () => {
        for (const w of windows) {
          w.mitto.abort();
        }
        windows.clear();
        count = 0;
      },
    });
  }
}

export function windowCount<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = windowCount.Name,
>(size: number, startWindowEvery = size): Mitto.Transform<INPUT, NAME, WindowCount<INPUT, VALUE, NAME>> {
  return (input, name) => new WindowCount(name, input, size, startWindowEvery);
}
export namespace windowCount {
  export const NAME = "windowCount";
  export type Name = typeof NAME;
}
