import { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class WindowCount<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = windowCount.Name,
> extends Transformer<INPUT, Mitto<VALUE>, NAME> {
  private _windows = new Queue<{ mitto: Mitto<VALUE>; count: number }>();
  constructor(
    name = windowCount.NAME as NAME,
    input: INPUT,
    public readonly size: number,
    public readonly startWindowEvery = size,
  ) {
    let count = 0;

    super(name, input, {
      source: () => {
        const signal = input.consume((value) => {
          if (count % startWindowEvery === 0) {
            const window = new Mitto<VALUE>();
            this._windows.enqueue({ mitto: window, count: 0 });
            this.emit(window);
          }

          for (const window of this._windows) {
            window.mitto.emit(value);
            window.count++;
            if (window.count >= size) {
              window.mitto.abort();
              this._windows.dequeue();
            }
          }

          count++;
        });

        return () => signal.emit();
      },
      abort: () => {
        for (const w of this._windows) {
          w.mitto.abort();
        }
        this._windows.clear();
        count = 0;
      },
    });
  }

  get windows() {
    return this._windows.values();
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
