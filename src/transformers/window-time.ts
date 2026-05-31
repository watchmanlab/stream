import { Mitto } from "../mitto";
import { Queue } from "../queue";
import { Transformer } from "../transformer";

export class WindowTime<
  INPUT extends Mitto.AnyMitto,
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = windowTime.Name,
> extends Transformer<INPUT, Mitto<VALUE>, NAME> {
  private _windows = new Queue<{ mitto: Mitto<VALUE>; timer: any }>();
  constructor(
    name = windowTime.NAME as NAME,
    input: INPUT,
    public readonly ms: number,
    public readonly startWindowEvery = ms,
  ) {
    super(name, input, {
      source: () => {
        let creationTimer: any;

        this.createWindow();
        creationTimer = setInterval(this.createWindow.bind(this), startWindowEvery);

        const signal = input.listen((value) => {
          for (const window of this._windows) {
            window.mitto.emit(value);
          }
        });

        return () => {
          signal.emit();
          clearInterval(creationTimer);
          for (const w of this._windows) {
            clearTimeout(w.timer);
          }
        };
      },
      aborted: () => {
        for (const w of this._windows) {
          clearTimeout(w.timer);
          w.mitto.abort();
        }
        this._windows.clear();
      },
    });
  }

  private createWindow() {
    const window = new Mitto<VALUE>();
    const timer = setTimeout(() => {
      window.abort();
      this._windows.dequeue();
    }, this.ms);

    this._windows.enqueue({ mitto: window, timer });
    this.emit(window);
  }
  get windows() {
    return this._windows.values();
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
