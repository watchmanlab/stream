import { Signal } from "./signal";

export class Mitto<VALUE> extends Signal<VALUE> {
  constructor(source?: Signal<VALUE>) {
    super(source);
  }

  protected override _swapEmit() {
    switch (this._listeners.length) {
      case 0:
        this.emit = () => {};
        break;
      case 1:
        const sub = this._listeners[0]!;
        this.emit = (value: VALUE) => sub.listener(value);
        break;
      default:
        this.emit = (value: VALUE) => {
          for (let i = 0, length = this._listeners.length; i < length; i++) {
            this._listeners[i]!.listener(value);
          }
        };
    }
  }

  once(listener: Signal.Listener<VALUE>): Signal.Abort {
    const abort = this.listen((value) => {
      listener(value);
      abort();
    });
    return abort;
  }
}

export namespace Mitto {}

function test() {
  const MAX = 775_000_000;

  const start = performance.now();

  const mitto = new Mitto<number>();

  mitto.listen((value) => {
    if (value === MAX) console.log("l1", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
  });

  for (let i = 0; i <= MAX; i++) {
    mitto.emit(i);
  }
}

test(); // 1000000000 1292
