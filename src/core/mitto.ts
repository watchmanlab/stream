export class Mitto<VALUE> {
  protected _listeners: Mitto.Subscription<VALUE>[] = [];
  protected _cleared?: Mitto<void>;
  constructor(source?: Mitto<VALUE>) {
    if (source) {
      source.listen((value) => this.emit(value));
      source.cleared.listen(() => this.clear());
    }
  }

  protected _swapEmit() {
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

  static once<VALUE>(source: Mitto<VALUE>, listener: Mitto.Listener<VALUE>): Mitto.Abort {
    const abort = source.listen((value) => {
      listener(value);
      abort();
    });
    return abort;
  }
  emit(value: VALUE) {}

  listen(listener: Mitto.Listener<VALUE>): Mitto.Abort {
    const sub: Mitto.Subscription<VALUE> = {
      listener,
      index: this._listeners.length,
    };

    this._listeners.push(sub);
    this._swapEmit();

    return () => {
      const idx = sub.index;
      if (idx === -1) return;

      const last = this._listeners.pop()!;

      if (idx < this._listeners.length) {
        this._listeners[idx] = last;
        last.index = idx;
      }

      sub.index = -1;
      this._swapEmit();
    };
  }
  clear() {
    this._listeners.length = 0;
    this._cleared?.emit();
    this._cleared?.clear();
    this._cleared = undefined;
  }
  [Symbol.dispose]() {
    this.clear();
  }
  get cleared() {
    if (!this._cleared) this._cleared = new Mitto();
    return this._cleared;
  }
}

export namespace Mitto {
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => void;
  export type Subscription<VALUE> = { listener: Listener<VALUE>; index: number };
}

function test() {
  const MAX = 775_000_000;

  const start = performance.now();

  const mitto = new Mitto<number>();

  mitto.listen((value) => {
    if (value === MAX) console.log("L1", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
  });

  for (let i = 0; i <= MAX; i++) {
    mitto.emit(i);
  }
}

test(); // 1000000000 1292
