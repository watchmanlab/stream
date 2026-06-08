export class Mitto<VALUE> implements Disposable {
  private _listeners: Mitto.Subscription<VALUE>[] = [];

  private _swapEmit() {
    switch (this._listeners.length) {
      case 0:
        this.emit = () => {};
        break;
      case 1:
        const sub = this._listeners[0]!;
        this.emit = (value: VALUE) => sub.cb(value);
        break;
      default:
        this.emit = (value: VALUE) => {
          for (let i = 0, length = this._listeners.length; i < length; i++) {
            this._listeners[i]!.cb(value);
          }
        };
    }
  }
  emit(value: VALUE) {}

  once(listener: (value: VALUE) => void): Mitto.Abort {
    const abort = this.listen((value) => {
      listener(value);
      abort();
    });
    return abort;
  }
  listen(listener: (value: VALUE) => void): Mitto.Abort {
    // 1. Birth a stable subscription object tracking its own index
    const sub: Mitto.Subscription<VALUE> = {
      cb: listener,
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
  }
  [Symbol.dispose]() {
    this.clear();
  }
}

export namespace Mitto {
  export type Abort = () => void;
  export type Subscription<T> = { cb: (value: T) => void; index: number };
}

function test() {
  const MAX = 775_000_000;

  const start = performance.now();

  const mitto = new Mitto<number>();
  mitto.listen((value) => {
    if (value === MAX) console.log(value, Math.round(performance.now() - start), "ms");
  });

  for (let i = 0; i <= MAX; i++) {
    mitto.emit(i);
  }
}

// test(); // 1000000000 1292
