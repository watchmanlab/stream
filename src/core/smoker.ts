export class Smoker<VALUE> implements Disposable {
  private _listeners: Subscription<VALUE>[] = [];
  private _emit = (value: VALUE) => {};
  private _swapEmit() {
    switch (this._listeners.length) {
      case 0:
        this._emit = (value: VALUE) => {};
        break;
      case 1:
        const sub = this._listeners[0]!;
        this._emit = sub.listener;

        break;
      default:
        this._emit = (value: VALUE) => {
          for (let length = this._listeners.length, i = length - 1; i >= 0; i--) {
            this._listeners[i]!.listener(value);
          }
        };
    }
  }
  emit(value: VALUE) {
    this._emit(value);
  }
  listen(listener: Smoker.Listener<VALUE>): Smoker.Abort {
    const sub: Subscription<VALUE> = {
      listener,
      index: this._listeners.length,
    };
    this._listeners.push(sub);
    this._swapEmit();

    return () => {
      const idx = sub.index;
      if (idx === -1) return;

      const last = this._listeners.pop()!;

      if (idx < this._listeners.length) (this._listeners[idx] = last).index = idx;

      sub.index = -1;
      this._swapEmit();
    };
  }
  clear() {
    this._listeners.length = 0;
  }

  get listeners() {
    return this._listeners.length;
  }
  [Symbol.dispose]() {
    this.clear();
  }
}

export namespace Smoker {
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => void;
}
type Subscription<VALUE> = {
  listener: Smoker.Listener<VALUE>;
  index: number;
};
