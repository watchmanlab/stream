export class Signal<VALUE> implements Disposable {
  protected _listeners: Signal.Subscription<VALUE>[] = [];
  protected _cleared?: Signal<void>;
  constructor(source?: Signal<VALUE>) {
    if (source) {
      source.listen((value) => this.emit(value));
      source.cleared.listen(() => this.clear());
    }
  }
  protected _swapEmit() {
    switch (this._listeners.length) {
      case 0:
        this.emit = () => this.clear();
        break;
      case 1:
        const listener = this._listeners[0]!.listener;
        this.emit = (value: VALUE) => {
          listener(value);
          this.clear();
        };
        break;
      default:
        this.emit = (value: VALUE) => {
          for (let i = 0, length = this._listeners.length; i < length; i++) {
            this._listeners[i]!.listener(value);
          }
          this.clear();
        };
    }
  }
  emit(value: VALUE) {}

  listen(listener: Signal.Listener<VALUE>): Signal.Abort {
    const sub: Signal.Subscription<VALUE> = {
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
    this._cleared?.clear();
    this._cleared = undefined;
  }
  [Symbol.dispose]() {
    this.clear();
  }
  get cleared() {
    if (!this._cleared) this._cleared = new Signal();
    return this._cleared;
  }
}

export namespace Signal {
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => void;
  export type Subscription<VALUE> = { listener: Listener<VALUE>; index: number };
}
