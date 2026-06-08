export interface Mitto<VALUE> {
  emit(value: VALUE): void;
  listen(listener: mitto.Listener<VALUE>): mitto.Abort;
  clear(): void;
  readonly cleared: Mitto<void>;
}
export function mitto<VALUE>(source?: Mitto<VALUE>): Mitto<VALUE> {
  if (source) {
    source.listen((value) => out.emit(value));
    source.cleared.listen(() => out.clear());
  }

  const listeners: mitto.Subscription<VALUE>[] = [];
  let cleared: Mitto<void> | undefined = undefined;

  const out: Mitto<VALUE> = {
    emit(value) {},
    listen(listener: mitto.Listener<VALUE>): mitto.Abort {
      const sub: mitto.Subscription<VALUE> = {
        listener,
        index: listeners.length,
      };

      listeners.push(sub);
      swapEmit();

      return () => {
        const idx = sub.index;
        if (idx === -1) return;

        const last = listeners.pop()!;

        if (idx < listeners.length) {
          listeners[idx] = last;
          last.index = idx;
        }

        sub.index = -1;
        swapEmit();
      };
    },
    clear() {
      listeners.length = 0;
      cleared?.emit();
      cleared?.clear();
      cleared = undefined;
    },
    get cleared() {
      if (!cleared) cleared = mitto();
      return cleared;
    },
  };

  return out;

  function swapEmit() {
    switch (listeners.length) {
      case 0:
        out.emit = () => {};
        break;
      case 1:
        const sub = listeners[0]!;
        out.emit = (value: VALUE) => sub.listener(value);
        break;
      default:
        out.emit = (value: VALUE) => {
          for (let i = 0, length = listeners.length; i < length; i++) {
            listeners[i]!.listener(value);
          }
        };
    }
  }
}

export namespace mitto {
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => void;
  export type Subscription<VALUE> = { listener: Listener<VALUE>; index: number };
}

function test() {
  const MAX = 775_000_000;

  const start = performance.now();

  const m = mitto<number>();

  m.listen((value) => {
    if (value === MAX) console.log("L1", value.toLocaleString("fr"), Math.round(performance.now() - start), "ms");
  });

  for (let i = 0; i <= MAX; i++) {
    m.emit(i);
  }
}

test(); // 1000000000 1000 ms
