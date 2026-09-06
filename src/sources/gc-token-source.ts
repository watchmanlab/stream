import { Consumer } from "../core/consumer";
import { Source } from "../core/source";

/**
 * Emits once when the given object is garbage collected via `FinalizationRegistry`.
 * If the object is already collected at subscription time, emits immediately.
 *
 * @example
 * let obj: object | null = {};
 * fromGCToken(obj).pipe(listen(() => console.log('collected')));
 * obj = null;
 */
export class GCTokenSource extends Source<void> {
  private ref: WeakRef<object>;
  private registry?: FinalizationRegistry<unknown>;
  constructor(token: object) {
    super();
    this.ref = new WeakRef(token);
  }
  consume(handler: Consumer.Handler<void>, options?: Consumer.Options<void> | undefined): Consumer<void> {
    const { init, ...rest } = options ?? {};
    return new Consumer(handler, {
      ...rest,
      init: (c) => {
        const obj = this.ref.deref();

        if (!obj) {
          c.push();
          c.terminate("complete");
        } else {
          this.registry = new FinalizationRegistry(() => {
            c.push();
            c.terminate("complete");
          });

          this.registry.register(obj, undefined, this);
        }

        const cleanup = init?.(c);

        return (r) => {
          cleanup?.(r);
          this.registry?.unregister(this);
        };
      },
    });
  }
}

/**
 * Emits once when the given object is garbage collected via `FinalizationRegistry`.
 * If the object is already collected at subscription time, emits immediately.
 *
 * @param token The object to watch for garbage collection.
 *
 * @example
 * let obj: object | null = {};
 * fromGCToken(obj).pipe(listen(() => console.log('collected')));
 * obj = null;
 */
export function fromGCToken(token: object): GCTokenSource {
  return new GCTokenSource(token);
}
