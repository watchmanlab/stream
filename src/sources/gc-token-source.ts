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
      init: (consumer) => {
        const obj = this.ref.deref();
        if (!obj) {
          consumer.push();
          consumer.terminate("complete");
        } else {
          this.registry = new FinalizationRegistry(() => {
            consumer.push();
            consumer.terminate("complete");
          });

          this.registry.register(obj, undefined, this);
        }

        const cleanup = init?.(consumer);

        return (reason) => {
          cleanup?.(reason);
          this.registry?.unregister(this);
        };
      },
    });
  }
}

/**
 * Creates a `GCTokenSource`.
 * @param token The object to watch for garbage collection.
 */
export function fromGCToken(token: object): GCTokenSource {
  return new GCTokenSource(token);
}
