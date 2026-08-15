import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Consumable } from "../core/types";

export class FromGCToken extends Source<void> {
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

export function fromGCToken(token: object): FromGCToken {
  return new FromGCToken(token);
}
