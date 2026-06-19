import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";

export function fromIterable<VALUE, NAME extends string = Stream.Name>(iterable: Iterable<VALUE>, name?: NAME) {
  return new Stream<VALUE, NAME>({
    name,
    source: {
      listen: (init) => {
        const iterator = iterable[Symbol.iterator]();

        const outputConsumer = new Consumer<VALUE, any>({
          ...init,
          ready: (self) => {
            const result = iterator.next();
            if (result.done) {
              self.complete();
            } else {
              self.push(result.value);
            }
            init.ready?.(self);
          },
        });
        return outputConsumer;
      },
    },
  });
}
