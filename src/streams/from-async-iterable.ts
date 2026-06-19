import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";

export function fromAsyncIterable<VALUE, NAME extends string = Stream.Name>(
  iterable: AsyncIterable<VALUE>,
  name?: NAME,
) {
  return new Stream<VALUE, NAME>({
    name,
    source: {
      listen: (init) => {
        const iterator = iterable[Symbol.asyncIterator]();

        const outputConsumer = new Consumer<VALUE, any>({
          ...init,
          ready: (self) => {
            iterator.next().then((result) => {
              if (result.done) {
                self.complete();
              } else {
                self.push(result.value);
              }
            });
            init.ready?.(self);
          },
        });
        return outputConsumer;
      },
    },
  });
}
