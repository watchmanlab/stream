import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyPromise, ExtractValueFromPromise, NonEmptyString } from "../core/types";

export class Concurrent<
  INPUT extends Stream<AnyPromise, any>,
  VALUE extends ExtractValueFromPromise<Stream.ExtractValue<INPUT>> = ExtractValueFromPromise<
    Stream.ExtractValue<INPUT>
  >,
  NAME extends NonEmptyString = "concurrent",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, limit: number, options?: Concurrent.Options<VALUE, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? ("concurrent" as NAME),
      source: {
        listen: (handler, options) => {
          let counter = 0;
          const inputConsumer = input.listen((self, promise) => {
            counter++;
            if (counter < limit) {
              promise
                .then((value) => outputConsumer.push(value))
                .catch((error) => outputConsumer.next(error))
                .finally(() => {
                  counter--;
                  if (counter < limit) self.next();
                });
              self.next();
            }
          });

          const outputConsumer = new Consumer(handler, {
            ...options,
            events: {
              ...options?.events,
              ready(context, value) {
                inputConsumer.next();
                options?.events?.ready?.(context, value);
              },
              abort(context, value) {
                inputConsumer.abort(value);
                options?.events?.abort?.(context, value);
              },
              complete(context, value) {
                inputConsumer.complete();
                options?.events?.complete?.(context, value);
              },
            },
          });

          return outputConsumer;
        },
      },
    });
  }
}

export function concurrent<
  INPUT extends Stream<AnyPromise, any>,
  VALUE extends ExtractValueFromPromise<Stream.ExtractValue<INPUT>> = ExtractValueFromPromise<
    Stream.ExtractValue<INPUT>
  >,
  NAME extends NonEmptyString = "concurrent",
>(
  limit: number,
  options?: Concurrent.Options<VALUE, NAME>,
): Stream.Transform<INPUT, NAME, Concurrent<INPUT, VALUE, NAME>> {
  return (input, name) => new Concurrent(input, limit, { ...options, name: name ?? options?.name });
}

export namespace Concurrent {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
}
