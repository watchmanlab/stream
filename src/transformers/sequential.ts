import { Consumer } from "../core/consumer";
import { Stream } from "../core/stream";
import { Transformer } from "../core/transformer";
import { AnyPromise, ExtractValueFromPromise, NonEmptyString } from "../core/types";

export class Sequential<
  INPUT extends Stream<AnyPromise, any>,
  VALUE extends ExtractValueFromPromise<Stream.ExtractValue<INPUT>> = ExtractValueFromPromise<
    Stream.ExtractValue<INPUT>
  >,
  NAME extends NonEmptyString = "sequential",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, options?: Sequential.Options<VALUE, NAME>) {
    super(input, {
      ...options,
      name: options?.name ?? ("sequential" as NAME),
      source: {
        listen: (handler, options) => {
          const inputConsumer = input.listen((self, promise) => {
            promise.then((value) => outputConsumer.push(value)).catch((error) => outputConsumer.next(error));
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

export function sequential<
  INPUT extends Stream<AnyPromise, any>,
  VALUE extends ExtractValueFromPromise<Stream.ExtractValue<INPUT>> = ExtractValueFromPromise<
    Stream.ExtractValue<INPUT>
  >,
  NAME extends NonEmptyString = "sequential",
>(options?: Sequential.Options<VALUE, NAME>): Stream.Transform<INPUT, NAME, Sequential<INPUT, VALUE, NAME>> {
  return (input, name) => new Sequential(input, { ...options, name: name ?? options?.name });
}

export namespace Sequential {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
}
