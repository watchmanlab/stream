import { Consumer } from "../core/consumer";
import { Producer } from "../core/stream";
import { Transformer } from "../core/transformer";
import type { AnyProducer, ExtractValue, NonEmptyString, Transform } from "../core/types";

export class CatchError<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "catchError",
> extends Transformer<INPUT, VALUE, NAME> {
  constructor(input: INPUT, handler: Consumer.Handler<any, never, any>, options?: CatchError.Options<VALUE, NAME>) {
    let upstream = input;
    const consumers: Consumer.AnyConsumer[] = [];
    do {
      //
    } while ((upstream as any)["$"]);

    super(input, {
      ...options,
      name: options?.name ?? ("catchError" as NAME),
      source: {
        consume: (handler, options) => input.consume(handler, options),
      },
    });
  }
}

export function catchError<
  INPUT extends AnyProducer,
  VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>,
  NAME extends NonEmptyString = "catchError",
>(
  handler: Consumer.Handler<any, never, any>,
  options?: CatchError.Options<VALUE, NAME>,
): Transform<INPUT, NAME, CatchError<INPUT, VALUE, NAME>> {
  return (input, name) => new CatchError(input, handler, { ...options, name: name ?? options?.name });
}

export namespace CatchError {
  export type Options<VALUE, NAME extends NonEmptyString> = Omit<Transformer.Options<VALUE, NAME>, "source">;
}
