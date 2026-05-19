import { Stream, Transformer } from "../core/index.ts";
import { each } from "./each.ts";

const NAME = "filter";
export class Filter<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  FILTERED extends VALUE = VALUE,
  CTX = {},
  NAME extends string = filter.Name,
> extends Transformer<INPUT_STREAM, FILTERED, NAME> {
  constructor(
    name = NAME as NAME,
    inputStream: INPUT_STREAM,
    predicate: filter.Predicate<VALUE, FILTERED, CTX>,
    ctx = {} as CTX,
  ) {
    super(
      name,
      inputStream,
      inputStream.channels.get({
        next: (batch) => {
          this.batch(batch.filter((value) => predicate(value, ctx)));
          this.source?.ready();
        },
        return: () => this.source?.return(),
      }),
    );
  }
}
export function filter<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  FILTERED extends VALUE = VALUE,
  NAME extends string = filter.Name,
>(
  predicate: filter.Predicate<VALUE, FILTERED, {}>,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, VALUE, FILTERED, {}, NAME>>;
export function filter<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  FILTERED extends VALUE = VALUE,
  CTX = {},
  NAME extends string = filter.Name,
>(
  ctx: CTX,
  predicate: filter.Predicate<VALUE, FILTERED, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, VALUE, FILTERED, CTX, NAME>>;

export function filter<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  FILTERED extends VALUE = VALUE,
  CTX = {},
  NAME extends string = filter.Name,
>(
  ctxOrFilter: filter.Predicate<VALUE, FILTERED, CTX> | CTX,
  predicate?: filter.Predicate<VALUE, FILTERED, CTX>,
): Stream.Transform<INPUT_STREAM, NAME, Filter<INPUT_STREAM, VALUE, FILTERED, CTX, NAME>> {
  return (inputStream, name) => {
    return new Filter(
      name,
      inputStream,
      predicate ?? (ctxOrFilter as filter.Predicate<VALUE, FILTERED, CTX>),
      predicate ? (ctxOrFilter as CTX) : undefined,
    );
  };
}

export namespace filter {
  export type Name = typeof NAME;
  export type Predicate<VALUE, FILTERED extends VALUE, CTX> =
    | ((value: VALUE, ctx: CTX) => value is FILTERED)
    | ((value: VALUE, ctx: CTX) => boolean);
}
