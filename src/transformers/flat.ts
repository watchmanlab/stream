import { Stream, Transformer } from "../core/index.ts";

const NAME = "flat";

export class Flat<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  DEPTH extends number = 0,
  NAME extends string = flat.Name,
> extends Transformer<INPUT_STREAM, FlatArray<VALUE, DEPTH>, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, depth = 0 as DEPTH) {
    super(
      name,
      inputStream,
      inputStream.channels.get({
        next: (batch) => {
          this.batch(batch.flat(depth + 1) as FlatArray<VALUE, DEPTH>);
          this.source?.ready();
        },
        return: () => this.source?.return(),
      }),
    );
  }
}
export function flat<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  DEPTH extends number = 0,
  NAME extends string = flat.Name,
>(depth?: DEPTH): Stream.Transform<INPUT_STREAM, NAME, Flat<INPUT_STREAM, VALUE, DEPTH, NAME>> {
  return (inputStream, name) => new Flat(name, inputStream, depth);
}

export namespace flat {
  export type Name = typeof NAME;
}
