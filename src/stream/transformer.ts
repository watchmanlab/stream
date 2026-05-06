import { Source } from "./source.ts";
import { Stream } from "./stream.ts";

export abstract class Transformer<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE,
  ERROR extends { error: unknown; reason: Stream.ExtractValue<INPUT_STREAM> },
  NAME extends string,
> extends Stream<VALUE, ERROR, NAME> {
  constructor(
    name: NAME,
    protected readonly inputStream: INPUT_STREAM,
    sourceData?: Source.SourceData<VALUE, ERROR>,
  ) {
    super(name, sourceData);

    return new Proxy(this, {
      get(target, p, receiver) {
        if (p in target) return Reflect.get(target, p, receiver);
        return inputStream;
      },
    });
  }
  get traversal(): Record<INPUT_STREAM["name"] | (`$${string}` & {}), Transformer.Traversable<INPUT_STREAM>> {
    const self = this;
    return new Proxy(
      {},
      {
        get() {
          return self.inputStream;
        },
      },
    ) as never;
  }
}

export namespace Transformer {
  export type AnyTransformer = Transformer<Stream.AnyStream, any, any, any>;
  export type ExtractValue<T> = T extends Transformer<any, infer VALUE, any, any> ? VALUE : never;
  export type ExtractName<T> = T extends AnyTransformer ? T["name"] : never;
  export type ExtractError<T> =
    T extends Transformer<any, any, { error: infer ERROR; reason: any }, any> ? ERROR : never;
  export type ExtractInputStream<T> = T extends Transformer<infer INPUT_STREAM, any, any, any> ? INPUT_STREAM : never;
  export type Traversable<T extends Stream.AnyStream> =
    ExtractInputStream<T> extends never
      ? T
      : Omit<T, "traversal"> &
          Record<ExtractInputStream<T>["name"] | (`$${string}` & {}), Traversable<ExtractInputStream<T>>>;
}
