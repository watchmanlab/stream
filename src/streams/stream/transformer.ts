import { Stream } from "./stream";

type ExtractTraversal<T extends Stream.AnyStream, ACC extends Stream.AnyStream[] = []> =
  T extends Transformer<infer INPUT_STREAM, any, any, any>
    ? ExtractTraversal<INPUT_STREAM, [INPUT_STREAM, ...ACC]>
    : ACC[number] | T;

export abstract class Transformer<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE,
  ERROR,
  NAME extends string,
> extends Stream<VALUE, ERROR, NAME> {
  constructor(
    name: NAME,
    protected readonly inputStream: INPUT_STREAM,
    fn?: () => AsyncGenerator<VALUE>,
  ) {
    super(name, fn!);
  }

  getUpStream<NAME extends string = Stream.ExtractName<ExtractTraversal<INPUT_STREAM>>>(name: NAME) {
    //
  }
}
