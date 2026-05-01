import { Stream } from "../../stream/stream";

const NAME = "withStats";

class WithStats<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = withStats.Name,
> extends Stream<VALUE, NAME> {
  private stats?: {
    activeSince: number;
    eventsPerSecond: number;
    listenersCount: number;
    valuesDroppedCount: number;
    terminatedCount: number;
  };
  constructor(name: NAME, inputStream: INPUT_STREAM) {
    super(name);
    inputStream.listen((value) => this.push(value));

    this.hooks = {
      //
    };
  }
}

export function withStats<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = withStats.Name,
>(name?: NAME): Stream.Transform<INPUT_STREAM, Stream.Traversable<WithStats<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>> {
  return (inputStream) => Stream.traversable(new WithStats(name ?? (NAME as NAME), inputStream), inputStream);
}

export namespace withStats {
  export type Name = typeof NAME;
}
