import { Channel, Stream, Transformer } from "../core/index.ts";

const NAME = "batchCount";

export class BatchCount<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = batchCount.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, count: number) {
    const buffer: VALUE[] = [];
    const inputChannel = inputStream.channels.get({ onNext: (batch) => buffer.push(...batch) });
    const outputChannel = new Channel<VALUE>({ onNext: (batch) => this.batch(batch) });

    super(name, inputStream, () => {
      return {
        next: () => {
          if (buffer.length >= count) {
            //
          } else {
            //
          }
          return "dd";
        },
        return: () => {
          //
        },
      };
    });
  }
}

export namespace batchCount {
  export type Name = typeof NAME;
}
