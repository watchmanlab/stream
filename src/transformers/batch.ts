import { Stream, Transformer } from "../core/index.ts";
import { each } from "./each.ts";
import { pump } from "./pump.ts";

const NAME = "batch";

export class Batch<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = batch.Name,
> extends Transformer<INPUT_STREAM, VALUE, NAME> {
  private _buffer: VALUE[] = [];
  constructor(name = NAME as NAME, inputStream: INPUT_STREAM, size: number) {
    const inputChannel = inputStream.channels.get({
      next: (batch) => {
        this._buffer.push(...batch);
        if (this._buffer.length >= size) {
          this.batch(this._buffer.splice(0, size));
        } else {
          inputChannel.next();
        }
      },
    });
    super(name, inputStream, {
      next: () => {
        if (this._buffer.length >= size) {
          this.batch(this._buffer.splice(0, size));
        } else {
          inputChannel.next();
        }
      },
      return: () => {
        // inputChannel.return();
      },
    });
  }
  get buffer() {
    return this._buffer;
  }
}

export function batch<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = batch.Name,
>(size: number): Stream.Transform<INPUT_STREAM, NAME, Batch<INPUT_STREAM, VALUE, NAME>> {
  return (inputStream, name) => new Batch(name, inputStream, size);
}

export namespace batch {
  export type Name = typeof NAME;
}
