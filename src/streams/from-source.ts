import { Source } from "../core/source";
import { Stream } from "../core/stream";

export class FromSource<VALUE> extends Stream<VALUE> {
  constructor(source: Source<VALUE>, options?: Stream.Options<VALUE>) {
    super(options);
  }
}
