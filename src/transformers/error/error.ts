import { Stream } from "../../streams";

export class Error<
  INPUT_STREAM extends Stream.AnyStream<{ value: any; error: any }>,
  NAME extends string,
> extends Stream<Stream.ExtractValue<INPUT_STREAM>, NAME> {
  constructor() {
    super();
  }
}
