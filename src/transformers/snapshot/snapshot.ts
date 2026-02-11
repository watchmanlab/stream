import { Stream } from "../../stream";

export function snapshot<NAME extends string, INPUT extends Stream<any>>(
  name: NAME,
): Stream.Transformer<INPUT, INPUT & { [K in NAME]: INPUT }> {
  return function (source) {
    const output = new Stream(source) as any;

    output[name] = source;

    return output;
  };
}
