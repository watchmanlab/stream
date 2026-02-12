import { Stream } from "../../stream";
import { map } from "../map";

// export function filter<VALUE, FILTERED extends VALUE = VALUE>(
//   predicate: filter.GardPredicate<VALUE, FILTERED>,
// ): Stream.Transformer<Stream<VALUE>, filter.Filter<FILTERED>>;

// export function filter<VALUE>(
//   predicate: filter.Predicate<VALUE>,
// ): Stream.Transformer<Stream<VALUE>, filter.Filter<VALUE>>;

// export function filter<VALUE, FILTERED extends VALUE = VALUE>(
//   predicate: filter.Predicate<VALUE>,
// ): Stream.Transformer<Stream<VALUE>, filter.Filter<FILTERED>> {
//   return (source) => {
//     return new Stream<FILTERED>(async function* () {
//       for await (const value of source) {
//         if (await predicate(value)) yield value as FILTERED;
//       }
//     });
//   };
// }
export function filter<VALUE, FILTERED extends VALUE>(predicate: filter.Predicate<VALUE>) {
  return (source: Stream<VALUE, any>) =>
    new Stream<FILTERED, "filtered">(async function* () {
      for await (const value of source) {
        if (await predicate(value)) yield value as FILTERED;
      }
    }, "filtered");
}

export namespace filter {
  export type Filter = { open(): void };
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean | Promise<boolean>;
}

const stream = new Stream<number, "user">([], "user").pipe(filter((x) => x > 0)).pipe(map((x) => x.toFixed()));

stream.listen((v) => console.log(v));
