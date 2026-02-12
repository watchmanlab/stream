import { OutputStream, Stream } from "../../stream";

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
export function filter<INPUT extends Stream<any>, FILTERED extends Stream.ValueOf<INPUT>>(
  predicate: filter.Predicate<Stream.ValueOf<INPUT>>,
): Stream.Transformer<INPUT, Stream<FILTERED, "filter">> {
  return (source) =>
    new Stream<FILTERED, "filter">(async function* () {
      for await (const value of source) {
        if (await predicate(value)) yield value as FILTERED;
      }
    });
}

export namespace filter {
  export type Filter<VALUE> = Stream<VALUE>;
  export type GardPredicate<VALUE, FILTERED extends VALUE = VALUE> = (value: VALUE) => value is FILTERED;
  export type Predicate<VALUE> = (value: VALUE) => boolean | Promise<boolean>;
}

const stream = new Stream<number>().pipe(filter((x) => x > 0)).pipe(filter((x) => x > 0));
// .pipe(snapshot("f2"));

stream.listen((v) => console.log(v));

stream.filter.root.