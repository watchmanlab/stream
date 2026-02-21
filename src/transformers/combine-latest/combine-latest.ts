import { Stream } from "../../streams/index.ts";

const NAME = "combine-latest";

export class CombineLatest<
  VALUE,
  SOURCES extends [Stream<any, any>, ...Stream<any, any>[]],
  NAME extends string = combineLatest.Name,
> extends Stream<[VALUE, ...{ [K in keyof SOURCES]: Stream.ValueOf<SOURCES[K]> }], NAME> {
  protected _latest: [
    VALUE | combineLatest.Empty,
    ...{ [K in keyof SOURCES]: Stream.ValueOf<SOURCES[K]> | combineLatest.Empty },
  ];
  constructor(source: Stream<VALUE, any>, name = NAME as NAME, ...sources: SOURCES) {
    super(name, async function* () {
      try {
        while (true) {
          if (self._latest.every((v) => v !== combineLatest.EMPTY)) {
            yield [...self._latest] as [VALUE, ...{ [K in keyof SOURCES]: Stream.ValueOf<SOURCES[K]> }];
          }
          await Promise.any(
            [source, ...sources].map(
              (stream, i) =>
                new Promise<void>((resolve, reject) => {
                  stream.next().then((result) => {
                    if (result.done) {
                      reject();
                      return;
                    }

                    self._latest[i] = result.value;

                    resolve();
                  });
                }),
            ),
          );
        }
      } finally {
        self._latest.length = 0;
      }
    });
    const self = this;
    this._latest = (
      new Array(sources.length + 1) as [
        VALUE | combineLatest.Empty,
        ...{ [K in keyof SOURCES]: Stream.ValueOf<SOURCES[K]> | combineLatest.Empty },
      ]
    ).fill(combineLatest.EMPTY);
  }

  get latest() {
    return [...this._latest];
  }
}
export function combineLatest<
  VALUE,
  SOURCES extends [Stream<any, any>, ...Stream<any, any>[]],
  NAME extends string = combineLatest.Name,
>(...sources: SOURCES): Stream.Transformer<NAME, Stream<VALUE, any>, CombineLatest<VALUE, SOURCES, NAME>> {
  return (_, source, name) => new CombineLatest(source, name, ...sources);
}

export namespace combineLatest {
  export type Name = typeof NAME;
  export const EMPTY = Symbol("*EMPTY#");
  export type Empty = typeof EMPTY;
}
