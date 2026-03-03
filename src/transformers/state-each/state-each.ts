import { Stream } from "../../streams";
import { each } from "../each";
import { effect } from "../effect";
import { map } from "../map";
import { pump } from "../pump";
import { stateMap, StateMap } from "../state-map";

const NAME = "stateEach";

export class StateEach<
  VALUE,
  STATE extends Record<string, unknown>,
  ERROR,
  NAME extends string = stateEach.Name,
> extends Stream<VALUE, NAME> {
  private _stateMap: StateMap<VALUE, VALUE, STATE, ERROR>;
  constructor(
    source: Stream<VALUE, any>,
    name = NAME as NAME,
    initialState: STATE,
    callback: stateEach.Callback<VALUE, STATE, ERROR, StateEach<VALUE, STATE, ERROR, NAME>>,
  ) {
    super(name, async function* () {
      const generator = self._stateMap[Symbol.asyncIterator]();
      let next = await generator.next();
      try {
        while (!next.done) {
          const feedback = yield next.value;
          next = await generator.next(feedback);
        }
      } finally {
        await generator.return();
      }
    });
    const self = this;

    this._stateMap = new StateMap<VALUE, VALUE, STATE, ERROR>(
      source,
      undefined,
      initialState,
      async (state, value, _, compensate) => {
        const newState = await callback(state, value, this, compensate);
        if (Stream.Result.isErr(newState)) return newState;
        return [value, newState];
      },
    );
  }
}

export function stateEach<VALUE, STATE extends Record<string, unknown>, ERROR, NAME extends string = stateEach.Name>(
  initialState: STATE,
  callback: stateEach.Callback<VALUE, STATE, ERROR, StateEach<VALUE, STATE, ERROR, NAME>>,
): Stream.Transformer<NAME, Stream<VALUE, any>, StateEach<VALUE, STATE, ERROR, NAME>> {
  return (_, source, name) => new StateEach(source, name, initialState, callback);
}

export namespace stateEach {
  export type Name = typeof NAME;

  export type Callback<VALUE, STATE extends Record<string, unknown>, ERROR, SELF extends Stream<any, any>> = (
    state: STATE,
    value: VALUE,
    self: SELF,
    compensate: map.Compensate,
  ) => STATE | Stream.Result.Err<ERROR> | Promise<STATE | Stream.Result.Err<ERROR>>;
}

const now = performance.now();

// function getStream(gen: () => AsyncGenerator<number>) {
//   return new Stream<number>(gen)
//     .pipe(
//       stateEach({ count: 0 }, (state, v) => {
//         return { count: state.count + 4 };
//       }),
//     )
//     .pipe(
//       each((v) => {
//         if (v === MAX + 1) console.log(performance.now() - now);
//       }),
//     )
//     .pipe(
//       each((v) => {
//         if (v === MAX + 1) console.log(performance.now() - now);
//       }),
//     )
//     .pipe(
//       each((v) => {
//         if (v === MAX + 1) console.log(performance.now() - now);
//       }),
//     )
//     .pipe(
//       each((v) => {
//         if (v === MAX + 1) console.log(performance.now() - now);
//       }),
//     )
//     .pipe(
//       each((v) => {
//         if (v === MAX / WORKERS) console.log(performance.now() - now);
//       }),
//     );
// }
// const MAX = 1_000_000;
// const WORKERS = 100;
// let i = 0;

// while (i <= WORKERS) {
//   i++;
//   getStream(async function* () {
//     let j = 0;
//     while (j <= MAX / WORKERS) {
//       yield j++;
//     }
//   }).pipe(pump());
// }

const MAX = 1_000_000;
function* gen() {
  let i = 1;
  while (i <= MAX) {
    yield i++;
  }
}
function* gen2() {
  yield* (function* () {
    yield* gen();
  })();
}

for (const v of gen()) {
  if (v === MAX + 10) console.log();
}
console.log("gen", performance.now() - now);
for (const v of gen2()) {
  if (v === MAX + 10) console.log();
}
console.log("gen2", performance.now() - now);
