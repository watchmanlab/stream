import { Stream } from "../../streams";

const NAME = "catchUp";

export class CatchUp<VALUE, NAME extends string = catchUp.Name, ERROR = unknown> extends Stream<VALUE, NAME> {
  protected _events?: Stream<catchUp.Event<VALUE, NAME, ERROR>, `${NAME}-events`>;
  protected _errors?: Stream<catchUp.ErrorEvent<VALUE, NAME, ERROR>, `${NAME}-errors`>;

  constructor(source: Stream<VALUE, any>, name = NAME as NAME, callback?: catchUp.Callback<VALUE, NAME, ERROR>) {
    super(name, async function* () {
      const generator = source[Symbol.asyncIterator]();
      let next = await generator.next();

      let feedback: unknown;
      try {
        while (!next.done) {
          if (!Stream.Result.isSourceErr(next.value)) {
            feedback = yield next.value;
            next = await generator.next(feedback);
            continue;
          }

          //
        }
      } finally {
        await generator.return();
      }
    });

    const self = this;
  }
}

export function catchUp() {
  //
}

export namespace catchUp {
  export type Name = typeof NAME;
  export type Callback<VALUE, NAME extends string, ERROR> = (
    error: Stream.Result.SourceErr,
    self: CatchUp<VALUE, NAME, ERROR>,
  ) => void | Stream.Result.Err<ERROR> | Promise<void | Stream.Result.Err<ERROR>>;

  export type Event<VALUE, NAME extends string, ERROR> = {
    type: "caugh";
    error: Stream.Result.SourceErr;
    self: CatchUp<VALUE, NAME, ERROR>;
  };
  export type ErrorEvent<VALUE, NAME extends string, ERROR> =
    | { type: "expected-error"; error: ERROR; self: CatchUp<VALUE, NAME, ERROR> }
    | { type: "unexpected-error"; error: unknown; self: CatchUp<VALUE, NAME, ERROR> };
}
