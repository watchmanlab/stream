import { Stream } from "../core/stream";
import { NonEmptyString } from "../core/types";

export class FromPromise<VALUE, NAME extends NonEmptyString = "$promise"> extends Stream<VALUE, NAME> {
  private _$error?: Stream<any, `${NAME}Error`>;
  constructor(promise: Promise<VALUE>, options?: Stream.Options<VALUE, NAME>) {
    super({
      ...options,
      name: options?.name ?? ("$promise" as NAME),
      next(stream, consumer) {
        promise
          .then((value) => stream.push(value))
          .catch((error) => {
            if (self?._$error?.consumers.count) {
              self._$error.push(error);
            } else {
              throw error;
            }
          })
          .finally(() => (self._$error?.terminate("complete"), stream.terminate("complete")));

        options?.next?.(stream, consumer);
      },
    });
    const self = this;
  }

  get $error() {
    return (this._$error ??= new Stream({
      name: `${this.name}Error`,
      consumerLeft: (stream) => {
        if (!stream.consumers.count) this._$error = undefined;
      },
    }));
  }
}

export function fromPromise<VALUE, NAME extends NonEmptyString = "$promise">(
  promise: Promise<VALUE>,
  options?: Stream.Options<VALUE, NAME>,
): FromPromise<VALUE, NAME> {
  return new FromPromise(promise, options);
}
