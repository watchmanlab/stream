import { Stream } from "../../streams";

const NAME = "concurrent";

export class Concurrent<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = concurrent.Name,
> extends Stream<
  | MAPPED
  | Stream.ExtractSentinel<SOURCE>
  | Stream.MaybeSourceErr<CLEAN_VALUE, ERROR, Concurrent<SOURCE, CLEAN_VALUE, MAPPED, ERROR, NAME>>,
  NAME
> {
  protected _options: Required<concurrent.Options> = { concurrencyLimit: 1000, preserveOrder: false };
  protected _buffer: (MAPPED | Promise<MAPPED | Stream.Err<ERROR>>)[] = [];
  protected _pending: number = 0;
  protected _events?: Stream<concurrent.Event<this>, `${NAME}Events`>;
  protected _errors?: Stream<Stream.ErrorEvent<CLEAN_VALUE, ERROR, this>, `${NAME}Errors`>;
  constructor(
    source: SOURCE,
    name = NAME as NAME,
    mapper: concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
    options?: concurrent.Options,
  ) {
    super(name, async function* () {
      let resolver: () => void;
      let concurrencyLimitResolver: () => void;
      let aborted = false;
      const generator = Stream.generator(source);

      (async () => {
        for await (const value of generator) {
          if (aborted) break;
          if (self._pending >= self._options.concurrencyLimit) {
            self._events?.push({ type: "concurrency-limit-reached", self });
            await new Promise<void>((r) => (concurrencyLimitResolver = r));
          }
          self._pending++;

          if (self._options.preserveOrder) {
            try {
              self._buffer.push(mapper(value));
              resolver!?.();
            } catch (error) {
              if (Stream.isErr(error)) {
                self._errors?.push({ type: "expected", error: error.value as ERROR, self });
              } else {
                self._errors?.push({ type: "unexpected", error, self });
              }
            }
          } else {
            mapper(value)
              .then((mapped) => {
                self._pending--;
                if (aborted) return;

                if (Stream.isErr(mapped)) {
                  self._errors?.push({ type: "expected", error: mapped.value, self });
                  return;
                }

                self._buffer.push(mapped);
                resolver!?.();
              })
              .catch((error) => {
                if (Stream.isErr(error)) {
                  self._errors?.push({ type: "expected", error: error.value as ERROR, self });
                } else {
                  self._errors?.push({ type: "unexpected", error, self });
                }
              });
          }
        }
        aborted = true;
        resolver!?.();
      })();
      try {
        while (true) {
          if (self._buffer.length && !aborted) {
            const maybePromise = self._buffer.shift()!;

            if (maybePromise instanceof Promise) self._pending--;

            try {
              const result = await maybePromise;

              if (Stream.isErr(result)) {
                self._errors?.push({ type: "expected", error: result.value as ERROR, self });
                continue;
              }

              yield result;

              concurrencyLimitResolver!?.();
            } catch (error) {
              if (Stream.isErr(error)) {
                self._errors?.push({ type: "expected", error: error.value as ERROR, self });
              } else {
                self._errors?.push({ type: "unexpected", error, self });
              }
            }
          } else {
            await new Promise<void>((r) => (resolver = r));
          }
        }
      } finally {
        self._buffer.length = 0;
        aborted = true;
        generator.return();
        concurrencyLimitResolver!?.();
        resolver!?.();
      }
    });
    const self = this;
    this.options = options ?? {};
  }

  get options() {
    return this._options;
  }
  set options(options: concurrent.Options) {
    this._options = { ...this._options, ...options };
  }
  get events() {
    if (!this._events) this._events = new Stream(`${this._name}Events` as never);
    return this._events;
  }
  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);
    return this._errors;
  }
  get buffer() {
    return [...this._buffer];
  }
  get pending() {
    return this._pending;
  }
}

export function concurrent<
  SOURCE extends Stream<any, any>,
  CLEAN_VALUE extends Stream.ExtractCleanValue<SOURCE> = Stream.ExtractCleanValue<SOURCE>,
  MAPPED = CLEAN_VALUE,
  ERROR = never,
  NAME extends string = concurrent.Name,
>(
  mapper: concurrent.Mapper<CLEAN_VALUE, MAPPED, ERROR>,
  options?: concurrent.Options,
): Stream.Transformer<NAME, SOURCE, Concurrent<SOURCE, CLEAN_VALUE, MAPPED, ERROR, NAME>> {
  return (_, source, name) => new Concurrent(source, name, mapper, options);
}

export namespace concurrent {
  export type Name = typeof NAME;
  export type Mapper<CLEAN_VALUE, MAPPED, ERROR> = (value: CLEAN_VALUE) => Promise<MAPPED | Stream.Err<ERROR>>;
  export type Options = {
    concurrencyLimit?: number;
    preserveOrder?: boolean;
  };
  export type Event<SELF extends Stream<any, any>> = {
    type: "concurrency-limit-reached";
    self: SELF;
  };
}
