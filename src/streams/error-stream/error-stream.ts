import { Stream } from "../stream/index.ts";

export class ErrorStream<VALUE, ERROR, NAME extends string> extends Stream<VALUE, NAME> {
  protected _errors?: Stream<ErrorStream.ErrorEvent<ErrorStream<VALUE, ERROR, NAME>>, `${NAME}Errors`>;
  constructor();
  constructor(source: Stream.Source<VALUE>);
  constructor(name: NAME);
  constructor(name: NAME, source: Stream.Source<VALUE>);

  constructor(sourceOrName?: Stream.Source<VALUE> | NAME, source?: Stream.Source<VALUE>) {
    super(sourceOrName as never, source as never);
  }

  get errors() {
    if (!this._errors) this._errors = new Stream(`${this._name}Errors` as never);
    return this._errors;
  }
}

export namespace ErrorStream {
  export type ExtractErrorFromErrorStream<STREAM extends ErrorStream<any, any, any>> =
    STREAM extends ErrorStream<any, infer ERROR, any> ? ERROR : never;

  export type ExtractErrorFromValue<T> = Extract<T, SourceErr<any>>;
  export type ExtractErrorFromSource<T extends Stream.Source<any>> = ExtractErrorFromValue<
    Stream.ExtractValueFromSource<T>
  >;
  export type ExtractErrorFromSourceErr<SOURCE_ERR extends SourceErr<any>> =
    SOURCE_ERR extends SourceErr<infer SOURCE> ? ExtractErrorFromErrorStream<SOURCE> : never;

  export type MaybeErr<ERROR> = [ERROR] extends [never] ? never : Err<ERROR>;
  export type MaybeSourceErr<ERROR, SOURCE_ERR extends SourceErr<any>> = [ERROR] extends [never] ? never : SOURCE_ERR;
  export type ErrorEvent<SOURCE extends ErrorStream<any, any, any>> =
    | {
        type: "expected";
        source: SOURCE;
        value: Stream.ExtractValueFromSource<SOURCE>;
        detail: ExtractErrorFromErrorStream<SOURCE>;
      }
    | { type: "unexpected"; source: SOURCE; value: Stream.ExtractValueFromSource<SOURCE>; detail: unknown };

  export class SourceErr<SOURCE extends ErrorStream<any, any, any>> extends Stream.Sentinel {
    constructor(
      public readonly source: SOURCE,
      public readonly value: Stream.ExtractValueFromSource<SOURCE>,
      public readonly detail: ExtractErrorFromErrorStream<SOURCE>,
    ) {
      super();
    }
  }

  export class Err<ERROR> {
    constructor(public readonly value: ERROR) {}
  }
  export function err<ERROR>(value: ERROR): Err<ERROR> {
    return new Err(value);
  }
  export function sourceErr<SOURCE extends ErrorStream<any, any, any>>({
    source,
    value,
    detail,
  }: {
    source: SOURCE;
    value: Stream.ExtractValueFromSource<SOURCE>;
    detail: ExtractErrorFromErrorStream<SOURCE>;
  }) {
    return new SourceErr(source, value, detail);
  }
  export function isErr<ERROR>(object: unknown): object is Err<ERROR> {
    return object instanceof Err;
  }
  export function isSourceErr<SOURCE extends ErrorStream<any, any, any>>(object: unknown): object is SourceErr<SOURCE> {
    return object instanceof SourceErr;
  }
}
