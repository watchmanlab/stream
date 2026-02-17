import { Stream } from "..";

const NAME = "stated";
type Name = typeof NAME;
export class State<VALUE> extends Stream<VALUE, Name> {
  protected _value: VALUE;
  protected _options: State.Options = { emitCurrent: false };

  constructor(initialValue: VALUE, options?: State.Options) {
    super(NAME);

    this._value = initialValue;
    this.options = options ?? {};
  }

  override push(value: VALUE, ...values: VALUE[]): Promise<void> {
    this._value = values.length ? values[values.length - 1] : value;

    if (this._options.emitCurrent && this._consumers.size === 0) {
      // Defer only on first push when emitCurrent is enabled
      return new Promise<void>((resolve) => {
        setTimeout(async () => {
          await super.push(value, ...values);
          resolve();
        }, 0);
      });
    }

    return super.push(value, ...values);
  }

  override async *[Symbol.asyncIterator]() {
    if (this._options.emitCurrent) {
      yield this.value as never;
    }
    yield* super[Symbol.asyncIterator]();
  }
  get value() {
    return this._value;
  }
  set value(newValue: VALUE) {
    if (this._value === newValue) return;
    this._value = newValue;
    super.push(newValue);
  }
  get options() {
    return this._options;
  }
  set options(options: State.Options) {
    this._options = { ...this._options, ...options };
  }
}

export namespace State {
  export type Options = {
    emitCurrent?: boolean; // Default: false
  };
}
