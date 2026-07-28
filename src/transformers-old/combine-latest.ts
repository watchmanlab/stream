import { Mitto } from "../mitto";
import { Transformer } from "../transformer";

export class CombineLatest<
  INPUT extends Mitto.AnyMitto,
  OTHERS extends readonly [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]],
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = combineLatest.Name,
> extends Transformer<INPUT, [curr: VALUE, ...{ [K in keyof OTHERS]: Mitto.ExtractValue<OTHERS[K]> }], NAME> {
  private _buffer: [curr: VALUE | Mitto.Empty, ...{ [K in keyof OTHERS]: Mitto.ExtractValue<OTHERS[K]> | Mitto.Empty }];
  private _others: OTHERS;
  constructor(name = combineLatest.NAME as NAME, input: INPUT, others: OTHERS) {
    const mittos = [input, ...others] as [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]];

    super(name, input, {
      scope: { any: mittos },
      source: () => {
        const signals = mittos.map((mitto, index) =>
          mitto.consume((value) => {
            this._buffer[index] = value;

            if (this._buffer.every((v) => v !== Mitto.EMPTY)) this.emit([...this._buffer] as never);
          }),
        );

        return () => signals.forEach((signal) => signal.emit());
      },

      abort: () => {
        mittos.length = 0;
        this._buffer.length = 0;
      },
    });
    this._buffer = new Array(others.length + 1).fill(Mitto.EMPTY) as never;
    this._others = others;
  }

  get buffer(): [curr: VALUE | Mitto.Empty, ...{ [K in keyof OTHERS]: Mitto.ExtractValue<OTHERS[K]> | Mitto.Empty }] {
    return [...this._buffer];
  }
  get others(): OTHERS {
    return [...this._others];
  }
}

export function combineLatest<
  INPUT extends Mitto.AnyMitto,
  OTHERS extends readonly [other: Mitto.AnyMitto, ...others: Mitto.AnyMitto[]],
  VALUE extends Mitto.ExtractValue<INPUT> = Mitto.ExtractValue<INPUT>,
  NAME extends string = combineLatest.Name,
>(...others: OTHERS): Mitto.Transform<INPUT, NAME, CombineLatest<INPUT, OTHERS, VALUE, NAME>> {
  return (input, name) => new CombineLatest(name, input, others);
}

export namespace combineLatest {
  export const NAME = "combineLatest";
  export type Name = typeof NAME;
}
