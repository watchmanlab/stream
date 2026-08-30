import { EMPTY } from "../core/consts";
import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { Empty, ValueOfConsumable, ZipedArray } from "../core/types";

export class Zip<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [Consumable.AnyConsumable, ...Consumable.AnyConsumable[]],
> extends Source<ZipedArray<[INPUT, ...OTHERS]>> {
  private $inputs: [INPUT, ...OTHERS];
  private _$rest?: Stream<
    [ValueOfConsumable<INPUT> | Empty, ...{ [K in keyof OTHERS]: ValueOfConsumable<OTHERS[K]> | Empty }]
  >;
  constructor($input: INPUT, $others: OTHERS) {
    super();
    this.$inputs = [$input, ...$others];
  }
  get $rest() {
    return Source.from(
      (this._$rest ??= new Stream({
        lastConsumerLeft: (stream, consumer) => {
          this._$rest = undefined;
        },
      })),
    );
  }
  override consume(
    handler: Consumer.Handler<ZipedArray<[INPUT, ...OTHERS]>>,
    options?: Consumer.Options<ZipedArray<[INPUT, ...OTHERS]>>,
  ): Consumer<ZipedArray<[INPUT, ...OTHERS]>> {
    const { next, terminate, ...rest } = options ?? {};

    const buffer = new Array(this.$inputs.length).fill(EMPTY);
    let count = buffer.length;

    const consumers$ = this.$inputs.map(($input, index) =>
      $input.consume(
        (_, value) => {
          buffer[index] = value;

          if (!--count) {
            count = buffer.length;
            const values = [...buffer];
            buffer.fill(EMPTY);
            output$.push(values as any);
          }
        },
        {
          terminate(consumer, reason) {
            output$.terminate(reason);
          },
        },
      ),
    );

    const output$ = new Consumer<ZipedArray<[INPUT, ...OTHERS]>>(handler, {
      ...rest,
      next(consumer) {
        consumers$.forEach((c) => c.next());
        next?.(consumer);
      },
      terminate: (consumer, reason) => {
        if (count < buffer.length) this._$rest?.push([...buffer] as any);
        consumers$.forEach((c) => c.terminate(reason));
        consumers$.length = 0;
        buffer.length = 0;
        terminate?.(consumer, reason);
      },
    });
    return output$;
  }
}

export function zip<
  INPUT extends Consumable.AnyConsumable,
  OTHERS extends [Consumable.AnyConsumable, ...Consumable.AnyConsumable[]],
>(...others: OTHERS) {
  return ($input: INPUT) => new Zip($input, others);
}
