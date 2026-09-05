import { Consumable } from "../core/consumable";
import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Stream } from "../core/stream";
import { ValueOfConsumable } from "../core/types";

/**
 * Passes only values that satisfy the predicate downstream.
 * Values that fail the predicate are forwarded to the lazy `$complements` stream
 * at zero cost if unused.
 * `$complements` is passive relative to the `filter` , that's mean consuming `$complements`
 * will never trigger the `filter` consumption
 *
 * @template INPUT The upstream consumable type.
 * @template VALUE The upstream value type.
 * @template FILTERED The narrowed output type (supports type guards).
 *
 * @example
 * const f = of(1,2,3,4).pipe(filter(v => v % 2 === 0));
 * f.$complements.pipe(listen(console.log)); // 1, 3
 * f.pipe(listen(console.log));              // 2, 4
 */
export class Filter<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  FILTERED extends VALUE = VALUE,
> extends Source<FILTERED> {
  constructor(
    readonly $input: INPUT,
    private predicate: Filter.Predicate<VALUE, FILTERED>,
    private complement?: (value: VALUE, index: number) => void,
  ) {
    super();
  }
  private _$complements?: Stream<VALUE>;

  /**
   * Lazy stream of values rejected by the predicate.
   * Only allocated when first accessed.
   */
  get $complements(): Source<VALUE> {
    return Source.from(
      (this._$complements ??= new Stream({
        lastConsumerLeft: () => (this._$complements = undefined),
      })),
    );
  }
  consume(handler: Consumer.Handler<FILTERED>, options?: Consumer.Options<FILTERED>): Consumer<FILTERED> {
    let filteredIndex = 0;
    let complementIndex = 0;

    return this.$input.consume((c, v) => {
      if (this.predicate(v, filteredIndex++)) {
        handler(c, v);
      } else {
        this._$complements?.push(v);
        this.complement?.(v, complementIndex++);
        c.next();
      }
    }, options);
  }
}
/**
 * Passes only values that satisfy the predicate downstream.
 * Values that fail the predicate are forwarded to the lazy `$complements` stream
 * at zero cost if unused.
 * `$complements` is passive relative to the `filter` , that's mean consuming `$complements`
 * will never trigger the `filter` consumption
 *
 * @template INPUT The upstream consumable type.
 * @template VALUE The upstream value type.
 * @template FILTERED The narrowed output type (supports type guards).
 *
 * @param predicate Function `(value, index) => boolean` or type guard.
 * @param complement Optional inline callback for rejected values.
 *
 * @example
 * const f = of(1,2,3,4).pipe(filter(v => v % 2 === 0));
 * f.$complements.pipe(listen(console.log)); // 1, 3
 * f.pipe(listen(console.log));              // 2, 4
 */
export function filter<
  INPUT extends Consumable.AnyConsumable,
  VALUE extends ValueOfConsumable<INPUT> = ValueOfConsumable<INPUT>,
  FILTERED extends VALUE = VALUE,
>(predicate: Filter.Predicate<VALUE, FILTERED>, complement?: (value: VALUE, index: number) => void) {
  return ($input: INPUT) => new Filter($input, predicate, complement);
}

export namespace Filter {
  export type Predicate<VALUE, FILTERED extends VALUE> =
    | ((value: VALUE, index: number) => value is FILTERED)
    | ((value: VALUE, index: number) => boolean);
}
