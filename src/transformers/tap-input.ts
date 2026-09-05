import { Consumable } from "../core/consumable";

/**
 * Runs a side-effect with access to the raw input source before it enters the pipeline.
 * Returns the input unchanged.
 *
 * @example
 * source.pipe(tapInput(input => input.$complements.pipe(listen(console.log))));
 */
export function tapInput<INPUT extends Consumable.AnyConsumable>(fn: ($input: INPUT) => void) {
  return ($input: INPUT) => {
    fn($input);
    return $input;
  };
}
