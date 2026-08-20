import { Consumable } from "./consumable";
import { Source } from "./source";

export interface Transformer<INPUT extends Consumable.AnyConsumable, VALUE> extends Source<VALUE> {
  readonly $input: INPUT;
}

export namespace Transformer {
  export type AnyTransformer = Transformer<any, any>;
}
