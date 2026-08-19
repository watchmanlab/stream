import { Consumable } from "./consumable0";
import { Source } from "./source0";

export interface Transformer<INPUT extends Consumable.AnyConsumable, VALUE> extends Source<VALUE> {
  readonly $input: INPUT;
}

export namespace Transformer {
  export type AnyTransformer = Transformer<any, any>;
}
