import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import type { AnyConsumable, ExtractValue, Transformer } from "../core/types";

export class CatchError<INPUT extends AnyConsumable, VALUE extends ExtractValue<INPUT> = ExtractValue<INPUT>>
  extends Source<VALUE>
  implements Transformer<INPUT, VALUE>
{
  constructor(
    readonly $input: INPUT,
    private handler: (error: any) => void,
  ) {
    super();
  }
  consume(handler: Consumer.Handler<VALUE>, options?: Consumer.Options<VALUE>): Consumer<VALUE> {}
}

export function catchError<INPUT extends AnyConsumable>(handler: (error: any) => void) {
  return ($input: INPUT) => new CatchError($input, handler);
}
