import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { Result } from "../core/types";

export class PromiseSource<VALUE> extends Source<Result<VALUE>> {
  constructor(private promise: Promise<VALUE>) {
    super();
  }
  consume(options?: Consumer.Options<Result<VALUE>>): Consumer<Result<VALUE>> {
    return new Consumer(new ConsumerOptions(this.promise, options));
  }
}

export function fromPromise<VALUE>(promise: Promise<VALUE>): PromiseSource<VALUE> {
  return new PromiseSource(promise);
}

class ConsumerOptions<VALUE> extends Consumer.DefaultOptions<Result<VALUE>> {
  constructor(
    private promise: Promise<VALUE>,
    options?: Consumer.Options<Result<VALUE>>,
  ) {
    super(options);
  }
  override next(consumer: Consumer<Result<VALUE>>): void {
    super.next(consumer);
    this.promise
      .then((value) => consumer.push({ ok: true, value }))
      .catch((error) => consumer.push({ ok: false, error }))
      .finally(() => consumer.terminate("complete"));
  }
}
