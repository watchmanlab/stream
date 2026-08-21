import { Consumer } from "../core/consumer";
import { Source } from "../core/source";
import { TerminateReason } from "../core/types";

export class EventTargetSource<EVENT_TYPE extends keyof HTMLElementEventMap | (string & {})> extends Source<
  EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event
> {
  constructor(
    private target: EventTarget,
    private eventType: EVENT_TYPE,
  ) {
    super();
  }

  consume(
    options?: Consumer.Options<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event>,
  ): Consumer<EVENT_TYPE extends keyof HTMLElementEventMap ? HTMLElementEventMap[EVENT_TYPE] : Event> {
    let abortController = new AbortController();

    return new Consumer(new ConsumerOptions(this, abortController, options));
  }
}
export function fromEventTarget<EVENT_TYPE extends keyof HTMLElementEventMap | (string & {})>(
  target: EventTarget,
  eventType: EVENT_TYPE,
): EventTargetSource<EVENT_TYPE> {
  return new EventTargetSource(target, eventType);
}

class ConsumerOptions extends Consumer.DefaultOptions<any> {
  constructor(
    private eventTargetSource: EventTargetSource<any>,
    private abortController: AbortController,
    options?: Consumer.Options<any>,
  ) {
    super(options);
  }
  override next(consumer: Consumer<any>): void {
    super.next(consumer);
    this.eventTargetSource["target"].addEventListener(
      this.eventTargetSource["eventType"],
      (e: any) => consumer.push(e),
      { signal: this.abortController.signal },
    );
  }
  override terminate(consumer: Consumer<any>, reason: TerminateReason): void {
    super.terminate(consumer, reason);
    this.abortController.abort();
  }
}
