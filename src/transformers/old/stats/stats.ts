import { Stream } from "../../../streams";

const NAME = "stats";

export class Stats<VALUE, NAME extends string = stats.Name> extends Stream<VALUE, NAME> {
  protected _events?: Stream<stats.Events<VALUE, NAME>, `${NAME}-events`>;
  constructor(source: Stream<VALUE, any>, name = NAME as NAME) {
    super(name, source);

    this._onConsumerJoin = () => this._events?.push({ type: "channel-joined", self: this });
    this._onConsumerLeft = () => this._events?.push({ type: "channel-left", self: this });
  }

  get consumersCount() {
    return this._channels.size;
  }
  get consumersPressure() {
    return [...this._channels.keys()].map((queue) => queue.length);
  }
  get events() {
    if (!this._events)
      this._events = new Stream<stats.Events<VALUE, NAME>, `${NAME}-events`>(
        `${this._name}-events` as `${NAME}-events`,
      );
    return this._events;
  }
}

export namespace stats {
  export type Name = typeof NAME;

  export type Events<VALUE, NAME extends string> =
    | { type: "channel-joined"; self: Stats<VALUE, NAME> }
    | { type: "channel-left"; self: Stats<VALUE, NAME> };
}
