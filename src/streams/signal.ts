import { Stream } from "../core/stream";

export class Signal<VALUE> extends Stream<VALUE> {
  override push(value: VALUE): this {
    super.push(value);
    this.terminate("complete");
    return this;
  }
}
