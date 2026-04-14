import { Stream } from "./stream9";

const NAME = "withEvents";

class WithEvents<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = withEvents.Name,
> extends Stream<VALUE, NAME> {
  private events?: {
    listenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}ListenerAdded`>;
    firstListenerAdded?: Stream<Stream.Listener<VALUE>, `${NAME}FirstListenerAdded`>;
    listenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}ListenerRemoved`>;
    lastListenerRemoved?: Stream<Stream.Listener<VALUE>, `${NAME}LastListenerRemoved`>;
    valuesDropped?: Stream<VALUE[], `${NAME}ValuesDropped`>;
    terminated?: Stream<void, `${NAME}Terminated`>;
  };
  constructor(name: NAME, inputStream: INPUT_STREAM) {
    super(name);
    inputStream.listen((value) => this.push(value));

    this.hooks = {
      afterListenerAdded: (fn) => this.events?.listenerAdded?.push(fn),
      afterFirstListenerAdded: (fn) => this.events?.firstListenerAdded?.push(fn),
      afterListenerRemoved: (fn) => this.events?.listenerRemoved?.push(fn),
      afterLastListenerRemoved: (fn) => this.events?.lastListenerRemoved?.push(fn),
      afterValuesDropped: (values) => this.events?.valuesDropped?.push(values),
      afterTerminate: () => {
        this.events?.terminated?.push();
        this.events?.terminated?.terminate();
      },
    };
  }

  get listenerAdded() {
    if (!this.events?.listenerAdded)
      this.events = { ...this.events, listenerAdded: new Stream(`${this.name}ListenerAdded`) };
    return this.events.listenerAdded;
  }
  get firstListenerAdded() {
    if (!this.events?.firstListenerAdded)
      this.events = { ...this.events, firstListenerAdded: new Stream(`${this.name}FirstListenerAdded`) };
    return this.events.firstListenerAdded;
  }
  get listenerRemoved() {
    if (!this.events?.listenerRemoved)
      this.events = { ...this.events, listenerRemoved: new Stream(`${this.name}ListenerRemoved`) };
    return this.events.listenerRemoved;
  }
  get lastListenerRemoved() {
    if (!this.events?.lastListenerRemoved)
      this.events = { ...this.events, lastListenerRemoved: new Stream(`${this.name}LastListenerRemoved`) };
    return this.events.lastListenerRemoved;
  }

  get valuesDropped() {
    if (!this.events?.valuesDropped)
      this.events = { ...this.events, valuesDropped: new Stream(`${this.name}ValuesDropped`) };
    return this.events.valuesDropped;
  }
  get terminated() {
    if (!this.events?.terminated) this.events = { ...this.events, terminated: new Stream(`${this.name}Terminated`) };
    return this.events.terminated;
  }
}

export function withEvents<
  INPUT_STREAM extends Stream.AnyStream,
  VALUE extends Stream.ExtractValue<INPUT_STREAM> = Stream.ExtractValue<INPUT_STREAM>,
  NAME extends string = withEvents.Name,
>(
  name?: NAME,
): Stream.Transform<INPUT_STREAM, Stream.Traversable<WithEvents<INPUT_STREAM, VALUE, NAME>, INPUT_STREAM>> {
  return (inputStream) => Stream.traversable(new WithEvents(name ?? (NAME as NAME), inputStream), inputStream);
}

export namespace withEvents {
  export type Name = typeof NAME;
}
