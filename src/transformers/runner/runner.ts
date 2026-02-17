import { Stream } from "../../streams/stream/stream";
import { filter } from "../filter";

export function runner(options?: runner.Options): Stream.Transformer<Stream<any>, runner.CapableStream> {
  let {
    autoStart = false,
    startSignal,
    stopSignal,
    startSignalActivated = true,
    stopSignalActivated = true,
  } = options ?? {};

  return function runner(source) {
    let generator: AsyncGenerator | undefined;
    let events: Stream<runner.Event> | undefined;

    if (autoStart) start();

    if (startSignalActivated) startSignal?.next().then(start);

    return Stream.create<void, runner.Capability>(source, () => {
      return {
        runner: {
          get running() {
            return generator !== undefined;
          },
          get events() {
            if (!events) events = new Stream<runner.Event>();
            return events;
          },
          get startSignalActivated() {
            return startSignalActivated;
          },
          set startSignalActivated(value) {
            if (startSignalActivated === value) return;
            startSignalActivated = value;
            if (value) events?.push({ type: "start-signal-activated" });
            else events?.push({ type: "start-signal-deactivated" });
          },
          get stopSignalActivated() {
            return stopSignalActivated;
          },
          set stopSignalActivated(value) {
            if (stopSignalActivated === value) return;
            stopSignalActivated = value;
            if (value) events?.push({ type: "stop-signal-activated" });
            else events?.push({ type: "stop-signal-deactivated" });
          },
          start,
          stop,
        },
      };
    });

    function start() {
      if (generator) return;

      if (stopSignalActivated) stopSignal?.next().then(stop);

      generator = source[Symbol.asyncIterator]();
      (async () => {
        for await (const _ of generator) {
          if (!generator) break;
        }
      })();

      events?.push({ type: "start" });
    }
    function stop() {
      if (!generator) return;

      if (startSignalActivated) startSignal?.next().then(start);

      const gen = generator;
      generator = undefined;
      gen?.return(undefined);
      events?.push({ type: "stop" });
    }
  };
  //
}

export namespace runner {
  export type Options = {
    autoStart?: boolean;
    stopSignal?: Stream<any>;
    startSignal?: Stream<any>;
    startSignalActivated?: boolean;
    stopSignalActivated?: boolean;
  };
  export type Event =
    | { type: "start" }
    | { type: "stop" }
    | { type: "start-signal-activated" }
    | { type: "start-signal-deactivated" }
    | { type: "stop-signal-activated" }
    | { type: "stop-signal-deactivated" };
  export type Runner = {
    readonly running: boolean;
    readonly events: Stream<Event>;
    startSignalActivated: boolean;
    stopSignalActivated: boolean;
    start: () => void;
    stop: () => void;
  };
  export type Capability = { runner: Runner };
  export type CapableStream = Stream.Capable<void, Capability>;
}
