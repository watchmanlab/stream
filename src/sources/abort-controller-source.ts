import { AbortSignalSource } from "./abort-signal-source";

/**
 * Convenience wrapper around {@link AbortSignalSource} that accepts an `AbortController`.
 *
 * @example
 * const controller = new AbortController();
 * fromAbortController(controller).pipe(listen(() => console.log('aborted')));
 * controller.abort();
 */
export class AbortControllerSource extends AbortSignalSource {
  constructor(controller: AbortController) {
    super(controller.signal);
  }
}
/**
 * Creates an `AbortControllerSource`.
 * @param controller The `AbortController` whose signal to observe.
 */
export function fromAbortController(controller: AbortController): AbortControllerSource {
  return new AbortControllerSource(controller);
}
