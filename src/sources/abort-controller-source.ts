import { AbortSignalSource } from "./abort-signal-source";

export class AbortControllerSource extends AbortSignalSource {
  constructor(controller: AbortController) {
    super(controller.signal);
  }
}
export function fromAbortController(controller: AbortController): AbortControllerSource {
  return new AbortControllerSource(controller);
}
