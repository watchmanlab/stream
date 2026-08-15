import { FromAbortSignal } from "./from-abort-signal";

export class FromAbortController extends FromAbortSignal {
  constructor(private controller: AbortController) {
    super(controller.signal);
  }
}
export function fromAbortController(controller: AbortController): FromAbortController {
  return new FromAbortController(controller);
}
