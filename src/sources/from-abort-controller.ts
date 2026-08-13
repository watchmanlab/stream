import { Source } from "../core/types";
import { fromAbortSignal } from "./from-abort-signal";

export function fromAbortController(controller: AbortController): Source<void> {
  return fromAbortSignal(controller.signal);
}
