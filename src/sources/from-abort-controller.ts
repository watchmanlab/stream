import { Consumable } from "../core/types";
import { fromAbortSignal } from "./from-abort-signal";

export function fromAbortController(controller: AbortController): Consumable<void> {
  return fromAbortSignal(controller.signal);
}
