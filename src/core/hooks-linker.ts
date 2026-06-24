import { Hooks } from "./types";

export class HooksLinker<EVENTS extends Record<string, unknown>, TARGET> {
  constructor(
    private target: TARGET,
    private hooks: Hooks<EVENTS, TARGET>,
  ) {}
  hook<KEY extends keyof Hooks<EVENTS, TARGET>, ARGS extends [...any[]], FN extends (...args: ARGS) => void>(
    hookName: KEY,
    fn: FN,
  ): FN {
    const { target } = this;
    return this.hooks[hookName] ? this.hooks[hookName](target, fn) : fn;
  }
}
