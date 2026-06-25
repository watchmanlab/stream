export class HooksLinker<FNS extends Record<string, (...args: any) => any>> {
  constructor(private hooks?: HooksLinker.Hooks<FNS>) {}
  hook<KEY extends keyof FNS, FN extends FNS[KEY], ARGS extends Parameters<FN>, RETURN extends ReturnType<FN>>(
    hookName: KEY,
    trapped: FN,
  ): (...args: ARGS) => RETURN {
    const { hooks } = this;
    return hooks?.[hookName] ? (...args) => hooks[hookName]!(trapped, ...args) : trapped;
  }
}

export namespace HooksLinker {
  export type Hooks<FNS extends Record<string, (...args: any) => any>> = {
    [K in keyof FNS]?: (fn: FNS[K], ...args: Parameters<FNS[K]>) => ReturnType<FNS[K]>;
  };

  export type TrappedFromEvents<
    EVENTS extends Record<string, unknown>,
    FNS extends Record<keyof EVENTS, (...args: any) => any>,
  > = FNS;
}
