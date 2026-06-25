import { Stream } from "./stream";

export class HooksLinker<FNS extends Record<string, (...args: any) => any>, SELF extends Stream.AnyStream> {
  constructor(
    private self: SELF,
    private hooks?: HooksLinker.Hooks<FNS, SELF>,
  ) {}
  hook<KEY extends keyof FNS, FN extends FNS[KEY]>(hookName: KEY, trapped: FN): FN {
    const { hooks } = this;
    return hooks?.[hookName]
      ? (((...args: Parameters<FN>) => hooks[hookName]!(this.self, trapped, ...args) as FN) as FN)
      : trapped;
  }
}

export namespace HooksLinker {
  export type Hooks<FNS extends Record<string, (...args: any) => any>, SELF extends Stream.AnyStream> = {
    [K in keyof FNS]?: (self: SELF, fn: FNS[K], ...args: Parameters<FNS[K]>) => FNS[K];
  };

  export type TrappedFromEvents<
    EVENTS extends Record<string, unknown>,
    FNS extends Record<keyof EVENTS, (...args: any) => any>,
  > = FNS;
}
