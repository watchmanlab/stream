import { Stream } from "./stream";

export class HooksLinker<FNS extends Record<string, (...args: any) => any>, SELF extends Stream.AnyStream> {
  constructor(
    private self: SELF,
    private hooks?: HooksLinker.Hooks<FNS, SELF>,
  ) {}
  hook<KEY extends keyof FNS, FN extends FNS[KEY]>(hookName: KEY, trapped: FN): FN {
    const { hooks } = this;
    return hooks?.[hookName] ? (hooks[hookName](this.self, trapped) as FN) : trapped;
  }
}

export namespace HooksLinker {
  export type Hooks<FNS extends Record<string, (...args: any) => any>, SELF extends Stream.AnyStream> = {
    [K in keyof FNS]?: (self: SELF, trapped: FNS[K]) => FNS[K];
  };

  export type TrappedFromEvents<
    EVENTS extends Record<string, unknown>,
    FNS extends { [K in keyof EVENTS]: (...arg: [EVENTS[K]]) => void } = {
      [K in keyof EVENTS]: (...arg: [EVENTS[K]]) => void;
    },
  > = FNS;
}
