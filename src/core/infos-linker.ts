export class InfosLinker<INFOS extends Record<string, any>> {
  constructor(private getters: InfosLinker.Getters<INFOS>) {}

  get infos() {
    const { getters } = this;
    return new Proxy({} as INFOS, {
      get(target, p: string) {
        const getter = getters[p];
        if (typeof getter === "function") return getter();
      },
    });
  }
}

export namespace InfosLinker {
  export type Getters<INFOS extends Record<string, any>> = { [K in keyof INFOS]: () => INFOS[K] };
}
