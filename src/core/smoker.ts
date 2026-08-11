export class Smoker<VALUE> {
  constructor() {}

  listen(listener: Smoker.Listener<VALUE>) {}
}

export namespace Smoker {
  export type Listener<VALUE> = { handler: (value: VALUE) => void; next: () => void; abort: () => void };
}
