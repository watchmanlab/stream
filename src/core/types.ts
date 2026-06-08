export interface Observable<VALUE> {
  listen(listener: Observable.Listener<VALUE>): Observable.Abort;
}
export namespace Observable {
  export type Abort = () => void;
  export type Listener<VALUE> = (value: VALUE) => void;
  export type Subscription<VALUE> = { listener: Listener<VALUE>; index: number };
}
