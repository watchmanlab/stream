//
//

import { Consumer } from "./consumer";
import type { Source } from "./types";

function smoke<VALUE>(source: Source<VALUE>) {
  ///
}

smoke({
  listen: (init) => {
    return new Consumer<any, any, any>({
      ...init,
      //
    });
  },
});
