// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import RouteParser from "route-parser";

export type Path = string;

export class Route<P, A, S> {
  private route: RouteParser;

  constructor(
    private pattern: string,
    private createAction: (params: P) => A | undefined,
    public extractParams: (state: S) => P | undefined,
  ) {
    this.route = new RouteParser(this.pattern);
  }

  render(params: P): Path {
    const result = this.route.reverse(params || {});
    return typeof result === "string" ? result : "";
  }

  match(path: Path): A | undefined {
    const match = this.route.match(path) as P | boolean;
    if (typeof match !== "boolean") {
      return this.createAction(match);
    } else {
      return undefined;
    }
  }
}

type CombinedRoutes<P, A, S> = {
  stateToPath: (state: S) => Path;
  pathToAction: (path: Path) => A;
  activeRoute: () => Route<P, A, S>;
};

export function combineRoutes<P, A, S>(
  routes: Route<P, A, S>[],
): CombinedRoutes<P, A, S> {
  // Given a state, what's the current path?
  function stateToPath(state: S): Path {
    for (const route of routes) {
      const params = route.extractParams(state);
      if (params) {
        return route.render(params);
      }
    }
    throw new Error(`Some route must match the state`);
  }

  // Given a Path change, what action should be sent?
  function pathToAction(path: Path): A {
    for (const route of routes) {
      const action = route.match(path);
      if (action) {
        return action;
      }
    }
    throw new Error(`Some route must match the path: ${path}`);
  }

  // What's the active route?
  function activeRoute(): Route<P, A, S> {
    const path = window.location.pathname;
    for (const route of routes) {
      const action = route.match(path);
      if (action) {
        return route;
      }
    }
    throw new Error(`Some route must match the path: ${path}`);
  }

  return { stateToPath, pathToAction, activeRoute };
}
