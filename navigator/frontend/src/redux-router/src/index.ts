// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Action, Dispatch, Middleware, MiddlewareAPI } from "redux";

/**
 * This is Redux middleware for handling History API based routing/URLs. The
 * core idea is that the URL bar is just a special view component. Its state is
 * fully defined by the Redux state and user interactions with the URL bar --
 * like clicking back -- results in a normal Redux action. This middleware
 * manages this given two functions:
 *
 * export interface RouterOptions<S> {
 *   stateToUrl(state: S): URL;
 *   urlToAction(url: URL): Action;
 * }
 *
 * Upon initialisation, an action for the initial URL will be dispatched
 * immediately.
 */

export type URL = string;

export interface RouterOptions<S> {
  stateToUrl(state: S): URL;
  urlToAction(url: URL): Action;
}

export type RouterMiddleware<S> = (options: RouterOptions<S>) => Middleware;

export function middleware<S>(options: RouterOptions<S>): Middleware<{}, S> {
  const { stateToUrl, urlToAction } = options;
  return ({
    dispatch,
    getState,
  }: MiddlewareAPI<Dispatch, S>): ((next: Dispatch) => Dispatch) => {
    // Install URL change listener. This dispatches an action whenever a URL is
    // popped from the browser history.
    window.onpopstate = (_: PopStateEvent) => {
      dispatch(urlToAction(window.location.pathname));
    };

    return (next: Dispatch) =>
      ((action: Action) => {
        // If this is a router action, we capture it and dispatch the action
        // corresponding to the specified URL. We then get the URL for the new
        // state and push or replace this onto the browser history. If it's not a
        // router action, we just pass it on and update the browser URL in place.
        const returnValue = next(action);
        // Middleware typings are not very good in Redux 3,
        // We know that S and S2 are the same type.
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const url = stateToUrl(getState() as any as S);
        if (window.location.pathname !== url) {
          window.history.pushState({}, "", url);
        }
        return returnValue;
      }) as Dispatch;
  };
}
