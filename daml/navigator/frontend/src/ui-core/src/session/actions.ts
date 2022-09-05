// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as Redux from "redux";
import { ThunkAction } from "redux-thunk";
import { Dispatch } from "../types";
import {
  Action,
  AuthFailure,
  AuthMethod,
  ServerResponse,
  User,
  UserId,
} from "./types";

const sessionUrl = "/api/session/";

export type To<A extends Redux.Action> = (action: Action) => A;

function authenticated<A extends Redux.Action>(to: To<A>, user: User): A {
  return to({ type: "AUTHENTICATED", user });
}

function authenticationRequired<A extends Redux.Action>(
  to: To<A>,
  method: AuthMethod,
  failure?: AuthFailure,
): A {
  return to({ type: "AUTHENTICATION_REQUIRED", method, failure });
}

function sessionError<A extends Redux.Action>(to: To<A>, error: string): A {
  return to({ type: "ERROR", error });
}

function handleSessionResponse<A extends Redux.Action>(
  to: To<A>,
  dispatch: Dispatch<A>,
) {
  return (response: ServerResponse): void => {
    switch (response.type) {
      case "sign-in":
        dispatch(authenticationRequired(to, response.method, response.error));
        break;
      case "session":
        dispatch(authenticated(to, response.user));
        break;
      default:
        dispatch(sessionError(to, "Session fetch error"));
    }
  };
}

// Async action creators
function init<A extends Redux.Action>(
  to: To<A>,
): ThunkAction<void, void, undefined, A> {
  return dispatch => {
    fetch(sessionUrl, { credentials: "include" })
      // TODO(NAV-14): Add better error handling here
      .then((res: Response) => res.json() as Promise<ServerResponse>)
      .then(handleSessionResponse(to, dispatch));
  };
}

function signIn<S, A extends Redux.Action>(
  to: To<A>,
  userId: UserId,
): ThunkAction<void, S, undefined, A> {
  return dispatch => {
    dispatch(to({ type: "AUTHENTICATING" }));
    fetch(sessionUrl, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ userId }),
    })
      .then((res: Response) => res.json() as Promise<ServerResponse>)
      .then(handleSessionResponse(to, dispatch));
  };
}

function signOut<S, A extends Redux.Action>(
  to: To<A>,
  resetStoreAction: A,
): ThunkAction<void, S, undefined, A> {
  return dispatch => {
    dispatch(to({ type: "UNAUTHENTICATING" }));
    // Empty out store on logout to avoid stale caches.
    dispatch(resetStoreAction);
    fetch(sessionUrl, {
      method: "DELETE",
      credentials: "include",
    })
      .then((res: Response) => res.json() as Promise<ServerResponse>)
      .then(handleSessionResponse(to, dispatch));
  };
}

export { signOut, signIn, init, sessionError };
