// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Action, State, User } from "./types";

export default (state?: State, action?: Action): State => {
  if (state === undefined || action === undefined) {
    return { type: "loading" };
  }

  // First handle actions that don't depend on previous state.
  switch (action.type) {
    case "AUTHENTICATED":
      return { type: "authenticated", user: action.user as User };
    case "UNAUTHENTICATING":
      return { type: "loading" };
    case "AUTHENTICATION_REQUIRED":
      return {
        type: "required",
        isAuthenticating: false,
        method: action.method,
        failure: action.failure,
      };
    case "AUTHENTICATING":
      if (state.type === "required") {
        // We're attempting authentication so reset any failures.
        return { ...state, failure: undefined, isAuthenticating: true };
      } else {
        return state;
      }
    case "ERROR":
      // FIXME: Don't ignore this.
      return state;
  }
};
