// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export type UserId = string;
export type Party = string;
export type Role = string;
export interface User {
  id: UserId;
  party: Party;
  canAdvanceTime: boolean;
  role?: Role;
}

export type ServerResponse = SignInResponse | SessionResponse;
export type SessionResponse = { type: "session"; user: User };
export type SignInResponse = {
  type: "sign-in";
  method: AuthMethod;
  error?: "invalid-credentials";
};

export type State =
  | { type: "loading" }
  | {
      type: "required";
      method: AuthMethod;
      isAuthenticating: boolean;
      failure?: AuthFailure;
    }
  | { type: "authenticated"; user: User };

export type AuthMethod = AuthMethodSelect;
export type AuthMethodSelect = { type: "select"; users: UserId[] };
export type AuthFailure = "invalid-credentials";

export type Action =
  | {
      // Tell session state that user has authenticated.
      type: "AUTHENTICATED";
      user: User;
    }
  | {
      // Tell session state that authentication is being checked.
      type: "AUTHENTICATING";
    }
  | {
      // Tell session state that we are in the process of unauthenticating.
      type: "UNAUTHENTICATING";
    }
  | {
      // Tell session state that authentication is required.
      type: "AUTHENTICATION_REQUIRED";
      method: AuthMethod;
      failure?: AuthFailure;
    }
  | {
      // Tell session state that something went wrong.
      type: "ERROR";
      error: string;
    };
