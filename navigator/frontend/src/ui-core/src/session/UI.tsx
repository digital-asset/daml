// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import { Action } from "redux";
import styled from "styled-components";
import { Dispatch } from "../types";
import { sessionError, signIn } from "./actions";
import * as Session from "./index";

const SignInForm = styled.form`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
`;

const ErrorMessage = styled.p`
  color: red;
  font-weight: bold;
`;

const WarningMessage = styled.p`
  color: yellow;
  font-weight: bold;
`;

const Frame = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  height: 100%;
  width: 100%;
  background: ${({ theme }) => theme.documentBackground};
`;

const Above = styled.div`
  flex-grow: 1;
  width: 100%;
`;

const Below = styled.div`
  flex-grow: 3;
  width: 100%;
`;

const Logo = styled.img`
  max-height: 5rem;
  margin: 1rem;
`;

export interface OwnProps<A extends Action> {
  method: Session.AuthMethod;
  isAuthenticating: boolean;
  toSelf(action: Session.Action): A;
  failure?: Session.AuthFailure;
  logoUrl?: string;
}

export interface ReduxProps<A extends Action> {
  dispatch: Dispatch<A>;
}

export type Props<A extends Action> = OwnProps<A> & ReduxProps<A>;

export interface State {
  userId: string;
}

export default class Component<A extends Action> extends React.Component<
  Props<A>,
  State
> {
  constructor(props: Props<A>) {
    super(props);
    if (props.dispatch === undefined) {
      throw new Error("No dispatch function available to SignIn component");
    }
    this.state = { userId: "" };
    this.signIn = this.signIn.bind(this);
  }

  signIn(userId: Session.UserId): void {
    const { dispatch, toSelf } = this.props;
    if (!dispatch) {
      throw new Error("dispatch not available");
    }
    if (userId) {
      dispatch(signIn(toSelf, userId));
    }
  }

  render(): JSX.Element {
    const { isAuthenticating, method, failure } = this.props;
    let loginEl = null;
    let errorEl = null;
    if (failure === "invalid-credentials") {
      errorEl = (
        <ErrorMessage>
          <div>
            You don&apos;t have the necessary authorization to access the ledger
          </div>
          <div>
            Make sure to start the Navigator server with a valid access token
          </div>
        </ErrorMessage>
      );
    } else if (failure === "not-connected") {
      errorEl = (
        <WarningMessage>
          <div>Not yet connected to the ledger</div>
          <div>Verify that the ledger is available and try again</div>
        </WarningMessage>
      );
    } else if (failure === "unresponsive") {
      errorEl = (
        <WarningMessage>
          <div>Actor for party was unresponsive</div>
          <div>Try restarting Navigator</div>
        </WarningMessage>
      );
    } else if (failure === "unknown-error") {
      errorEl = (
        <ErrorMessage>
          <div>An error occurred when connecting to the ledger</div>
          <div>Refer to the Navigator server logs to know the cause</div>
        </ErrorMessage>
      );
    }

    switch (method.type) {
      case "select":
        loginEl = (
          <SignInForm>
            <select
              disabled={isAuthenticating}
              onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
                this.signIn(e.currentTarget.value);
              }}>
              <option value="" defaultValue="">
                Choose your role...
              </option>
              {method.users.map((id: Session.UserId, idx: number) => (
                <option key={idx} value={id}>
                  {id}
                </option>
              ))}
            </select>
            {errorEl}
          </SignInForm>
        );
        break;

      default: {
        const { dispatch } = this.props;
        if (!dispatch) {
          throw new Error("dispatch not available");
        }
        const to = this.props.toSelf;
        dispatch(sessionError(to, "Invalid sign-in method."));
      }
    }

    const logoEl = this.props.logoUrl ? (
      <Logo src={this.props.logoUrl} />
    ) : null;

    return (
      <Frame>
        <Above />
        {logoEl}
        {loginEl}
        <Below />
      </Frame>
    );
  }
}
