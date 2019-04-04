// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import { Action } from 'redux';
import styled from 'styled-components';
import Button from '../Button';
import { Dispatch } from '../types';
import { sessionError, signIn } from './actions';
import * as Session from './index';

const SignInButton = styled(Button)`
  width: 100%;
  margin-top: 1rem;
`;

const Frame = styled.div`
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
  height: 100%;
  width: 100%;
  background: ${({theme}) => theme.documentBackground};
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

export interface ReduxProps<A> {
  dispatch: Dispatch<A>;
}

export type Props<A extends Action> = OwnProps<A> & ReduxProps<A>;

export interface State {
  userId: string;
  password: string;
}

export default class Component<A extends Action>
  extends React.Component<Props<A>, State> {

  constructor(props: Props<A>) {
    super(props);
    if (props.dispatch === undefined) {
      throw new Error('No dispatch function available to SignIn component');
    }
    this.state = { userId: '', password: '' };
    this.signIn = this.signIn.bind(this);
  }

  signIn(userId: Session.UserId, password?: string) {
    const { dispatch, toSelf } = this.props;
    if (!dispatch) { throw new Error('dispatch not available'); }
    if (userId) {
      dispatch(signIn(toSelf, userId, password));
    }
  }

  render() {
    const { isAuthenticating, method, failure } = this.props;
    const { userId, password } = this.state;
    let loginEl = null;
    let errorEl = null;
    if (failure === 'invalid-credentials') {
      errorEl = (
        <p>Sorry, those credentials are invalid</p>
      );
    }

    switch (method.type) {

      case 'password':
        loginEl = (
          <form>
            {errorEl}
            <input
              type="text"
              disabled={isAuthenticating}
              placeholder="Username"
              value={userId}
              onChange={(e: React.FormEvent<HTMLInputElement>) => {
                this.setState({ userId: e.currentTarget.value });
              }}
            />
            <input
              type="password"
              disabled={isAuthenticating}
              placeholder="Password"
              value={password}
              onChange={(e: React.FormEvent<HTMLInputElement>) => {
                this.setState({ password: e.currentTarget.value });
              }}
            />
            <SignInButton
              type="main"
              onClick={(e: React.MouseEvent<HTMLButtonElement>) => {
                e.preventDefault();
                e.stopPropagation();
                this.signIn(userId, password);
              }}
            >
              Sign in
            </SignInButton>
          </form>
        );
        break;

      case 'select':
        loginEl = (
          <select
            disabled={isAuthenticating}
            onChange={(e: React.ChangeEvent<HTMLSelectElement>) => {
              this.signIn(e.currentTarget.value);
            }}
          >
            <option value="" defaultValue="">Choose your role...</option>
            {method.users.map((id: Session.UserId, idx: number) =>
              (<option key={idx} value={id}>{id}</option>))}
          </select>
        );
        break;

      default:
        const { dispatch } = this.props;
        if (!dispatch) { throw new Error('dispatch not available'); }
        const to = this.props.toSelf;
        dispatch(sessionError(to, 'Invalid sign-in method.'));
    }

    const logoEl =
      this.props.logoUrl ? <Logo src={this.props.logoUrl} /> : null;

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

