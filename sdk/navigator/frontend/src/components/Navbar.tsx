// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ApolloClient } from "@apollo/client";
import { withApollo } from "@apollo/client/react/hoc";
import { AdvanceTime, Button, Dispatch, NavBar } from "@da/ui-core";
import * as LedgerWatcher from "@da/ui-core/lib/ledger-watcher";
import * as Session from "@da/ui-core/lib/session";
import * as React from "react";
import { connect, ConnectedComponent } from "react-redux";
import { AnyAction } from "redux";
import { ThunkAction } from "redux-thunk";
import styled from "styled-components";
import * as App from "../applets/app";
import logoUrl from "../images/logo-large.png";
import { about } from "../routes";
import { Icon } from "./Icon";
import Link from "./Link";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function signOut<S>(
  client: ApolloClient<any>,
  toSession: (action: Session.Action) => App.Action,
) {
  return Session.signOut<S, App.Action>(toSession, ((
    dispatch: Dispatch<App.Action>,
  ) => {
    client.resetStore();
    dispatch(App.resetApp());
    // Session.signOut signature can't handle ThunkAction
    // eslint-disable-next-line
  }) as ThunkAction<void, App.State, undefined, AnyAction> as any);
}

interface ReduxProps {
  dispatch: Dispatch<App.Action>;
}
interface OwnProps {
  toSession(action: Session.Action): App.Action;
  toWatcher(action: LedgerWatcher.Action): App.Action;
  user: Session.User;
  watcher: LedgerWatcher.State;
}
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Props = ReduxProps & OwnProps & { client: ApolloClient<any> };

const InlineDiv = styled.div`
  display: inline;
`;

const LogoImg = styled.img`
  height: 100%;
`;

const LogoLink = styled(Link)`
  height: 100%;
`;

const RightIcon = styled(Icon)`
  margin-left: 0.5rem;
  font-size: 1rem;
`;

const LeftIcon = styled(Icon)`
  margin-right: 0.5rem;
  font-size: 1.4rem;
`;

const SpacerFlex = styled.div`
  flex: 1;
`;

const SpacerSmall = styled.div`
  width: 1rem;
`;

const Logo = () => (
  <LogoLink route={about} params={{}}>
    <LogoImg src={logoUrl} />
  </LogoLink>
);

const Component = ({
  toSession,
  toWatcher,
  user,
  dispatch,
  watcher,
  client,
}: Props) => (
  <NavBar logo={<Logo />}>
    <InlineDiv>
      <Button
        type="nav-transparent"
        onClick={() => {
          dispatch(signOut(client, toSession));
        }}>
        <LeftIcon name="user" />
        {user.id}
        <RightIcon name="sign-out" />
      </Button>
    </InlineDiv>
    <SpacerFlex />
    <LedgerWatcher.UI watcher={watcher} toSelf={toWatcher} />
    <SpacerSmall />
    {user.canAdvanceTime ? <AdvanceTime /> : null}
  </NavBar>
);

const C: ConnectedComponent<
  React.ComponentClass<OwnProps & ReduxProps>,
  OwnProps
> = connect()(withApollo<OwnProps & ReduxProps>(Component));
export default C;
