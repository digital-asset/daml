// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { AdvanceTime, Button, Dispatch, NavBar, ThunkAction } from '@da/ui-core';
import * as LedgerWatcher from '@da/ui-core/lib/ledger-watcher';
import * as Session from '@da/ui-core/lib/session';
import * as React from 'react';
import { connect } from 'react-redux';
import styled from 'styled-components';
import * as App from '../applets/app';
import logoUrl = require('../images/logo-large.png');
import { about } from '../routes';
import { Icon } from './Icon';
import Link from './Link';

function signOut(toSession: (action: Session.Action) => App.Action) {
  return Session.signOut(toSession, ((dispatch) => {
    dispatch({ type: 'APOLLO_STORE_RESET', observableQueryIds: [] });
    dispatch(App.resetApp());
  // Session.signOut signature can't handle ThunkAction
  // tslint:disable-next-line
  }) as ThunkAction<void, App.State> as any);
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
type Props = ReduxProps & OwnProps;

const InlineDiv = styled.div`
  display: inline;
`;

const LogoImg = styled.img`
  height: 100%;
`

const LogoLink = styled(Link)`
height: 100%;
`

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
  width: 1.0rem;
`;

const Logo = () => (
  <LogoLink
    route={about}
    params={{}}
  >
    <LogoImg
      src={logoUrl}
    />
  </LogoLink>
);

const Component = ({
  toSession,
  toWatcher,
  user,
  dispatch,
  watcher,
}: Props) => (
  <NavBar logo={<Logo/>}>
    <InlineDiv>
      <Button
        type="nav-transparent"
        onClick={() => { dispatch(signOut(toSession)); }}
      >
        <LeftIcon name="user" />
        {user.id}
        <RightIcon name="sign-out" />
      </Button>
    </InlineDiv>
    <SpacerFlex />
    <LedgerWatcher.UI
      watcher={watcher}
      toSelf={toWatcher}
    />
    <SpacerSmall/>
    {user.canAdvanceTime ? (<AdvanceTime/>) : null}
  </NavBar>
);

export default connect<void, ReduxProps, OwnProps>(
  null, (dispatch) => ({ dispatch }),
)(Component);
