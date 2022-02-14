// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ApolloClient } from "@apollo/client";
import { withApollo } from "@apollo/client/react/hoc";
import * as React from "react";
import { connect } from "react-redux";
import { AnyAction } from "redux";
import { Set } from "typescript-collections";
import Watcher, { Action, MAX_COMMAND_AGE, State, WatchedCommand } from ".";
import styled from "../theme";
import { Dispatch } from "../types";
import { createIcon, fadeTime } from "./icons";

// TODO: with ES6, just pass an array to the Set constructor.
export function setOf<E>(xs: E[]): Set<E> {
  const set: Set<E> = new Set<E>();
  xs.forEach(x => {
    set.add(x);
  });
  return set;
}

// ----------------------------------------------------------------------------
// Styling
// ----------------------------------------------------------------------------

const Container = styled.div`
  display: flex;
  align-items: center;
`;

// ----------------------------------------------------------------------------
// Component
// ----------------------------------------------------------------------------

export interface OwnProps<A extends Action> {
  watcher: State;
  toSelf(action: Action): A;
}

export interface ApolloProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client: ApolloClient<any>;
}

export interface ReduxProps<A extends Action> {
  dispatch: Dispatch<A>;
}

export type Props<A extends Action> = OwnProps<A> & ApolloProps & ReduxProps<A>;

interface ComponentState {
  oldCommands: WatchedCommand[];
}

// Note: processed commands are kept by LedgerWatcher for a short time.
// After that, they are removed from LedgerWatcher, but kept in the oldCommands
// property of this components state, so that we can draw them fading out.
// After the fade out time has passed, they can be removed from the oldCommands
// list as well, to prevent drawing an increasing number of invisible icons.
function canRemove(cmd: WatchedCommand, now: number) {
  // eslint-disable-next-line @typescript-eslint/restrict-plus-operands
  const maxTime = MAX_COMMAND_AGE + fadeTime;
  return cmd.result && now - cmd.result.processedAt.getTime() > maxTime;
}

class Component<A extends Action> extends React.Component<
  Props<A>,
  ComponentState
> {
  private watcher: Watcher;

  constructor(props: Props<A>) {
    super(props);

    this.watcher = new Watcher(
      this.props.client,
      () => this.props.watcher,
      (action: Action) => this.props.dispatch(this.props.toSelf(action)),
    );

    this.state = {
      oldCommands: [],
    };
  }

  UNSAFE_componentWillReceiveProps(nextProps: Props<A>) {
    const nextCommands = nextProps.watcher.commands;
    const commands = this.props.watcher.commands;
    const oldCommands = this.state.oldCommands;

    if (nextCommands === commands) {
      return;
    }

    // Note: Is it really worth constructing a set instead of using an
    // O(N) array lookup? There won't be many concurrent outstanding commands.
    const nextIds = setOf(nextCommands.map(cmd => cmd.commandId));
    const oldIds = setOf(oldCommands.map(cmd => cmd.commandId));
    const now = Date.now();
    this.setState({
      oldCommands: [
        ...this.state.oldCommands,
        ...commands.filter(
          cmd =>
            !nextIds.contains(cmd.commandId) && !oldIds.contains(cmd.commandId),
        ),
      ].filter(cmd => !canRemove(cmd, now)),
    });
  }

  componentDidMount() {
    this.watcher.start();
  }

  componentWillUnmount() {
    this.watcher.stop();
  }

  render() {
    const { commands } = this.props.watcher;
    const { oldCommands } = this.state;
    const icons: JSX.Element[] = [
      ...oldCommands.map(cmd => createIcon(cmd, true)),
      ...commands.map(cmd => createIcon(cmd, false)),
    ];
    return <Container>{icons}</Container>;
  }
}

export const UI: React.ComponentClass<OwnProps<AnyAction>> = withApollo<
  OwnProps<AnyAction>
>(connect()(Component));
