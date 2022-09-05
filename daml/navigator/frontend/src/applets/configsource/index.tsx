// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Button, styled } from "@da/ui-core";
import * as Session from "@da/ui-core/lib/session";
import * as React from "react";
import { connect, ConnectedComponent } from "react-redux";
import { ThunkAction, ThunkDispatch } from "redux-thunk";
import { configFileAPI, ConfigType, prettyPrintConfig } from "../../config";
import * as App from "../app";

export type Action =
  | { type: "SET_SOURCE"; source: string }
  | { type: "SET_FETCH_ERROR"; error: string }
  | { type: "SET_NO_CONFIG" }
  | { type: "SET_LOADING" };

export const setSource = (source: string): Action => ({
  type: "SET_SOURCE",
  source,
});
export const setLoading = (): Action => ({ type: "SET_LOADING" });
export const setFetchError = (error: string): Action => ({
  type: "SET_FETCH_ERROR",
  error,
});
export const setNoConfig = (): Action => ({ type: "SET_NO_CONFIG" });

export type StateResult =
  | { type: "none" }
  | { type: "loaded"; source: string }
  | { type: "fetch-error"; error: string };

export interface State {
  loading: boolean;
  result: StateResult;
}

export type ToSelf = (
  action: Action | ThunkAction<void, undefined, undefined, Action>,
) => App.Action;

export function init(): State {
  return {
    loading: false,
    result: { type: "none" },
  };
}

export function reload(
  toSelf: ToSelf,
): ThunkAction<void, App.State, undefined, App.Action> {
  return dispatch => {
    dispatch(toSelf(setLoading()));

    // Disable cache in order to facilitate live editing of the config file.
    const headers = new Headers();
    headers.append("pragma", "no-cache");
    headers.append("cache-control", "no-cache");
    fetch("/api/config", { cache: "no-cache", headers }).then(
      (res: Response) => {
        if (res.ok) {
          res
            .text()
            .then(handleResponse(toSelf, dispatch))
            .catch(handleFetchError(toSelf, dispatch));
        } else if (res.status === 404) {
          dispatch(toSelf(setNoConfig()));
        } else {
          handleFetchError(toSelf, dispatch);
        }
      },
    );
  };
}

function handleResponse(
  to: ToSelf,
  dispatch: ThunkDispatch<App.State, undefined, App.Action>,
) {
  return (source: string): void => {
    dispatch(to(setSource(source)));
  };
}

function handleFetchError(
  to: ToSelf,
  dispatch: ThunkDispatch<App.State, undefined, App.Action>,
) {
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  return (reason: any) => {
    if (reason instanceof Error) {
      // Log to console to show error call stack
      console.log(reason);
      dispatch(to(setFetchError(reason.message)));
    } else {
      dispatch(to(setFetchError(`${reason}`)));
    }
  };
}

export const reduce = (state?: State, action?: Action): State => {
  if (state === undefined || action === undefined) {
    // Return the initial state
    return { loading: false, result: { type: "none" } };
  }

  switch (action.type) {
    case "SET_SOURCE":
      return {
        loading: false,
        result: { type: "loaded", source: action.source },
      };
    case "SET_LOADING":
      return { ...state, loading: true };
    case "SET_FETCH_ERROR":
      return {
        loading: false,
        result: { type: "fetch-error", error: action.error },
      };
    case "SET_NO_CONFIG":
      return { loading: false, result: { type: "none" } };
  }
};

const Wrapper = styled.div`
  width: 100%;
  padding-left: 2.5rem;
  padding-right: 2.5rem;
`;

const TextArea = styled.textarea`
  display: block;
  resize: none;
  width: 100%;
  min-height: 150px;
`;

interface OwnProps {
  state: State;
  user: Session.User;
  config: ConfigType;
  toSelf: ToSelf;
}
interface ReduxProps {
  dispatch: ThunkDispatch<App.State, undefined, App.Action>;
}

type Props = OwnProps & ReduxProps;

class Component extends React.Component<Props, {}> {
  constructor(props: Props) {
    super(props);
  }

  render() {
    const { loading, result } = this.props.state;
    const { dispatch, config, toSelf } = this.props;

    const sourceEl = (res: StateResult) => {
      switch (res.type) {
        case "none":
          return (
            <ul>
              <li>
                No config file available. A default (empty) config will be used.
              </li>
              <li>
                The server should return the config at the following URL:{" "}
                <a href={"/api/config"}>/api/config</a>
              </li>
              <li>
                The server loads the config from the following file:
                frontend-config.js (same directory as ui-backend.conf).
              </li>
            </ul>
          );
        case "fetch-error":
          return <span>Network error: {res.error}</span>;
        case "loaded":
          return <TextArea value={res.source} readOnly={true} />;
      }
    };

    const configEl = (conf: ConfigType) => {
      const text = prettyPrintConfig(conf);
      switch (text.type) {
        case "left":
          return <span>{text.value.message}</span>;
        case "right":
          return <TextArea value={text.value} readOnly={true} />;
      }
    };

    return (
      <Wrapper>
        <p>Loading: {loading ? "true" : "false"}</p>
        <Button onClick={() => dispatch(reload(toSelf))}>Reload</Button>
        <h1>Config source</h1>
        {sourceEl(result)}
        <h1>Evaluated config</h1>
        {configEl(config)}
        <h1>Config file interface</h1>
        <TextArea value={configFileAPI} readOnly={true} />
      </Wrapper>
    );
  }
}

export const UI: ConnectedComponent<typeof Component, OwnProps> =
  connect()(Component);
