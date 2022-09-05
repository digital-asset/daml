// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Dispatch, styled } from "@da/ui-core";
import * as React from "react";
import { connect, ConnectedComponent } from "react-redux";
import { AnyAction } from "redux";
import { ThunkAction, ThunkDispatch } from "redux-thunk";
import * as App from "../app";

// The backend returns an opaque JSON object
export type Info = Record<string, unknown>;

export type Action =
  | { type: "SET_BACKENDINFO_RESULT"; info: Info }
  | { type: "SET_BACKENDINFO_FETCH_ERROR"; error: string }
  | { type: "SET_BACKENDINFO_LOADING" };

export const setBackendInfoResult = (info: Info): Action => ({
  type: "SET_BACKENDINFO_RESULT",
  info,
});
export const setBackendInfoLoading = (): Action => ({
  type: "SET_BACKENDINFO_LOADING",
});
export const setBackendInfoFetchError = (error: string): Action => ({
  type: "SET_BACKENDINFO_FETCH_ERROR",
  error,
});

export type BackendInfoResult =
  | { type: "none" }
  | { type: "loading" }
  | { type: "loaded"; info: Info }
  | { type: "fetch-error"; error: string };

export interface State {
  backendInfo: BackendInfoResult;
}

export type ToSelf = (
  action: Action | ThunkAction<void, App.State, undefined, Action>,
) => App.Action;

export function init(): State {
  return {
    backendInfo: { type: "none" },
  };
}

export function reloadBackendInfo(
  toSelf: ToSelf,
): ThunkAction<void, App.State, undefined, AnyAction> {
  return dispatch => {
    dispatch(toSelf(setBackendInfoLoading()));

    fetch("/api/info")
      .then((res: Response) => {
        if (res.ok) {
          res
            .json()
            .then(handleBackendInfoResponse(toSelf, dispatch))
            .catch(handleBackendInfoFetchError(toSelf, dispatch));
        } else {
          handleBackendInfoFetchError(toSelf, dispatch)(res.statusText);
        }
      })
      .catch(handleBackendInfoFetchError(toSelf, dispatch));
  };
}

function handleBackendInfoResponse(
  to: ToSelf,
  dispatch: ThunkDispatch<App.State, undefined, App.Action>,
) {
  return (source: Record<string, unknown>): void => {
    dispatch(to(setBackendInfoResult(source)));
  };
}

function handleBackendInfoFetchError(
  to: ToSelf,
  dispatch: ThunkDispatch<App.State, undefined, App.Action>,
) {
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  return (reason: any) => {
    if (reason instanceof Error) {
      // Log to console to show error call stack
      console.log(reason);
      dispatch(to(setBackendInfoFetchError(reason.message)));
    } else {
      dispatch(to(setBackendInfoFetchError(`${reason}`)));
    }
  };
}

export const reduce = (state?: State, action?: Action): State => {
  if (state === undefined || action === undefined) {
    // Return the initial state
    return { backendInfo: { type: "none" } };
  }

  switch (action.type) {
    case "SET_BACKENDINFO_RESULT":
      return { ...state, backendInfo: { type: "loaded", info: action.info } };
    case "SET_BACKENDINFO_LOADING":
      return { ...state, backendInfo: { type: "loading" } };
    case "SET_BACKENDINFO_FETCH_ERROR":
      return {
        ...state,
        backendInfo: { type: "fetch-error", error: action.error },
      };
  }
};

const Wrapper = styled.div`
  width: 100%;
  padding-left: 2.5rem;
  padding-right: 2.5rem;
  display: flex;
  flex-direction: column;
`;

interface OwnProps {
  state: State;
  toSelf: ToSelf;
}
interface ReduxProps {
  dispatch: Dispatch<App.Action>;
}

type Props = OwnProps & ReduxProps;

const renderBackendInfo = (info: BackendInfoResult) => {
  switch (info.type) {
    case "none":
      return "";
    case "loading":
      return "loading...";
    case "fetch-error":
      return info.error;
    case "loaded":
      return JSON.stringify(info.info, undefined, "  ");
  }
};

class Component extends React.Component<Props, {}> {
  constructor(props: Props) {
    super(props);
  }

  componentDidMount() {
    this.props.dispatch(reloadBackendInfo(this.props.toSelf));
  }

  render() {
    const { backendInfo } = this.props.state;

    return (
      <Wrapper>
        <h1>Navigator debug information</h1>
        <p>
          <pre>{renderBackendInfo(backendInfo)}</pre>
        </p>
      </Wrapper>
    );
  }
}

export const UI: ConnectedComponent<typeof Component, OwnProps> =
  connect()(Component);
