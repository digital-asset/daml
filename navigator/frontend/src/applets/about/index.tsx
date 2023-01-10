// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Dispatch, styled } from "@da/ui-core";
import * as React from "react";
import { connect, ConnectedComponent } from "react-redux";
import { ThunkAction, ThunkDispatch } from "redux-thunk";
import * as App from "../app";

export type Action =
  | { type: "SET_BACKENDINFO_RESULT"; info: BackendVersionInfo }
  | { type: "SET_BACKENDINFO_FETCH_ERROR"; error: string }
  | { type: "SET_BACKENDINFO_LOADING" };

export const setBackendInfoResult = (info: BackendVersionInfo): Action => ({
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

export interface BackendVersionInfo {
  id: string;
  name: string;
  version: string;
}

export type BackendVersionInfoResult =
  | { type: "none" }
  | { type: "loading" }
  | { type: "loaded"; info: BackendVersionInfo }
  | { type: "fetch-error"; error: string };

export interface State {
  backendVersionInfo: BackendVersionInfoResult;
}

export type ToSelf = (
  action: Action | ThunkAction<void, App.State, undefined, Action>,
) => App.Action;

export function init(): State {
  return {
    backendVersionInfo: { type: "none" },
  };
}

export function reloadBackendInfo(
  toSelf: ToSelf,
): ThunkAction<void, App.State, undefined, App.Action> {
  return dispatch => {
    dispatch(toSelf(setBackendInfoLoading()));

    fetch("/api/about")
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

function handleBackendInfoResponse(to: ToSelf, dispatch: Dispatch<App.Action>) {
  return (source: BackendVersionInfo): void => {
    dispatch(to(setBackendInfoResult(source)));
  };
}

function handleBackendInfoFetchError(
  to: ToSelf,
  dispatch: Dispatch<App.Action>,
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
    return { backendVersionInfo: { type: "none" } };
  }

  switch (action.type) {
    case "SET_BACKENDINFO_RESULT":
      return {
        ...state,
        backendVersionInfo: { type: "loaded", info: action.info },
      };
    case "SET_BACKENDINFO_LOADING":
      return { ...state, backendVersionInfo: { type: "loading" } };
    case "SET_BACKENDINFO_FETCH_ERROR":
      return {
        ...state,
        backendVersionInfo: { type: "fetch-error", error: action.error },
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

const VSpace = styled.div`
  flex: 1;
`;

interface OwnProps {
  state: State;
  toSelf: ToSelf;
}
interface ReduxProps {
  dispatch: ThunkDispatch<App.State, undefined, App.Action>;
}

type Props = OwnProps & ReduxProps;

const BackendInfo: React.FC<{ info: BackendVersionInfoResult }> = ({
  info,
}) => {
  switch (info.type) {
    case "none":
      return <p />;
    case "loading":
      return <p>Loading...</p>;
    case "fetch-error":
      return <p>Error: {info.error}</p>;
    case "loaded":
      return (
        <p>
          Version: {info.info.version} <br />
          Application ID: {info.info.id}
        </p>
      );
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
    const { backendVersionInfo } = this.props.state;

    return (
      <Wrapper>
        <h1>Navigator</h1>
        <BackendInfo info={backendVersionInfo} />
        <VSpace />
        <div>
          <p>
            Copyright Notice Copyright (c) 2022 Digital Asset (Switzerland) GmbH
            and/or its affiliates. All rights reserved.
          </p>
        </div>
      </Wrapper>
    );
  }
}

export const UI: ConnectedComponent<typeof Component, OwnProps> =
  connect()(Component);
