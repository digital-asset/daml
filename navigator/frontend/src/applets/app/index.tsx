// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  defaultTheme,
  Dispatch,
  ThemeInterface,
  ThemeProvider,
} from "@da/ui-core";
import * as LedgerWatcher from "@da/ui-core/lib/ledger-watcher";
import * as Session from "@da/ui-core/lib/session";
import * as React from "react";
import { connect, ConnectedComponent } from "react-redux";
import { Action as ReduxAction } from "redux";
import { ThunkAction } from "redux-thunk";
import Frame from "../../components/Frame";
import {
  ConfigType,
  defaultConfig,
  EvalConfigCache,
  evalConfigCached,
  EvalConfigResult,
} from "../../config";
import * as Either from "../../config/either";
import logoUrl from "../../images/logo-large.png";
import * as ConfigSource from "../configsource";
import * as Page from "../page";

export type Action =
  | { type: "TO_SESSION"; action: Session.Action }
  | { type: "TO_PAGE"; action: Page.Action }
  | { type: "TO_WATCHER"; action: LedgerWatcher.Action }
  | { type: "TO_CONFIG"; action: ConfigSource.Action }
  | { type: "RESET_APP" };

export const toPage = (action: Page.Action): Action => ({
  type: "TO_PAGE",
  action,
});

export const toSession = (action: Session.Action): Action => ({
  type: "TO_SESSION",
  action,
});

export const toWatcher = (action: LedgerWatcher.Action): Action => ({
  type: "TO_WATCHER",
  action,
});

export const toConfig = (action: ConfigSource.Action): Action => ({
  type: "TO_CONFIG",
  action,
});

export const resetApp = (): Action => ({ type: "RESET_APP" });

export const initSession = (): ThunkAction<void, void, undefined, Action> =>
  Session.init(toSession);
export const initConfig = (): ThunkAction<void, State, undefined, Action> =>
  ConfigSource.reload(toConfig);

export interface State {
  session: Session.State;
  page: Page.State;
  watcher: LedgerWatcher.State;
  configSource: ConfigSource.State;
}

export function makeReducer() {
  return function reduce(state?: State, anyAction?: ReduxAction): State {
    if (state === undefined || anyAction === undefined) {
      return {
        session: Session.reduce(),
        page: Page.reduce(),
        watcher: LedgerWatcher.reduce(),
        configSource: ConfigSource.reduce(),
      };
    }
    // NOTE: Most actions will be known Actions, but there's nothing
    // stopping libraries or similar from sending unknown actions.
    // Thus, we may type cast to Action, but need to include a default
    // clause in the switch statement.
    const action = anyAction as Action;
    switch (action.type) {
      case "RESET_APP": {
        return {
          ...state,
          page: Page.reduce(),
          watcher: LedgerWatcher.reduce(),
        };
      }
      case "TO_SESSION":
        return {
          ...state,
          session: Session.reduce(state.session, action.action),
        };
      case "TO_PAGE":
        return {
          ...state,
          page: Page.reduce(state.page, action.action),
        };
      case "TO_WATCHER":
        return {
          ...state,
          watcher: LedgerWatcher.reduce(state.watcher, action.action),
        };
      case "TO_CONFIG":
        return {
          ...state,
          configSource: ConfigSource.reduce(state.configSource, action.action),
        };
      default:
        return state;
    }
  };
}

interface ReduxProps {
  state: State;
}
interface DispatchProps {
  dispatch: Dispatch<Action>;
}

type OwnProps = {};

type Props = ReduxProps & DispatchProps & OwnProps;

interface ComponentState {
  configCache: EvalConfigCache;
  config: EvalConfigResult;
  theme: ThemeInterface;
}

class SessionUI extends Session.UI<Action> {}

class Component extends React.Component<Props, ComponentState> {
  constructor(props: Props) {
    super(props);

    this.state = {
      configCache: {},
      ...this.computeStateFromSession(props),
    };
  }

  UNSAFE_componentWillReceiveProps(nextProps: Props) {
    // Fast skip if neither session nor config source have changed
    if (
      nextProps.state.session !== this.props.state.session ||
      nextProps.state.configSource !== this.props.state.configSource
    ) {
      this.setState<"config" | "theme">(
        this.computeStateFromSession(nextProps),
      );
    }
  }

  computeStateFromConfig(user: Session.User, configSource: ConfigSource.State) {
    const { configCache } = this.state;
    switch (configSource.result.type) {
      case "none":
        // No config available on server, or no config loaded yet. Use default config.
        return {
          config: defaultConfig(),
          theme: defaultTheme,
        };
      case "fetch-error":
        // Network error (other than 404).
        return {
          config: Either.left<Error, ConfigType>(
            new Error(configSource.result.error),
          ),
          theme: defaultTheme,
        };
      case "loaded": {
        // Got config source, try to parse and evaluate it (caching results)
        const source = configSource.result.source;
        const { result, cache: newCache } = evalConfigCached(
          user,
          source,
          configCache,
        );
        return {
          config: result,
          configCache: newCache,
          theme:
            result.type === "right"
              ? { ...defaultTheme, ...result.value.theme }
              : defaultTheme,
        };
      }
    }
  }

  computeStateFromSession(nextProps: Props) {
    const {
      state: { session, configSource },
    } = nextProps;
    switch (session.type) {
      case "loading":
        // Still loading session data, use default config.
        // Note: Can't evaluate config until the user is known.
        return {
          config: defaultConfig(),
          theme: defaultTheme,
        };
      case "required":
        // No user logged in yet, use default config.
        return {
          config: defaultConfig(),
          theme: this.computeStateFromConfig(
            { id: "", party: "", canAdvanceTime: false, role: "" },
            configSource,
          ).theme,
        };
      case "authenticated":
        // User available, try to evaluate the config.
        return this.computeStateFromConfig(session.user, configSource);
    }
  }

  render() {
    const { config, theme } = this.state;
    const {
      dispatch,
      state: { configSource, session, page, watcher },
    } = this.props;

    if (config.type === "left") {
      // Error in the config file, print it
      return <p>{config.value.message}</p>;
    } else if (configSource.loading || session.type === "loading") {
      // Still loading either session or config data
      return <p>LOADING</p>;
    } else if (session.type === "required") {
      return (
        <ThemeProvider theme={theme}>
          <SessionUI
            dispatch={dispatch}
            toSelf={toSession}
            method={session.method}
            isAuthenticating={session.isAuthenticating}
            failure={session.failure}
            logoUrl={logoUrl}
          />
        </ThemeProvider>
      );
    } else if (session.type === "authenticated") {
      return (
        <ThemeProvider theme={theme}>
          <Frame
            toConfig={toConfig}
            toSession={toSession}
            toPage={toPage}
            toWatcher={toWatcher}
            user={session.user}
            page={page}
            watcher={watcher}
            configSource={configSource}
            config={config.value}
          />
        </ThemeProvider>
      );
    } else {
      // TypeScript is not smart enough to realize that the above branches
      // are exhaustive. Need a dummy default branch.
      return <p>Unknown session or config type</p>;
    }
  }
}

export const UI: ConnectedComponent<typeof Component, OwnProps> = connect(
  state => ({ state }),
  dispatch => ({ dispatch }),
)(Component);
