// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Polyfills for older browsers (IE11)
import "./polyfills";

// Load global styles
import "./styles/base.css";

import {
  ApolloClient,
  ApolloProvider,
  createHttpLink,
  InMemoryCache,
} from "@apollo/client";
import * as Router from "@da/redux-router";
import { defaultTheme, ThemeProvider } from "@da/ui-core";
import possibleTypes from "@da/ui-core/lib/api/possibleTypes.json";
import * as React from "react";
import * as ReactDOM from "react-dom";
import { Provider } from "react-redux";
import { applyMiddleware, compose, createStore, Store } from "redux";
import ReduxThunk, { ThunkDispatch } from "redux-thunk";
import * as App from "./applets/app";
import { pathToAction, stateToPath } from "./routes";

const link = createHttpLink({
  uri: "/api/graphql",
  // Include credentials (cookies) with GraphQL queries
  credentials: "same-origin",
});

const dataIdFromObject = (result: { id: string; __typename: string }) => {
  if (result.id && result.__typename) {
    return `${result.__typename}:${result.id}`;
  } else {
    return undefined;
  }
};

const client = new ApolloClient({
  link,
  cache: new InMemoryCache({ dataIdFromObject, possibleTypes }),
});

// Set up a function to compose Redux enhancers such that Redux DevTools
// understand them.
declare global {
  interface Window {
    // eslint-disable-next-line @typescript-eslint/ban-types
    __REDUX_DEVTOOLS_EXTENSION_COMPOSE__?: Function;
  }
}
const composeEnhancers =
  typeof window === "object" && window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__
    ? window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__({})
    : compose;

// Create Redux store
const store: Store<App.State> & {
  dispatch: ThunkDispatch<App.State, undefined, App.Action>;
} = createStore<App.State, App.Action, undefined, undefined>(
  App.makeReducer(),
  composeEnhancers(
    applyMiddleware(
      Router.middleware({ stateToUrl: stateToPath, urlToAction: pathToAction }),
      ReduxThunk,
    ),
  ),
);

ReactDOM.render(
  <Provider store={store}>
    <ApolloProvider client={client}>
      <ThemeProvider theme={defaultTheme}>
        <App.UI />
      </ThemeProvider>
    </ApolloProvider>
  </Provider>,
  document.getElementById("app"),
);

// Dispatch an action for the initial URL so that the reducer can set the
// initial state correctly.
store.dispatch(pathToAction(window.location.pathname));
store.dispatch(App.initSession());
store.dispatch(App.initConfig());
