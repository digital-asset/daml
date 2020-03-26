// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Polyfills for older browsers (IE11)
import './polyfills';

// Load global styles
import './styles/base.css';

import * as Router from '@da/redux-router';
import { defaultTheme, ThemeProvider } from '@da/ui-core';
import * as introspectionQueryResultData from '@da/ui-core/lib/api/fragmentTypes.json';
import { IntrospectionResultData } from 'apollo-client/data/fragmentMatcher'
import * as React from 'react';
import {
  ApolloClient,
  ApolloProvider,
  createNetworkInterface,
  IntrospectionFragmentMatcher,
} from 'react-apollo';
import * as ReactDOM from 'react-dom';
import { applyMiddleware, compose, createStore, Store } from 'redux';
import ReduxThunk from 'redux-thunk';
import * as App from './applets/app';
import { pathToAction, stateToPath } from './routes';

const networkInterface = createNetworkInterface({
  uri: '/api/graphql',
  // Include credentials (cookies) with GraphQL queries
  opts: { credentials: 'same-origin' },
});

export function dataIdFromObject(result: { id: string, __typename: string }) {
  if (result.id && result.__typename) {
    return `${result.__typename}:${result.id}`;
  } else {
    return undefined;
  }
}

const client = new ApolloClient({
  networkInterface,
  dataIdFromObject,

  // The default fragment matcher does not work with interfaces and unions.
  // The apollo docs suggest using the introspection fragment matcher
  // initialized with a schema dump:
  // https://www.apollographql.com/docs/react/advanced/fragments.html#fragment-matcher

  fragmentMatcher: new IntrospectionFragmentMatcher({
    // TODO: The IntrospectionResultData type has a slightly wrong type definition inside apollo,
    // work around this with an explicit type cast.
    introspectionQueryResultData: introspectionQueryResultData as IntrospectionResultData,
  }),
});

// Set up a function to compose Redux enhancers such that Redux DevTools
// understand them.
declare const window: { __REDUX_DEVTOOLS_EXTENSION_COMPOSE__?: Function };
const composeEnhancers =
  typeof window === 'object' && window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__
    ? window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__({})
    : compose;

// Create Redux store
const store: Store<App.State> = createStore<App.State>(
  App.makeReducer(client),
  composeEnhancers(applyMiddleware(
    client.middleware(),
    Router.middleware({ stateToUrl: stateToPath, urlToAction: pathToAction }),
    ReduxThunk,
  )),
);

ReactDOM.render((
    <ApolloProvider store={store} client={client}>
      <ThemeProvider theme={defaultTheme}>
        <App.UI />
      </ThemeProvider>
    </ApolloProvider>
  ),
  document.getElementById('app'),
);

store.dispatch(App.initSession());
store.dispatch(App.initConfig());
