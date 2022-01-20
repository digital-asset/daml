// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import ReactDOM from 'react-dom';
import 'semantic-ui-css/semantic.min.css';
import './index.css';
import App from './components/App';
import { authConfig } from './config';
import { Auth0Provider } from "@auth0/auth0-react";

if (authConfig.provider !== "auth0") {
  ReactDOM.render(<App />, document.getElementById('root'));
} else {
  ReactDOM.render(
  <Auth0Provider domain={authConfig.domain}
                 clientId={authConfig.clientId}
                 redirectUri={window.location.origin}>
    <App />
  </Auth0Provider>,
  document.getElementById('root'));
}
