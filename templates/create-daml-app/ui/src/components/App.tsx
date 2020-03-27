import React from 'react';
import LoginScreen from './LoginScreen';
import MainScreen from './MainScreen';
import DamlLedger from '@daml/react';
import Credentials from '../Credentials';
import { httpBaseUrl, wsBaseUrl } from '../config';

/**
 * React component for the entry point into the application.
 */
const App: React.FC = () => {
  const [credentials, setCredentials] = React.useState<Credentials | undefined>();

  return credentials
    ? <DamlLedger
        token={credentials.token}
        party={credentials.party}
        httpBaseUrl={httpBaseUrl}
        wsBaseUrl={wsBaseUrl}
      >
        <MainScreen onLogout={() => setCredentials(undefined)}/>
      </DamlLedger>
    : <LoginScreen onLogin={setCredentials} />
}

export default App;
