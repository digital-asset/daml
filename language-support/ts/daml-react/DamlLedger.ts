// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React, { useReducer, useMemo } from 'react';
import { DamlLedgerContext } from './context';
import * as LedgerStore from './ledgerStore';
import Ledger from '@daml/ledger';
import { Party } from '@daml/types';
import { reducer } from './reducer';

type Props = {
  token: string;
  httpBaseUrl?: string;
  wsBaseUrl?: string;
  party: Party;
}

const DamlLedger: React.FC<Props> = ({token, httpBaseUrl, wsBaseUrl, party, children}) => {
  const [store, dispatch] = useReducer(reducer, LedgerStore.empty());
  const state = useMemo(() => ({
    store,
    dispatch,
    party,
    ledger: new Ledger({token, httpBaseUrl, wsBaseUrl}),
  }), [party, token, httpBaseUrl, wsBaseUrl, store, dispatch]);
  return React.createElement(DamlLedgerContext.Provider, {value: state}, children);
}

export default DamlLedger;
