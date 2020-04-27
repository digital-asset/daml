// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import Ledger from '@daml/ledger';
import { Party } from '@daml/types';

export type DamlLedgerState = {
  reloadToken: unknown;
  triggerReload: () => void;
  party: Party;
  ledger: Ledger;
}

export const DamlLedgerContext = React.createContext(null as DamlLedgerState | null);
