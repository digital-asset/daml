// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import createLedgerContext, {FetchResult, LedgerContext, QueryResult} from './createLedgerContext';
export {createLedgerContext, FetchResult, LedgerContext, QueryResult};

import { DamlLedger, useParty, useLedger, useQuery, useFetchByKey, useStreamQuery, useStreamFetchByKey, useReload } from "./defaultLedgerContext";
export { useParty, useLedger, useQuery, useFetchByKey, useStreamQuery, useStreamFetchByKey, useReload };
export default DamlLedger;
