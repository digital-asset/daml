// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export {
  createLedgerContext,
  FetchResult,
  LedgerContext,
  QueryResult,
  FetchByKeysResult,
} from "./createLedgerContext";

import {
  DamlLedger,
  useParty,
  useUser,
  useLedger,
  useQuery,
  useFetch,
  useFetchByKey,
  useStreamQuery,
  useStreamQueries,
  useStreamFetchByKey,
  useStreamFetchByKeys,
  useReload,
} from "./defaultLedgerContext";
export {
  useParty,
  useUser,
  useLedger,
  useQuery,
  useFetch,
  useFetchByKey,
  useStreamQuery,
  useStreamQueries,
  useStreamFetchByKey,
  useStreamFetchByKeys,
  useReload,
};
export default DamlLedger;
