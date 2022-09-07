// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  createLedgerContext,
  FetchResult,
  QueryResult,
  LedgerProps,
  FetchByKeysResult,
} from "./createLedgerContext";
import { ContractId, Party, Template, TemplateOrInterface } from "@daml/types";
import Ledger, { Query, StreamCloseEvent, User } from "@daml/ledger";

/**
 * @internal
 */
const ledgerContext = createLedgerContext();

/**
 * Within a `DamlLedger` one can use the hooks provided here.
 *
 * @param props React props and children for this element.
 */
export function DamlLedger(
  props: React.PropsWithChildren<LedgerProps>,
): React.ReactElement | null {
  return ledgerContext.DamlLedger(props);
}

/**
 * React hook to get the party currently connected to the ledger.
 */
export function useParty(): Party {
  return ledgerContext.useParty();
}

/**
 * React hook to get the user currently connected to the ledger participant.
 */
export function useUser(): User {
  return ledgerContext.useUser();
}

/**
 * React Hook that returns the Ledger instance to interact with the connected Daml ledger.
 */
export function useLedger(): Ledger {
  return ledgerContext.useLedger();
}

/**
 * React Hook for a ``query`` against the ledger.
 *
 * @typeparam T The contract template or interface type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The contract template or interface to filter for.
 * @param queryFactory A function returning a query. If the query is omitted, all visible contracts of the given template are returned.
 * @param queryDeps The dependencies of the query (which trigger a reload when changed).
 *
 * @return The result of the query.
 */
export function useQuery<T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
  queryFactory: () => Query<T>,
  queryDeps: readonly unknown[],
): QueryResult<T, K, I>;
export function useQuery<T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
): QueryResult<T, K, I>;
export function useQuery<T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
  queryFactory?: () => Query<T>,
  queryDeps?: readonly unknown[],
): QueryResult<T, K, I> {
  return ledgerContext.useQuery(template, queryFactory, queryDeps);
}

/**
 * React Hook for a lookup by contractId against the `/v1/fetch` endpoint of the JSON API.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contract to fetch.
 * @param contractId The contractId to fetch.
 *
 * @return The fetched contract.
 */
export function useFetch<T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
  contractId: ContractId<T>,
): FetchResult<T, K, I> {
  return ledgerContext.useFetch(template, contractId);
}

/**
 * React Hook for a lookup by key against the `/v1/fetch` endpoint of the JSON API.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to fetch.
 * @param keyFactory A function returning the contract key of the contracts to fetch.
 * @param keyDeps Dependencies of this hook (for which the fetch is reexecuted on change).
 *
 * @return The fetched contract.
 */
export function useFetchByKey<T extends object, K, I extends string>(
  template: Template<T, K, I>,
  keyFactory: () => K,
  keyDeps: readonly unknown[],
): FetchResult<T, K, I> {
  return ledgerContext.useFetchByKey(template, keyFactory, keyDeps);
}

/**
 * React Hook to query the ledger, the returned result is updated as the ledger state changes.
 *
 * @deprecated prefer useStreamQueries
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to match.
 * @param queryFactory A function returning a query. If the query is omitted, all visible contracts of the given template are returned.
 * @param queryDeps The dependencies of the query (for which a change triggers an update of the result).
 * @param closeHandler A callback that will be called if the underlying WebSocket connection fails in an unrecoverable way.
 *
 * @return The matching contracts.
 */
export function useStreamQuery<T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
  queryFactory?: () => Query<T>,
  queryDeps?: readonly unknown[],
  closeHandler?: (e: StreamCloseEvent) => void,
): QueryResult<T, K, I> {
  return ledgerContext.useStreamQuery(
    template,
    queryFactory,
    queryDeps,
    closeHandler,
  );
}

/**
 * React Hook to query the ledger, the returned result is updated as the ledger state changes.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to match.
 * @param queryFactory A function returning an array of queries. If no queryFactory is given, or if the given factory returns an empty array, all visible contracts of the given template are returned. Otherwise, returns a union of all the contracts that match at least one of the given queries.
 * @param queryDeps The dependencies of the query (for which a change triggers an update of the result).
 * @param closeHandler A callback that will be called if the underlying WebSocket connection fails in an unrecoverable way.
 *
 * @return The matching contracts.
 */
export function useStreamQueries<T extends object, K, I extends string>(
  template: TemplateOrInterface<T, K, I>,
  queryFactory?: () => Query<T>[],
  queryDeps?: readonly unknown[],
  closeHandler?: (e: StreamCloseEvent) => void,
): QueryResult<T, K, I> {
  return ledgerContext.useStreamQueries(
    template,
    queryFactory,
    queryDeps,
    closeHandler,
  );
}

/**
 * React Hook to query the ledger. Same as useStreamQuery, but query by contract key instead.
 *
 * @deprecated prefer useStreamFetchByKeys
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to match.
 * @param keyFactory A function returning a contract key. Contract
 *        keys must be in "output" format as defined in the [JSON API
 *        docs](https://docs.daml.com/json-api/lf-value-specification.html),
 *        i.e., have to match the types generated by the daml-types library.
 * @param keyDeps The dependencies of the fetch-by-key (for which a change triggers an update of the result).
 * @param closeHandler A callback that will be called if the underlying WebSocket connection fails in an unrecoverable way.
 *
 * @return The matching (unique) contract, or null.
 */
export function useStreamFetchByKey<T extends object, K, I extends string>(
  template: Template<T, K, I>,
  keyFactory: () => K,
  keyDeps: readonly unknown[],
  closeHandler?: (e: StreamCloseEvent) => void,
): FetchResult<T, K, I> {
  return ledgerContext.useStreamFetchByKey(
    template,
    keyFactory,
    keyDeps,
    closeHandler,
  );
}

/**
 * React Hook to query the ledger. Same as useStreamQueries, but query by contract keys instead.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 *
 * @param template The template of the contracts to match.
 * @param keyFactory A function returning an array of contract keys. Contract
 *        keys must be in "output" format as defined in the [JSON API
 *        docs](https://docs.daml.com/json-api/lf-value-specification.html),
 *        i.e., have to match the types generated by the daml-types library.
 * @param keyDeps The dependencies of the fetch-by-key (for which a change triggers an update of the result).
 * @param closeHandler A callback that will be called if the underlying WebSocket connection fails in an unrecoverable way.
 *
 * @return An array of the same length as the given array of keys, where each
 *         element is either the currently active contract that matches the
 *         corresponding key, or null if no active contract matches the key in
 *         the same position.
 */
export function useStreamFetchByKeys<T extends object, K, I extends string>(
  template: Template<T, K, I>,
  keyFactory: () => K[],
  keyDeps: readonly unknown[],
  closeHandler?: (e: StreamCloseEvent) => void,
): FetchByKeysResult<T, K, I> {
  return ledgerContext.useStreamFetchByKeys(
    template,
    keyFactory,
    keyDeps,
    closeHandler,
  );
}

/**
 * React Hook to reload all active queries.
 */
export function useReload(): () => void {
  return ledgerContext.useReload();
}
