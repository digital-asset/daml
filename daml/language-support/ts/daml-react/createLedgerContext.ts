// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import React, { useContext, useEffect, useMemo, useState } from "react";
import { ContractId, Party, Template } from "@daml/types";
import Ledger, {
  CreateEvent,
  Query,
  Stream,
  StreamCloseEvent,
  QueryResult,
  User,
} from "@daml/ledger";

export { QueryResult } from "@daml/ledger";

/**
 * @internal
 */
type DamlLedgerState = {
  reloadToken: unknown;
  triggerReload: () => void;
  user?: User;
  party: Party;
  ledger: Ledger;
};

/**
 * React props to initiate a connect to a Daml ledger.
 */
export type LedgerProps = {
  token: string;
  httpBaseUrl?: string;
  wsBaseUrl?: string;
  user?: User;
  party: Party;
  reconnectThreshold?: number;
};

/**
 * The result of a ``fetch`` against the ledger.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 */
export type FetchResult<T extends object, K, I extends string> = {
  /** Contracts of the given contract template and key. */
  contract: CreateEvent<T, K, I> | null;
  /** Indicator for whether the fetch is executing. */
  loading: boolean;
};

/**
 * The result of a streaming ``fetchByKeys`` against the ledger.
 *
 * @typeparam T The contract template type of the query.
 * @typeparam K The contract key type of the query.
 * @typeparam I The template id type.
 */
export type FetchByKeysResult<T extends object, K, I extends string> = {
  /** Contracts of the given contract template and key. */
  contracts: (CreateEvent<T, K, I> | null)[];
  /** Indicator for whether the fetch is executing. */
  loading: boolean;
};

/**
 * A LedgerContext is a React context that stores information about a Daml Ledger
 * and hooks necessary to use it.
 */
export type LedgerContext = {
  DamlLedger: React.FC<LedgerProps>;
  useParty: () => Party;
  useUser: () => User;
  useLedger: () => Ledger;
  useQuery: <T extends object, K, I extends string>(
    template: Template<T, K, I>,
    queryFactory?: () => Query<T>,
    queryDeps?: readonly unknown[],
  ) => QueryResult<T, K, I>;
  useFetch: <T extends object, K, I extends string>(
    template: Template<T, K, I>,
    contractId: ContractId<T>,
  ) => FetchResult<T, K, I>;
  useFetchByKey: <T extends object, K, I extends string>(
    template: Template<T, K, I>,
    keyFactory: () => K,
    keyDeps: readonly unknown[],
  ) => FetchResult<T, K, I>;
  useStreamQuery: <T extends object, K, I extends string>(
    template: Template<T, K, I>,
    queryFactory?: () => Query<T>,
    queryDeps?: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ) => QueryResult<T, K, I>;
  useStreamQueries: <T extends object, K, I extends string>(
    template: Template<T, K, I>,
    queryFactory?: () => Query<T>[],
    queryDeps?: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ) => QueryResult<T, K, I>;
  useStreamFetchByKey: <T extends object, K, I extends string>(
    template: Template<T, K, I>,
    keyFactory: () => K,
    keyDeps: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ) => FetchResult<T, K, I>;
  useStreamFetchByKeys: <T extends object, K, I extends string>(
    template: Template<T, K, I>,
    keyFactory: () => K[],
    keyDeps: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ) => FetchByKeysResult<T, K, I>;
  useReload: () => () => void;
};

/**
 * Create a [[LedgerContext]]. One should use this function, instead of the default [[DamlLedger]],
 * where one needs to be able to nest ledger interactions, by different parties or connections, within
 * one React application.
 *
 * @param contextName Used to refer to a context in case of errors.
 */
export function createLedgerContext(
  contextName = "DamlLedgerContext",
): LedgerContext {
  // NOTE(MH, useEffect dependencies): There are various places in this file
  // where we need to maintain the dependencies of the `useEffect` hook manually
  // and there's no tool to help us enfore they are correct. Thus, we need to be
  // extra careful in these locations. If we add too many dependencies, we will
  // make unnecessary network requests. If we forget adding some dependencies, we
  // not make a new network request although they are required to refresh data.

  const ledgerContext = React.createContext<DamlLedgerState | undefined>(
    undefined,
  );
  const DamlLedger: React.FC<LedgerProps> = ({
    token,
    httpBaseUrl,
    wsBaseUrl,
    reconnectThreshold,
    user,
    party,
    children,
  }) => {
    const [reloadToken, setReloadToken] = useState(0);
    const ledger = useMemo(
      () => new Ledger({ token, httpBaseUrl, wsBaseUrl, reconnectThreshold }),
      [token, httpBaseUrl, wsBaseUrl, reconnectThreshold],
    );
    const state: DamlLedgerState = useMemo(
      () => ({
        reloadToken,
        triggerReload: (): void => setReloadToken((x: number) => x + 1),
        user,
        party,
        ledger,
      }),
      [party, ledger, reloadToken],
    );
    return React.createElement(
      ledgerContext.Provider,
      { value: state },
      children,
    );
  };

  const useDamlState = (): DamlLedgerState => {
    const state = useContext(ledgerContext);
    if (!state) {
      throw Error(`Trying to use ${contextName} before initializing.`);
    }
    return state;
  };

  const useParty = (): Party => {
    const state = useDamlState();
    return state.party;
  };

  const useLedger = (): Ledger => {
    return useDamlState().ledger;
  };

  const useUser = (): User => {
    const user = useDamlState().user;
    if (!user) {
      throw Error(
        `Trying to use 'useUser' for a DamlLedger with a missing 'user' field.`,
      );
    } else return user;
  };

  function useQuery<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    queryFactory?: () => Query<T>,
    queryDeps?: readonly unknown[],
  ): QueryResult<T, K, I> {
    const state = useDamlState();
    const [result, setResult] = useState<QueryResult<T, K, I>>({
      contracts: [],
      loading: true,
    });
    useEffect(() => {
      setResult({ contracts: [], loading: true });
      const query = queryFactory ? queryFactory() : undefined;
      const load = async (): Promise<void> => {
        const contracts = await state.ledger.query(template, query);
        setResult({ contracts, loading: false });
      };
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      load();
      // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
    }, [state.ledger, state.reloadToken, template, ...(queryDeps ?? [])]);
    return result;
  }

  function useFetch<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    contractId: ContractId<T>,
  ): FetchResult<T, K, I> {
    const state = useDamlState();
    const [result, setResult] = useState<FetchResult<T, K, I>>({
      contract: null,
      loading: true,
    });
    useEffect(() => {
      setResult({ contract: null, loading: true });
      const load = async (): Promise<void> => {
        const contract = await state.ledger.fetch(template, contractId);
        setResult({ contract, loading: false });
      };
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      load();
      // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
    }, [state.ledger, state.reloadToken, template, contractId]);
    return result;
  }

  function useFetchByKey<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    keyFactory: () => K,
    keyDeps: readonly unknown[],
  ): FetchResult<T, K, I> {
    const state = useDamlState();
    const [result, setResult] = useState<FetchResult<T, K, I>>({
      contract: null,
      loading: true,
    });
    useEffect(() => {
      const key = keyFactory();
      setResult({ contract: null, loading: true });
      const load = async (): Promise<void> => {
        const contract = await state.ledger.fetchByKey(template, key);
        setResult({ contract, loading: false });
      };
      // eslint-disable-next-line @typescript-eslint/no-floating-promises
      load();
      // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
    }, [state.ledger, state.reloadToken, template, ...(keyDeps ?? [])]);
    return result;
  }

  // private
  interface StreamArgs<T extends object, K, I extends string, S, Result> {
    name: string;
    template: Template<T, K, I>;
    init: Result;
    mkStream: (state: DamlLedgerState) => [Stream<T, K, I, S>, object];
    setLoading: (r: Result, loading: boolean) => Result;
    setData: (r: Result, data: S) => Result;
    deps: readonly unknown[];
    closeHandler?: (e: StreamCloseEvent) => void;
  }
  function useStream<T extends object, K, I extends string, S, Result>({
    name,
    template,
    init,
    mkStream,
    setLoading,
    setData,
    deps,
    closeHandler,
  }: StreamArgs<T, K, I, S, Result>): Result {
    const [result, setResult] = useState<Result>(init);
    const state = useDamlState();
    useEffect(() => {
      setResult(init);
      const [stream, debugQuery] = mkStream(state);
      console.debug(`mount ${name}(${template.templateId}, ...)`, debugQuery);
      stream.on("live", () => setResult(result => setLoading(result, false)));
      stream.on("change", contracts =>
        setResult(result => setData(result, contracts)),
      );
      if (closeHandler) {
        stream.on("close", closeHandler);
      } else {
        stream.on("close", closeEvent => {
          if (closeEvent.code === 4000) {
            // deliberate call to .close, nothing to do
          } else {
            console.error(`${name}: WebSocket connection failed.`);
            setResult(result => setLoading(result, true));
          }
        });
      }
      return (): void => {
        console.debug(
          `unmount ${name}(${template.templateId}, ...)`,
          debugQuery,
        );
        stream.close();
      };
      // NOTE(MH): See note at the top of the file regarding "useEffect dependencies".
    }, [state.ledger, template, ...deps]);
    return result;
  }

  function useStreamQuery<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    queryFactory?: () => Query<T>,
    queryDeps?: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ): QueryResult<T, K, I> {
    return useStream<
      T,
      K,
      I,
      readonly CreateEvent<T, K, I>[],
      QueryResult<T, K, I>
    >({
      name: "useStreamQuery",
      template,
      init: { loading: true, contracts: [] },
      mkStream: state => {
        const query = queryFactory ? [queryFactory()] : [];
        const stream = state.ledger.streamQueries(template, query);
        return [stream, query];
      },
      setLoading: (r, b) => ({ ...r, loading: b }),
      setData: (r, d) => ({ ...r, contracts: d }),
      deps: queryDeps ?? [],
      closeHandler,
    });
  }

  function useStreamQueries<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    queryFactory?: () => Query<T>[],
    queryDeps?: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ): QueryResult<T, K, I> {
    return useStream<
      T,
      K,
      I,
      readonly CreateEvent<T, K, I>[],
      QueryResult<T, K, I>
    >({
      name: "useStreamQueries",
      template,
      init: { loading: true, contracts: [] },
      mkStream: state => {
        const query = queryFactory ? queryFactory() : [];
        const stream = state.ledger.streamQueries(template, query);
        return [stream, query];
      },
      setLoading: (r, b) => ({ ...r, loading: b }),
      setData: (r, d) => ({ ...r, contracts: d }),
      deps: queryDeps ?? [],
      closeHandler,
    });
  }

  function useStreamFetchByKey<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    keyFactory: () => K,
    keyDeps: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ): FetchResult<T, K, I> {
    return useStream<
      T,
      K,
      I,
      (CreateEvent<T, K, I> | null)[],
      FetchResult<T, K, I>
    >({
      name: "useStreamFetchByKey",
      template,
      init: { loading: true, contract: null },
      mkStream: state => {
        const key = keyFactory();
        const stream = state.ledger.streamFetchByKeys(template, [key]);
        return [stream, key as unknown as object];
      },
      setLoading: (r, b) => ({ ...r, loading: b }),
      setData: (r, d) => ({ ...r, contract: d[0] }),
      deps: keyDeps,
      closeHandler,
    });
  }

  function useStreamFetchByKeys<T extends object, K, I extends string>(
    template: Template<T, K, I>,
    keyFactory: () => K[],
    keyDeps: readonly unknown[],
    closeHandler?: (e: StreamCloseEvent) => void,
  ): FetchByKeysResult<T, K, I> {
    return useStream<
      T,
      K,
      I,
      (CreateEvent<T, K, I> | null)[],
      FetchByKeysResult<T, K, I>
    >({
      name: "useStreamFetchByKeys",
      template,
      init: { loading: true, contracts: [] },
      mkStream: state => {
        const keys = keyFactory();
        const stream = state.ledger.streamFetchByKeys(template, keys);
        return [stream, keys as unknown as object];
      },
      setLoading: (r, b) => ({ ...r, loading: b }),
      setData: (r, d) => ({ ...r, contracts: d }),
      deps: keyDeps,
      closeHandler,
    });
  }

  const useReload = (): (() => void) => {
    const state = useDamlState();
    return (): void => state.triggerReload();
  };

  return {
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
  };
}
