// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  ApolloClient,
  ObservableQuery,
  ObservableSubscription,
} from "@apollo/client";
import { DocumentNode } from "graphql";
import {
  ContractsResult,
  ContractTableConfig,
  DataProvider,
  ResultCallback,
} from "./";

export class ApolloDataProvider<C extends ContractTableConfig>
  implements DataProvider<C>
{
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client: ApolloClient<any>;
  query: DocumentNode;
  observableQuery: ObservableQuery<{}>;
  querySubscription?: ObservableSubscription;
  createVariables: (config: ContractTableConfig) => {};
  dataToContractsResult: (data: {}) => ContractsResult;

  constructor(
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    client: ApolloClient<any>,
    query: DocumentNode,
    createVariables: (config: ContractTableConfig) => {},
    dataToContractsResult: (data: {}) => ContractsResult,
  ) {
    this.client = client;
    this.query = query;
    this.createVariables = createVariables;
    this.dataToContractsResult = dataToContractsResult;
    this.fetchData = this.fetchData.bind(this);
    this.startCacheWatcher = this.startCacheWatcher.bind(this);
    this.stopCacheWatcher = this.stopCacheWatcher.bind(this);
  }

  fetchData(config: ContractTableConfig, onResult: ResultCallback): void {
    this.client
      .query<{}>({
        query: this.query,
        variables: this.createVariables(config),
        fetchPolicy: "network-only",
      })
      .then(({ data }) => {
        onResult(this.dataToContractsResult(data));
      })
      .catch(err => {
        console.error("Error fetching contract archiving updates:", err);
      });
  }

  startCacheWatcher(
    config: ContractTableConfig,
    onResult: ResultCallback,
  ): void {
    this.stopCacheWatcher();
    this.observableQuery = this.client.watchQuery<{}>({
      fetchPolicy: "cache-only",
      query: this.query,
      variables: this.createVariables(config),
    });
    const next = () => {
      onResult(
        this.dataToContractsResult(
          this.observableQuery.getCurrentResult().data,
        ),
      );
    };
    this.querySubscription = this.observableQuery.subscribe({ next });
  }

  stopCacheWatcher(): void {
    if (this.querySubscription) {
      this.querySubscription.unsubscribe();
      this.querySubscription = undefined;
    }
  }
}
