// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { DocumentNode } from 'graphql';
import {
  ApolloClient,
  ObservableQuery,
  Subscription,
} from 'react-apollo';
import {
  ContractsResult,
  ContractTableConfig,
  DataProvider,
  ResultCallback,
} from './';

export class ApolloDataProvider<C extends ContractTableConfig>
  implements DataProvider<C> {

  client: ApolloClient;
  query: DocumentNode;
  observableQuery: ObservableQuery<{}>;
  querySubscription?: Subscription;
  createVariables: (config: ContractTableConfig) => {};
  dataToContractsResult: (data: {}) => ContractsResult;

  constructor(
    client: ApolloClient,
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

  fetchData(config: ContractTableConfig, onResult: ResultCallback) {
    this.client.query<{}>({
      query: this.query,
      variables: this.createVariables(config),
      fetchPolicy: 'network-only',
    }).then(({ data }) => {
      onResult(this.dataToContractsResult(data));
    }).catch((err) => {
      console.error('Error fetching contract archiving updates:', err);
    });
  }

  startCacheWatcher(config: ContractTableConfig, onResult: ResultCallback) {
    this.stopCacheWatcher();
    this.observableQuery = this.client.watchQuery<{}>({
      fetchPolicy: 'cache-only',
      query: this.query,
      variables: this.createVariables(config),
    });
    const next = () => {
      onResult(this.dataToContractsResult(
        this.observableQuery.currentResult().data));
    };
    this.querySubscription = this.observableQuery.subscribe({ next });
  }

  stopCacheWatcher() {
    if (this.querySubscription) {
      this.querySubscription.unsubscribe();
      this.querySubscription = undefined;
    }
  }
}
