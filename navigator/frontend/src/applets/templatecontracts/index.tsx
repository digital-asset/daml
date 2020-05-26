// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  ApolloDataProvider,
  ContractColumn,
  ContractTable,
  ContractTableConfig,
  Dispatch,
  WithGraphQL,
  WithRedux,
} from '@da/ui-core';
import { User } from '@da/ui-core/lib/session';
import * as React from 'react';
import { ApolloClient, graphql, withApollo } from 'react-apollo';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { contract as contractRoute } from '../../routes';
import { pathToAction } from '../../routes';
import * as App from '../app';
import makeColumns from './columns';
import {
  Contract,
  dataToRows,
  makeParamQueryVariables,
  makeQueryVariables,
  paramQuery,
  ParamQueryData,
  query,
} from './data';

export const INITIAL_FETCH_SIZE = 100;

export interface TableConfig extends ContractTableConfig {
  id: string;
}

export type State = TableConfig;

export const init = (id: string) => ({
  search: '',
  filter: [],
  includeArchived: true,
  count: INITIAL_FETCH_SIZE,
  sort: [],
  isFrozen: false,
  id,
});

export type Action
  = { type: 'SET_CONFIG', config: TableConfig };

const setConfig = (config: TableConfig): Action =>
  ({ type: 'SET_CONFIG', config });

export const reduce = (_: State, action: Action): State => {
  switch (action.type) {
    case 'SET_CONFIG':
      return action.config;
  }
}

interface ReduxProps {
  dispatch: Dispatch<App.Action>;
}

interface ApolloProps {
  client: ApolloClient;
}

interface GraphQLProps {
  data: ParamQueryData;
}

interface OwnProps {
  state: State;
  user: User;
  columns?: ContractColumn<Contract>[];
  toSelf(action: Action): App.Action;
}

type Props = GraphQLProps & ReduxProps & ApolloProps & OwnProps;

class Component extends React.Component<Props, {}> {

  private dataProvider: ApolloDataProvider<TableConfig>;

  constructor(props: Props) {
    super(props);
    this.onConfigChange = this.onConfigChange.bind(this);
    this.onClick = this.onClick.bind(this);
    this.dataProvider = new ApolloDataProvider(
      props.client, query, makeQueryVariables, dataToRows);
  }

  onConfigChange(config: TableConfig) {
    const { dispatch, toSelf } = this.props;
    dispatch(toSelf(setConfig(config)));
  }

  onClick(contract: Contract) {
    const { dispatch } = this.props;
    dispatch(pathToAction(contractRoute.render({ id: encodeURIComponent(contract.id) })));
  }

  render() {
    const { data } = this.props;
    const columns =
      data && data.node && data.node.__typename === 'Template' && data.node.parameterDef.dataType.type === 'record' ?
      makeColumns(data.node.parameterDef.dataType) : [];
    return (
      <ContractTable
        title={`Contracts for [${this.props.state.id}]`}
        dataProvider={this.dataProvider}
        config={this.props.state}
        hideActionRow={false}
        columns={this.props.columns || columns}
        onConfigChange={this.onConfigChange}
        onContractClick={this.onClick}
        rowClassName="ContractTable__row"
        columnClassName="ContractTable__column"
        headerRowClassName="ContractTable__headerRow"
        headerColumnClassName="ContractTable__headerColumn"
        archivedRowClassName="ContractTable__archived"
        createdRowClassName="ContractTable__created"
        removedRowClassName="ContractTable__removed"
      />
    );
  }
}

const withRedux: WithRedux<Props> = connect();
const withGraphQL: WithGraphQL<Props>
  = graphql(paramQuery, { options: (s) => makeParamQueryVariables(s) });

export const UI: React.ComponentClass<OwnProps> = compose(
  withApollo,
  withRedux,
  withGraphQL,
)(Component);
