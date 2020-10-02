// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import {
  DataColumnConfig,
  DataTable,
  Dispatch,

  WithGraphQL,
  WithRedux,
} from '@da/ui-core';
import { User } from '@da/ui-core/lib/session';
import * as React from 'react';
import {
  ApolloClient,
  graphql,
  withApollo,
} from 'react-apollo';
import { connect } from 'react-redux';
import { compose } from 'redux';
import { template as templateRoute } from '../../routes';
import { pathToAction } from '../../routes';
import * as App from '../app';
import columns from './columns';
import {
  dataToRows,
  makeQueryVariables,
  query,
  TableConfig,
  TableProps,
  Template,
} from './data';

export const INITIAL_FETCH_SIZE = 100;

export type State = TableConfig;

export const init = () => ({
  search: '',
  filter: [],
  count: INITIAL_FETCH_SIZE,
  sort: [],
});

export type Action
  = { type: 'SET_CONFIG', config: TableConfig };

const setConfig = (config: TableConfig): Action =>
  ({ type: 'SET_CONFIG', config });

export const reduce = (state?: State, action?: Action): State => {
  if (state === undefined || action === undefined) {
    return init();
  }
  switch (action.type) {
    case 'SET_CONFIG':
      return action.config;
  }
}


// GraphQL-enhanced data table
const withGraphql: WithGraphQL<TableProps>
  = graphql(query, { options: makeQueryVariables });
export const TemplateTable = withGraphql(DataTable);

interface ReduxProps {
  dispatch: Dispatch<App.Action>;
}

interface ApolloProps {
  client: ApolloClient;
}

interface OwnProps {
  state: State;
  user: User;
  columns?: DataColumnConfig<Template, {}>[];
  toSelf(action: Action): App.Action;
}

type Props = ReduxProps & ApolloProps & OwnProps;

class Component
  extends React.Component<Props, {}>
  {

  constructor(props: Props) {
    super(props);
    this.onConfigChange = this.onConfigChange.bind(this);
    this.onClick = this.onClick.bind(this);
  }

  onConfigChange(config: TableConfig) {
    const { dispatch, toSelf } = this.props;
    dispatch(toSelf(setConfig(config)));
  }

  onClick(template: Template) {
    const { dispatch } = this.props;
    dispatch(pathToAction(templateRoute.render({ id: template.id })));
  }

  render() {

    return (
      <TemplateTable
        title="Templates"
        config={this.props.state}
        extractRowData={dataToRows}
        hideActionRow={false}
        columns={this.props.columns || columns}
        onConfigChange={this.onConfigChange}
        onRowClick={this.onClick}
        rowClassName={() => 'ContractTable__row'}
        columnClassName="ContractTable__column"
        headerRowClassName="ContractTable__headerRow"
        headerColumnClassName="ContractTable__headerColumn"
      />
    );
  }
}

const withRedux: WithRedux<Props & ApolloProps> = connect();

export const UI: React.ComponentClass<OwnProps> = compose(
  withApollo,
  withRedux,
)(Component);
