// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
import { ApolloClient } from "@apollo/client";
import { withApollo } from "@apollo/client/react/hoc";
import {
  ApolloDataProvider,
  ContractColumn,
  ContractTable,
  ContractTableConfig,
  Dispatch,
} from "@da/ui-core";
import { User } from "@da/ui-core/lib/session";
import * as React from "react";
import { connect } from "react-redux";
import { contract as contractRoute } from "../../routes";
import { pathToAction } from "../../routes";
import * as App from "../app";
import columns from "./columns";
import { Contract, dataToRows, makeQueryVariables, query } from "./data";

export const INITIAL_FETCH_SIZE = 100;

export type State = ContractTableConfig;

export const init = (): State => ({
  search: "",
  filter: [],
  includeArchived: false,
  count: INITIAL_FETCH_SIZE,
  sort: [],
  isFrozen: false,
});

export type Action = { type: "SET_CONFIG"; config: ContractTableConfig };

const setConfig = (config: ContractTableConfig): Action => ({
  type: "SET_CONFIG",
  config,
});

export const reduce = (state?: State, action?: Action): State => {
  if (state === undefined || action === undefined) {
    return init();
  }
  switch (action.type) {
    case "SET_CONFIG":
      return action.config;
  }
};

interface ReduxProps {
  dispatch: Dispatch<App.Action>;
}

interface ApolloProps {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  client: ApolloClient<any>;
}

interface OwnProps {
  state: State;
  user: User;
  columns?: ContractColumn<Contract>[];
  toSelf(action: Action): App.Action;
}

type Props = ReduxProps & ApolloProps & OwnProps;

class Component extends React.Component<Props, {}> {
  private dataProvider: ApolloDataProvider<ContractTableConfig>;

  constructor(props: Props) {
    super(props);
    this.onConfigChange = this.onConfigChange.bind(this);
    this.onClick = this.onClick.bind(this);
    this.dataProvider = new ApolloDataProvider(
      props.client,
      query,
      makeQueryVariables,
      dataToRows,
    );
  }

  onConfigChange(config: ContractTableConfig) {
    const { dispatch, toSelf } = this.props;
    dispatch(toSelf(setConfig(config)));
  }

  onClick(contract: Contract) {
    const { dispatch } = this.props;
    dispatch(
      pathToAction(
        contractRoute.render({ id: encodeURIComponent(contract.id) }),
      ),
    );
  }

  render() {
    return (
      <ContractTable
        title="Contracts"
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

export const UI: React.ComponentClass<OwnProps> = withApollo<OwnProps>(
  connect()(Component),
);
