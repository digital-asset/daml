// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Dispatch } from '@da/ui-core';
import { DamlLfValue } from '@da/ui-core/lib/api/DamlLfValue';
import * as LedgerWatcher from '@da/ui-core/lib/ledger-watcher';
import * as React from 'react';
import { gql, graphql, OptionProps, QueryProps as RAQueryProps } from 'react-apollo';
import { connect } from 'react-redux';
import { compose } from 'redux';
import {
  CreateContractVariables,
  TemplateInstance,
  TemplateInstance_node_Template,
  TemplateInstanceVariables,
} from '../../api/Queries';
import { pathToAction } from '../../routes';
import { contracts as dashboardRoute } from '../../routes';
import { Connect } from '../../types';
import * as App from '../app';
import TemplateComponent from './TemplateComponent';

export type Action
  = { type: 'SET_ERROR', error: string }
  | { type: 'SET_LOADING', isLoading: boolean }

export const setError = (error: string): Action =>
  ({ type: 'SET_ERROR', error });
export const setLoading = (isLoading: boolean): Action =>
  ({ type: 'SET_LOADING', isLoading });

export interface State {
  id: string;
  isLoading: boolean;
  error?: string;
}

export const init = (id: string): State => ({ id, isLoading: false });

export const reduce = (state: State, action: Action): State => {
  switch (action.type) {
    case 'SET_ERROR':
      return { ...state, error: action.error };
    case 'SET_LOADING':
      return { ...state, isLoading: action.isLoading };
  }
}

export type Template = TemplateInstance_node_Template;

interface OwnProps {
  state: State;
  toSelf(action: Action): App.Action;
  toWatcher(action: LedgerWatcher.Action): App.Action;
}

type ReduxProps = {
  dispatch: Dispatch<App.Action>;
}

interface MutationProps {
  //tslint:disable-next-line:no-any
  create(templateId: string, argument: DamlLfValue): Promise<any>;
}

interface QueryProps {
  data: RAQueryProps & TemplateInstance;
}

type Props = OwnProps & ReduxProps & MutationProps & QueryProps;

class Component extends React.Component<Props, {}> {

  constructor(props: Props) {
    super(props);
    this.create = this.create.bind(this);
  }

  create(e: React.MouseEvent<HTMLButtonElement>, argument?: DamlLfValue): void {
    e.preventDefault();
    const { create, toSelf, toWatcher, dispatch, state: { id } } = this.props;
    if (create && dispatch && argument) {
      dispatch(toSelf(setLoading(true)));
      create(id, argument)
        .then(({ data }) => {
          dispatch(toWatcher(LedgerWatcher.registerCommand(data.create)));
          dispatch(pathToAction(dashboardRoute.render({})));
        }).catch((error: Error) => {
          dispatch(toSelf(setLoading(false)));
          dispatch(toSelf(setError(error.message)));
        });
    }
  }

  render() {
    const { state, data } = this.props;
    if (data.loading) { return <p>Loading</p>; }
    else if (data.node === null) {
      return <p>Could not find template {state.id}</p>;
    } else {
      const template = data.node;

      return (
        <TemplateComponent
          template={template}
          error={state.error}
          isLoading={state.isLoading}
          onSubmit={this.create}
        />
      );
    }
  }
};

const query = gql`
  query TemplateInstance($templateId: ID!) {
    node(typename: "Template", id: $templateId) {
      ... on Template {
        id
        parameter
        topLevelDecl
      }
    }
  }
`;

const mutation = gql`
  mutation CreateContract($templateId: ID!, $argument: DamlLfValue) {
    create(templateId: $templateId, argument: $argument)
  }
`;

/**
 * We need to add 3 things to this component:
 * - dispatch so we can update our own Redux state
 * - a function to send exercise requests to the server via GraphQL
 * - the contract data fetched from the GraphQL API
 */

const withMutation: Connect<MutationProps, OwnProps> =
  graphql(mutation, {
    props: ({ mutate }: OptionProps<OwnProps, QueryProps>) => ({
      create: (templateId: string, argument: DamlLfValue) =>
        (mutate && mutate({ variables: { templateId, argument } as CreateContractVariables })),
    }),
  });

const withQuery: Connect<QueryProps, OwnProps & MutationProps> =
  graphql(query, {
    options: ({ state: { id } }: OwnProps & MutationProps) =>
      ({ variables: { templateId: id } as TemplateInstanceVariables }),
  });

const withRedux: Connect<ReduxProps, OwnProps & QueryProps & MutationProps> =
  connect();

export const UI = compose(
  withMutation,
  withQuery,
  withRedux,
)(Component);
