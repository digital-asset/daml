// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { Dispatch } from '@da/ui-core';
import { DamlLfValue } from '@da/ui-core/lib/api/DamlLfValue';
import * as LedgerWatcher from '@da/ui-core/lib/ledger-watcher';
import * as React from 'react';
import { gql, graphql } from 'react-apollo';
import { connect } from 'react-redux';
import { compose } from 'redux';
import {
  ContractDetailsById,
  ContractDetailsById_node_Contract,
  ContractDetailsByIdVariables,
  ContractExercise,
} from '../../api/Queries';
import { Connect } from '../../types';
import * as App from '../app';
import ContractComponent from './ContractComponent';

export type Action
  = { type: 'SET_CHOICE', choice?: string }
  | { type: 'SET_CHOICE_LOADING', choiceLoading: boolean }
  | { type: 'SET_ERROR', error: string }

export const setChoice = (choice?: string): Action =>
  ({ type: 'SET_CHOICE', choice });
export const setChoiceLoading = (choiceLoading: boolean): Action =>
  ({ type: 'SET_CHOICE_LOADING', choiceLoading });
export const setError = (error: string): Action =>
  ({ type: 'SET_ERROR', error });

export interface State {
  id: string;
  choice?: string;
  choiceLoading: boolean;
  error?: string;
}

export const init = (id: string, choice?: string): State =>
  ({ id, choice, choiceLoading: false });

export const reduce = (state: State, action: Action): State => {
  switch (action.type) {
    case 'SET_CHOICE': return { ...state, choice: action.choice };
    case 'SET_CHOICE_LOADING':
      return { ...state, choiceLoading: action.choiceLoading };
    case 'SET_ERROR':
      return { ...state, error: action.error };
  }
}

export type Contract = ContractDetailsById_node_Contract;

interface OwnProps {
  state: State;
  toSelf(action: Action): App.Action;
  toWatcher(action: LedgerWatcher.Action): App.Action;
}
interface ReduxProps {
  dispatch: Dispatch<App.Action>;
}
interface QueryProps {
  contract: Contract | null;
  isLoading: boolean;
}
interface MutationProps {
  exercise?(contractId: string, choiceId: string, argument?: DamlLfValue):
    //tslint:disable-next-line:no-any
    Promise<any>;
}

type Props = OwnProps & ReduxProps & QueryProps & MutationProps;

class Component extends React.Component<Props, {}> {

  constructor(props: Props) {
    super(props);
    this.exercise = this.exercise.bind(this);
    this.gotoParent = this.gotoParent.bind(this);
  }

  componentWillUnmount(): void { this.gotoParent = () => { ; }; }

  /**
   * This component deals with displaying a form for exercising a choice and
   * submitting that choice. At the moment, the component connects an
   * ApolloClient mutation function `exercise` that returns a promise. Things
   * get tricky when it comes to changing the view state when the exercise
   * mutation returns from the server, but only if the user hasn't already
   * navigated away from the view. It seems natural to say that we should manage
   * whether navigation is turned on or off from the `componentWillUnmount`
   * lifecycle method. Thus, that is what we do here (less straightforwardly
   * than if the Promise returned from Apollo Client was cancelable). There may
   * be a better way to achieve this, perhaps making more use of the Redux
   * state. Another wart is that async actions don't have access to the Apollo
   * Client by default so need to have that passed in. Overall, it feels like
   * there should be a neater way to organise this.
   */

  gotoParent(): void {
    // This function will navigate to the parent contract view by default, but
    // is replaced with a no-op on unmounting in order to not disrupt the user.
    // It is meant to be used in the asynchronous exercise function.
    const { toSelf, dispatch } = this.props;
    dispatch(toSelf(setChoice()));
  };

  exercise(e: React.MouseEvent<HTMLButtonElement>, argument?: DamlLfValue): void {
    e.preventDefault();
    const {
      exercise,
      toSelf,
      toWatcher,
      dispatch,
      state: { id, choice },
    } = this.props;
    if (exercise && dispatch && choice) {
      dispatch(toSelf(setChoiceLoading(true)));
      // We want to make sure we look up the function when this resolves, not
      // before then so can't just pass `this.gotoParent` directly to then.
      exercise(id, choice, argument)
        .then(({ data }) => {
          dispatch(toWatcher(LedgerWatcher.registerCommand(data.exercise)));
          this.gotoParent();
        }).catch((error: Error) => {
          dispatch(toSelf(setChoiceLoading(false)));
          dispatch(toSelf(setError(error.message)));
        });
    }
  }

  render() {
    const { state, contract, isLoading } = this.props;
    if (!contract || isLoading) { return <p>Loading</p>; }
    else {
      return (
        <ContractComponent
          contract={contract}
          choice={state.choice}
          choiceLoading={state.choiceLoading}
          error={state.error}
          exercise={this.exercise}
        />
      );
    }
  }
};

const query = gql`
  query ContractDetailsById($id: ID!) {
    node(id: $id, typename: "Contract") {
      ... on Contract {
        id argument
        archiveEvent {
          id
        }
        agreementText
        signatories
        observers
        key
        template {
          id topLevelDecl
          choices { name parameter }
        }
      }
    }
  }
`;

const mutation = gql`
  mutation ContractExercise($contractId: ID!, $choiceId: ID!, $argument: DamlLfValue) {
    exercise(contractId: $contractId, choiceId: $choiceId, argument: $argument)
  }
`;

/**
 * We need to add 3 things to this component:
 * - a function to send exercise requests to the server via GraphQL
 * - the contract data fetched from the GraphQL API
 * - dispatch so we can update our own Redux state
 */

// To manage the types here, we explicitly type the connect functions in order
// to make `compose` happy. The type annotations on `graphql` and `connect` are
// generally confusing to say the least, but works out with a bit of care and
// thinking about the ordering and what each connect function adds.

const withMutation: Connect<MutationProps, OwnProps> =
  graphql<ContractExercise, OwnProps>(mutation, {
    props: ({ mutate }) => ({
      exercise: (contractId: string, choiceId: string, argument?: DamlLfValue) =>
        (mutate && mutate({ variables: { contractId, choiceId, argument } })),
    }),
  });

const withQuery: Connect<QueryProps, OwnProps & MutationProps> =
  graphql<ContractDetailsById, OwnProps & MutationProps>(query, {
    props: ({ data }) => ({
      isLoading: data ? data.loading : false,
      contract: data ? data.node : null,
    }),
    options: ({ state: { id } }: OwnProps) => ({ variables: { id } as ContractDetailsByIdVariables}),
  });

const withRedux: Connect<ReduxProps, OwnProps & QueryProps & MutationProps> =
  connect();

export const UI = compose(
  withMutation,
  withQuery,
  withRedux,
)(Component);
