// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import { ApolloQueryResult } from 'apollo-client';
import { PureQueryOptions } from 'apollo-client/core/types';
import * as React from 'react';
import { gql, graphql } from 'react-apollo';
import { compose } from 'redux';
import { DamlLfValue } from '../api/DamlLfValue';
import * as DamlLfValueF from '../api/DamlLfValue';
import { WithExerciseQuery, WithExerciseQueryVariables } from '../api/Queries';
import { HOC } from '../types';

// ------------------------------------------------------------------------------------------------
// Types
// ------------------------------------------------------------------------------------------------

export type BusinessIntent = string;

interface MutationProps {
  exercise?(
    contractId: string,
    choiceId: string,
    argument?: DamlLfValue,
  ): Promise<ApolloQueryResult<WithExerciseQuery>>;
}

export interface ResultProps {
  exercise: BusinessIntent;
}

/**
 * The result of the exercise command.
 * - initial: No exercise command sent
 * - loading: Exercise command sent, no response yet
 * - success: Exercise successfully *submitted*
 * - error: Exercise immediately rejected
 */
export type Result
  = {type: 'initial', id: undefined}
  | {type: 'loading', id: string}
  | {type: 'success', id: string, businessIntent: string}
  | {type: 'error', id: string, error: string}
  ;

export interface State {
  /** Result of the last exercise call */
  lastResult: Result;
}

export interface Props {
  onExerciseError?(error: Error): void;
  onExerciseDone?(data: ResultProps): void;
}

export interface ExerciseProps {
  lastExerciseResult: Result;
  exercise(
    id: string,
    choice: string,
    argument?: DamlLfValue,
    affectsContractId?: string,
  ): void;
}

// ------------------------------------------------------------------------------------------------
// GraphQL query
// ------------------------------------------------------------------------------------------------

const mutation = gql`
  mutation WithExerciseQuery($contractId: ID!, $choiceId: ID!, $argument: DamlLfValue) {
    exercise(contractId: $contractId, choiceId: $choiceId, argument: $argument)
  }
`;


// ------------------------------------------------------------------------------------------------
// HOC
// ------------------------------------------------------------------------------------------------

/**
 * A higher order component that adds a `exercise` property. The value of the
 * property is a function that can be called to exercise a choice on a contract.
 *
 *
 * @param refetchQueries A function that returns a list of queries that should
 * be refetched once the exercise command is successfully *submitted*. Note: in a
 * distributed ledger, the command is processed with a delay. Use polling if you need
 * to refetch queries after an exercise was *processed* on the ledger.
 */
export default function withExercise<P extends Props>(
  refetchQueries?: (props: P) => string[] | PureQueryOptions[],
) : HOC<P & ExerciseProps, P> {

  // Step 1: Stateful component with a simple 'exercise' method
  type PExerciseIn = P & ExerciseProps;
  type PExerciseOut = P & MutationProps;

  const withExerciseComponent: HOC<PExerciseIn, PExerciseOut> =
  (InputComponent: React.ComponentType<PExerciseIn>) =>
  class WithExercise extends React.Component<PExerciseOut, State> {
    private _isMounted = false;

    constructor(props: PExerciseOut) {
      super(props);
      this.exercise = this.exercise.bind(this);
      this.state = {
        lastResult: {
          type: 'initial',
          id: undefined,
        },
      };
    }

    componentDidMount() {
      this._isMounted = true;
    }

    componentWillUnmount() {
      this._isMounted = false;
    }

    exercise(
      id: string,
      choice: string,
      argument?: DamlLfValue,
    ): void {
      const {
        exercise,
        onExerciseDone,
        onExerciseError,
      } = this.props;
      if (exercise) {
        this.setState({lastResult: {type: 'loading', id}});

        exercise(id, choice, argument || DamlLfValueF.unit())
          .then(({ data }) => {
            if (onExerciseDone) { onExerciseDone(data) }
            if (this._isMounted && this.state.lastResult.id === id) {
              this.setState({lastResult: {type: 'success', id, businessIntent: data.exercise}});
            }
          }).catch((error: Error) => {
            console.error(error);
            if (onExerciseError) { onExerciseError(error) }
            if (this._isMounted && this.state.lastResult.id === id) {
              this.setState({lastResult: {type: 'error', id, error: error.message}});
            }
          });
      }
    }

    render() {
      return (
        <InputComponent
          {...this.props}
          lastExerciseResult={this.state.lastResult}
          exercise={this.exercise}
        />
      )
    }
  }

  // Step 2: Adding the graphql mutation
  type PMutationIn = P & MutationProps;
  type PMutationOut = P;

  const withMutation: HOC<PMutationIn, PMutationOut> =
  graphql<ResultProps, P>(mutation, {
    props: ({ mutate }) => ({
      exercise: (contractId: string, choiceId: string, argument?: DamlLfValue) =>
        (mutate && mutate({ variables: { contractId, choiceId, argument } as WithExerciseQueryVariables })),
    }),
    options: (props) => ({
      refetchQueries: refetchQueries ? refetchQueries(props) : [],
    }),
  });

  // Compose
  const result = compose(withMutation, withExerciseComponent);

  return result;
}
