// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from 'react';
import * as Redux from 'redux';


// ----------------------------------------------------------------------------
// Redux dispatching
// ----------------------------------------------------------------------------

// ThunkAction, as defined in redux-thunk
export type ThunkAction<R, S = {}, E = void> = (
  dispatch: Redux.Dispatch<S>,
  getState: () => S,
  extraArgument: E,
) => R;

// Dispatch, as defined in redux and redux-thunk
export interface Dispatch<S> {
  <A extends Redux.Action>(action: A): A;
  <R, E>(thunkAction: ThunkAction<R, S, E>): R;
}

// ----------------------------------------------------------------------------
// Types for higher order components
// ----------------------------------------------------------------------------

export type Diff<T, U> = T extends U ? never : T;
export type Omit<T, K extends keyof T> = Pick<T, Diff<keyof T, K>>;

/**
 * Given a component with properties P,
 * returns a component with properties ((P - Remove) & Add).
 *
 * Useful for higher order components that:
 * - inject some properties to the child,
 *   thus removing them from the wrapper (Remove)
 * - remove some properties from the child,
 *   thus adding them to the wrapper (Add)
 */
export type ComponentEnhancer<Remove, P extends Remove, Add = {}> =
  (
    component: React.ComponentType<P>,
  ) => React.ComponentType<Omit<P, keyof Remove> & Add>;

/** Properties that are injected by connect() */
export interface ReduxProps {
  // tslint:disable-next-line no-any
  dispatch: any;
}

/** Properties that are injected by graphql() */
export interface GraphQLProps {
  // tslint:disable-next-line no-any
  data?: any;
  // tslint:disable-next-line no-any
  mutate?: any;
};

export type WithRedux<P extends ReduxProps>
  = ComponentEnhancer<ReduxProps, P>;
export type WithGraphQL<P extends GraphQLProps>
  = ComponentEnhancer<GraphQLProps, P>;

// ----------------------------------------------------------------------------
// Types for higher order components
// ----------------------------------------------------------------------------

/**
 * The `Connect` type helps type connect functions that add various props to React
 * components from the React context. Redux's `connect` and Apollo's `graphql`
 * are example of this. It is parameterised by the props interface of what is
 * added and what the result is (that is, the props the resulting component
 * still needs).
 */
export type Connect<Add, To> = (c: React.ComponentType<To & Add>) => React.ComponentClass<To>;

/**
 * The `HOC` type describes a generic React higher order component. Use this instead of
 * the above `Connect` type if your higher order component also adds properties.
 */
export type HOC<From, To> = (c: React.ComponentType<From>) => React.ComponentType<To>;
