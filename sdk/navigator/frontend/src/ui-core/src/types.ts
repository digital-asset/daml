// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

import * as React from "react";
import * as Redux from "redux";
import { ThunkDispatch } from "redux-thunk";

// ----------------------------------------------------------------------------
// Redux dispatching
// ----------------------------------------------------------------------------

// Dispatch, as defined in redux and redux-thunk
export type Dispatch<A extends Redux.Action, S = {}, E = void> = ThunkDispatch<
  S,
  E,
  A
>;

// ----------------------------------------------------------------------------
// Types for higher order components
// ----------------------------------------------------------------------------

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
export type ComponentEnhancer<Remove, P extends Remove, Add = {}> = (
  component: React.ComponentType<P>,
) => React.ComponentType<Omit<P, keyof Remove> & Add>;

/** Properties that are injected by connect() */
export interface ReduxProps {
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  dispatch: any;
}

/** Properties that are injected by graphql() */
export interface GraphQLProps {
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  data?: any;
  // eslint-disable-next-line  @typescript-eslint/no-explicit-any
  mutate?: any;
}

export type WithGraphQL<P extends GraphQLProps> = ComponentEnhancer<
  GraphQLProps,
  P
>;
