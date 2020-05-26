// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

export type TemplateId = string;

// The Connect type helps type connect functions that add various props to React
// components from the React context. Redux's `connect` and Apollo's `graphql`
// are example of this. It is parameterised by the props interface of what is
// added and what the result is (that is, the props the resulting component
// still needs).

export type Connect<Add, To> =
  (c: React.ComponentClass<To & Add> | React.StatelessComponent<To & Add>) =>
  React.ComponentClass<To>;

