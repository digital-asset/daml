// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.navigator.console.commands

import com.digitalasset.navigator.console._

case object GraphQLSchema extends SimpleCommand {
  def name: String = "graphql_schema"

  def description: String = "Print the GraphQL schema"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    Right((state, state.graphQL.renderSchema))
  }
}
