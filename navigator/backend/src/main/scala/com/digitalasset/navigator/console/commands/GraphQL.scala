// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.Try

case object GraphQL extends SimpleCommand {
  def name: String = "graphql"

  def description: String = "Execute a GraphQL query"

  def params: List[Parameter] = List(
    ParameterGraphQL("query", "The GraphQL query")
  )

  def createQuery(query: String) =
    // Note: the JSON object may contain optional 'variables' and 'operationName' properties.
    // The 'variables' property is useful for parametrized queries
    // The 'operationName' property is only useful if the query contains multiple operations
    s"""{"query": "${query.replaceAll("\"", """\\"""")}"}"""

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    // TODO: Use a jline3 parser that parses parameters according to their type
    val query = args.mkString(" ")
    implicit val executionContext: ExecutionContext = state.ec
    for {
      ps <- state.getPartyState ~> s"Unknown party ${state.party}"
      parsed <- state.graphQL.parse(createQuery(query)) ~> "Failed to parse query"
      future <- Try(state.graphQL.executeQuery(parsed, ps)) ~> "Failed to execute query"
      result <- Try(Await.result(future, 30.seconds)) ~> "Failed to execute query"
    } yield {
      (state, result._2.prettyPrint)
    }
  }
}
