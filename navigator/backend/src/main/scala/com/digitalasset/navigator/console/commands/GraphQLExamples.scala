// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import com.daml.navigator.console._

case object GraphQLExamples extends SimpleCommand {
  private final val contractsQuery =
    """query ContractsQuery {
      |  contracts(
      |    filter: [],
      |    search: "",
      |    includeArchived: false,
      |    count: 10,
      |    sort: []
      |  ) {
      |    totalCount
      |    edges {
      |      node {
      |        __typename
      |        id
      |        ... on Contract {
      |          createTx { effectiveAt }
      |          activeAtOrArchiveTx {
      |            __typename
      |            ... on Node {
      |              id
      |            }
      |          }
      |          argument
      |          template {
      |            id
      |          }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin

  private final val templatesQuery =
    """query TemplatesQuery {
      |  templates(search: "", filter: [], count: 10, sort: []) {
      |    totalCount
      |    edges {
      |      node {
      |         __typename
      |        id
      |        ... on Template {
      |          topLevelDecl
      |          contracts { totalCount }
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin

  def makeOneLine(s: String): String =
    s.split('\n').map(line => line.trim).mkString(" ")

  def name: String = "graphql_examples"

  def description: String = "Print some example GraphQL queries"

  def params: List[Parameter] = List.empty

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    Right(
      (
        state,
        s"""
         |List some contracts:
         |  ${makeOneLine(contractsQuery)}
         |
         |List some templates:
         |  ${makeOneLine(templatesQuery)}
       """.stripMargin))
  }
}
