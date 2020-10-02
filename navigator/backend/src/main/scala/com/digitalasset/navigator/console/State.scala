// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console

import akka.actor.ActorRef
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.{ApplicationInfo, GraphQLHandler}
import com.daml.navigator.config.{Arguments, Config}
import com.daml.navigator.model.PartyState
import org.jline.reader.{History, LineReader}
import org.jline.terminal.Terminal

import scala.concurrent.ExecutionContext

/**
  *
  * @param quit Set to true to request an application shutdown
  * @param rebuildLineReader Set to true to request a rebuild of the LineReader (updates help text and tab completion)
  * @param party Current party
  * @param arguments CLI arguments used to start this application
  * @param config Contains per-party state (including contract store)
  * @param store Main actor for the interaction with the ledger API
  * @param ec Execution context of the console
  * @param graphQL Handles GraphQL queries
  * @param applicationInfo Application name and version
  */
final case class State(
    terminal: Terminal,
    reader: LineReader,
    history: History,
    quit: Boolean,
    rebuildLineReader: Boolean,
    party: ApiTypes.Party,
    arguments: Arguments,
    config: Config,
    store: ActorRef,
    ec: ExecutionContext,
    graphQL: GraphQLHandler,
    applicationInfo: ApplicationInfo
) {
  def getPartyState: Option[PartyState] = config.parties.find(p => p.name == party)
}
