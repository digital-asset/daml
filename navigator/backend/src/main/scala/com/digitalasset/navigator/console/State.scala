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

final case class State(
    terminal: Terminal,
    reader: LineReader,
    history: History,
    /** Set to true to request an application shutdown */
    quit: Boolean,
    /** Set to true to request a rebuild of the LineReader (updates help text and tab completion) */
    rebuildLineReader: Boolean,
    /** Current party */
    party: ApiTypes.Party,
    /** CLI arguments used to start this application */
    arguments: Arguments,
    /** Contains per-party state (including contract store) */
    config: Config,
    /** Main actor for the interaction with the ledger API */
    store: ActorRef,
    /** Execution context of the console */
    ec: ExecutionContext,
    /** Handles GraphQL queries */
    graphQL: GraphQLHandler,
    /** Application name and version */
    applicationInfo: ApplicationInfo
) {
  def getPartyState: Option[PartyState] = config.parties.find(p => p.name == party)
}
