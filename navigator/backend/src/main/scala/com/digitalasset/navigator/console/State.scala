// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console

import java.util.concurrent.TimeUnit

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.{ApplicationInfo, GraphQLHandler}
import com.daml.navigator.config.{Arguments, Config}
import com.daml.navigator.model.PartyState
import com.daml.navigator.store.Store
import com.daml.navigator.store.Store.{
  ApplicationStateInfo,
  GetApplicationStateInfo,
  PartyActorRunning
}
import org.jline.reader.{History, LineReader}
import org.jline.terminal.Terminal

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

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
  def getParties: Option[Map[String, PartyState]] = {
    implicit val actorTimeout: Timeout = Timeout(5, TimeUnit.SECONDS)
    Await.result((store ? GetApplicationStateInfo).mapTo[ApplicationStateInfo], 10.seconds) match {
      case Store.ApplicationStateConnecting(_, _, _, _) =>
        None
      case Store.ApplicationStateConnected(_, _, _, _, _, _, partyActors) =>
        Some(partyActors.collect {
          case (str, PartyActorRunning(info)) => (str, info.state)
        })
      case Store.ApplicationStateFailed(_, _, _, _, _) => None
    }
  }
  def getPartyState: Option[PartyState] = {
    getParties.flatMap { parties =>
      parties.values.find(s => s.name == party)
    }
  }
}
