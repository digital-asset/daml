// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.console.commands

import java.util.concurrent.TimeUnit

import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.console._
import com.daml.navigator.store.Store._
import com.daml.navigator.time.TimeProviderType
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

case object Info extends SimpleCommand {
  def name: String = "info"

  def description: String = "Print debug information"

  def params: List[Parameter] = List.empty

  def prettyPartyInfo(partyInfo: PartyActorInfo): PrettyNode = partyInfo match {
    case info: PartyActorStarting => PrettyPrimitive("Actor starting")
    case info: PartyActorStarted => PrettyPrimitive("Actor running")
    case info: PartyActorFailed => PrettyPrimitive(s"Actor failed: ${info.error.getMessage}")
    case info: PartyActorUnresponsive => PrettyPrimitive(s"Actor unresponsive")
  }

  def prettyGeneralInfo(info: ApplicationStateInfo): PrettyNode = PrettyObject(
    PrettyField("Ledger host", info.platformHost),
    PrettyField("Ledger port", info.platformPort.toString),
    PrettyField("Secure connection", info.tls.toString),
    PrettyField("Application ID", info.applicationId)
  )

  def prettyLocalDataInfo(state: State): PrettyNode = PrettyObject(
    state.config.parties.toList.map(
      ps =>
        PrettyField(
          ApiTypes.Party.unwrap(ps.name),
          PrettyObject(
            PrettyField("Packages", ps.packageRegistry.packageCount.toString),
            PrettyField("Contracts", ps.ledger.allContractsCount.toString),
            PrettyField("Active contracts", ps.ledger.activeContractsCount.toString),
            PrettyField(
              "Last transaction",
              ps.ledger
                .latestTransaction(ps.packageRegistry)
                .map(t => ApiTypes.TransactionId.unwrap(t.id))
                .getOrElse("???"))
          )
      ))
  )

  def prettyInfo(applicationInfo: ApplicationStateInfo, state: State): PrettyObject =
    applicationInfo match {
      case info: ApplicationStateConnected =>
        PrettyObject(
          PrettyField("General info", prettyGeneralInfo(info)),
          PrettyField(
            "Ledger info",
            PrettyObject(
              PrettyField("Connection status", "Connected"),
              PrettyField("Ledger ID", info.ledgerId),
              PrettyField("Ledger time", Pretty.prettyInstant(info.ledgerTime.time.getCurrentTime)),
              PrettyField("Ledger time type", TimeProviderType.write(info.ledgerTime.`type`))
            )
          ),
          PrettyField(
            "Akka system",
            PrettyObject(
              info.partyActors
                .map(p => PrettyField(ApiTypes.Party.unwrap(p.party), prettyPartyInfo(p)))
            )),
          PrettyField("Local data", prettyLocalDataInfo(state))
        )
      case info: ApplicationStateConnecting =>
        PrettyObject(
          PrettyField("General info", prettyGeneralInfo(info)),
          PrettyField(
            "Ledger info",
            PrettyObject(
              PrettyField("Connection status", "Connecting")
            )),
          PrettyField("Local data", prettyLocalDataInfo(state))
        )
      case info: ApplicationStateFailed =>
        PrettyObject(
          PrettyField("General info", prettyGeneralInfo(info)),
          PrettyField(
            "Ledger info",
            PrettyObject(
              PrettyField("Connection status", "Failed"),
              PrettyField("Error", info.error.getMessage)
            )),
          PrettyField("Local data", prettyLocalDataInfo(state))
        )
    }

  def eval(
      state: State,
      args: List[String],
      set: CommandSet): Either[CommandError, (State, String)] = {
    implicit val actorTimeout: Timeout = Timeout(10, TimeUnit.SECONDS)
    for {
      future <- Try((state.store ? GetApplicationStateInfo).mapTo[ApplicationStateInfo]) ~> "Failed to get info"
      info <- Try(Await.result(future, 10.seconds)) ~> "Failed to get info"
    } yield (state, getBanner(state) + "\n" + Pretty.yaml(prettyInfo(info, state)))
  }

  def getBanner(state: State): String = {
    s"Navigator version: ${state.applicationInfo.version}"
  }
}
