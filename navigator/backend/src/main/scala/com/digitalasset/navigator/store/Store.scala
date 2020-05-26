// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.navigator.store

import java.time.Instant

import com.daml.navigator.model._
import com.daml.ledger.api.refinements.ApiTypes
import com.daml.navigator.time.TimeProviderWithType

trait ActorStatus

/** Actor reports working at full health */
case object ActorRunning extends ActorStatus

/** Actor is still starting up */
case object ActorStarting extends ActorStatus

/** Actor reports a permanently failed state */
case class ActorFailed(error: Throwable) extends ActorStatus

/** Actor did not respond within a reasonable time */
case object ActorUnresponsive extends ActorStatus

object Store {

  /** Reinitialize the platform connection and reset all local state `Unit` */
  case object ResetConnection

  /** Request to subscribe a party to the store (without response to sender). */
  case class Subscribe(party: PartyState)

  /** Request to create a contract instance for a template and respond with a `scala.util.Try[CommandId]`. */
  case class CreateContract(party: PartyState, templateId: TemplateStringId, argument: ApiRecord)

  /** Request to exercise a choice on a contract and respond with a `scala.util.Try[CommandId]`. */
  case class ExerciseChoice(
      party: PartyState,
      contractId: ApiTypes.ContractId,
      choiceId: ApiTypes.Choice,
      argument: ApiValue)

  /** Request to respond with a `scala.util.Try[TimeProviderWithType]` with the current store time. */
  case object ReportCurrentTime

  /**
    * Request to advance time to the specified instant and respond with updated store time as a
    * `scala.util.Try[TimeProviderWithType]`.
    */
  case class AdvanceTime(to: Instant)

  /** An indication that the store was not able to execute a request. */
  case class StoreException(message: String) extends Exception(message)

  /** Request diagnostic information about the state of the application and respond with a [[ApplicationStateInfo]]. */
  case object GetApplicationStateInfo

  /** Diagnostic information about the state of the application */
  sealed trait ApplicationStateInfo {
    def platformHost: String
    def platformPort: Int
    def tls: Boolean
    def applicationId: String
  }

  /** Application is still connecting to the ledger */
  final case class ApplicationStateConnecting(
      platformHost: String,
      platformPort: Int,
      tls: Boolean,
      applicationId: String
  ) extends ApplicationStateInfo

  /** Application is still connecting to the ledger */
  final case class ApplicationStateConnected(
      platformHost: String,
      platformPort: Int,
      tls: Boolean,
      applicationId: String,
      ledgerId: String,
      ledgerTime: TimeProviderWithType,
      partyActors: List[PartyActorInfo]
  ) extends ApplicationStateInfo

  /** Application failed to start up */
  final case class ApplicationStateFailed(
      platformHost: String,
      platformPort: Int,
      tls: Boolean,
      applicationId: String,
      error: Throwable
  ) extends ApplicationStateInfo

  /** Request diagnostic information about a party and respond with a [[PartyActorInfo]]. */
  case object GetPartyActorInfo

  /** Diagnostic information about a party */
  sealed trait PartyActorInfo {
    def party: ApiTypes.Party
  }

  /** Actor still starting up */
  final case class PartyActorStarting(party: ApiTypes.Party) extends PartyActorInfo

  /** Actor running and consuming the transaction stream */
  final case class PartyActorStarted(party: ApiTypes.Party) extends PartyActorInfo

  /** Actor permanently failed */
  final case class PartyActorFailed(
      party: ApiTypes.Party,
      error: Throwable
  ) extends PartyActorInfo

  /** Actor did not respond within a reasonable time */
  final case class PartyActorUnresponsive(party: ApiTypes.Party) extends PartyActorInfo
}
