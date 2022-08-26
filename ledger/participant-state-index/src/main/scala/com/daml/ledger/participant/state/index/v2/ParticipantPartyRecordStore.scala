// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.Future

trait LedgerPartyExists {

  /** @return Whether a party is known to this participant
    */
  def exists(party: Ref.Party): Future[Boolean]
}

trait ParticipantPartyRecordStore {
  import ParticipantPartyRecordStore._

  def createPartyRecord(partyRecord: domain.ParticipantParty.PartyRecord)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.ParticipantParty.PartyRecord]]

  def updatePartyRecord(partyRecordUpdate: PartyRecordUpdate, ledgerPartyExists: LedgerPartyExists)(
      implicit loggingContext: LoggingContext
  ): Future[Result[domain.ParticipantParty.PartyRecord]]

  def getPartyRecord(party: Ref.Party)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.ParticipantParty.PartyRecord]]

}

object ParticipantPartyRecordStore {
  type Result[T] = Either[Error, T]

  final case object PartyRecordNotFoundOnUpdateException extends RuntimeException

  sealed trait Error extends RuntimeException

  final case class PartyNotFound(party: Ref.Party) extends Error
  final case class PartyRecordNotFound(party: Ref.Party) extends Error
  final case class PartyRecordExists(party: Ref.Party) extends Error
  final case class ConcurrentPartyUpdate(party: Ref.Party) extends Error
  final case class ExplicitMergeUpdateWithDefaultValue(party: Ref.Party) extends Error

}
