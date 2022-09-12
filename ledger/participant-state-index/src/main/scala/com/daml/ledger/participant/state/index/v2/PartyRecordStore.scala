// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v2

import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.index.ResourceAnnotationValidation
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext

import scala.concurrent.Future

trait LedgerPartyExists {

  /** @return Whether a party is known to this participant
    */
  def exists(party: Ref.Party): Future[Boolean]
}

case class PartyRecordUpdate(
    party: Ref.Party,
    metadataUpdate: ObjectMetaUpdate,
) {
  def isNoUpdate: Boolean = metadataUpdate.isNoUpdate
}

trait PartyRecordStore {
  import PartyRecordStore._

  def createPartyRecord(partyRecord: domain.ParticipantParty.PartyRecord)(implicit
      loggingContext: LoggingContext
  ): Future[Result[domain.ParticipantParty.PartyRecord]]

  // TODO um-for-hub major: Validate the size of update annotations is within max annotations size
  def updatePartyRecord(partyRecordUpdate: PartyRecordUpdate, ledgerPartyExists: LedgerPartyExists)(
      implicit loggingContext: LoggingContext
  ): Future[Result[domain.ParticipantParty.PartyRecord]]

  def getPartyRecordO(party: Ref.Party)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Option[domain.ParticipantParty.PartyRecord]]]

}

object PartyRecordStore {
  type Result[T] = Either[Error, T]

  /** Represents an edge case where a participant server submits a party allocation command
    * but crashes before it had a chance to create a corresponding party-record (upon the successful party allocation).
    */
  final case object PartyRecordNotFoundOnUpdateException extends RuntimeException

  sealed trait Error

  final case class PartyNotFound(party: Ref.Party) extends Error
  final case class PartyRecordNotFound(party: Ref.Party) extends Error
  final case class PartyRecordExists(party: Ref.Party) extends Error
  final case class ConcurrentPartyUpdate(party: Ref.Party) extends Error
  final case class MaxAnnotationsSizeExceeded(party: Ref.Party) extends Error {
    def getReason: String = ResourceAnnotationValidation.AnnotationsSizeExceededError.reason
  }

}
