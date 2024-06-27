// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.localstore.api

import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.ledger.api.validation.ResourceAnnotationValidator
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.daml.lf.data.Ref

import scala.concurrent.Future

final case class PartyDetailsUpdate(
    party: Ref.Party,
    identityProviderId: IdentityProviderId,
    displayNameUpdate: Option[Option[String]],
    isLocalUpdate: Option[Boolean],
    metadataUpdate: ObjectMetaUpdate,
)

final case class PartyRecordUpdate(
    party: Ref.Party,
    identityProviderId: IdentityProviderId,
    metadataUpdate: ObjectMetaUpdate,
)

trait PartyRecordStore {
  import PartyRecordStore.*

  def createPartyRecord(partyRecord: PartyRecord)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[PartyRecord]]

  def updatePartyRecord(partyRecordUpdate: PartyRecordUpdate, ledgerPartyIsLocal: Boolean)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[PartyRecord]]

  def updatePartyRecordIdp(
      party: Ref.Party,
      ledgerPartyIsLocal: Boolean,
      sourceIdp: IdentityProviderId,
      targetIdp: IdentityProviderId,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Result[PartyRecord]]

  def getPartyRecordO(party: Ref.Party)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Result[Option[PartyRecord]]]

  def filterExistingParties(parties: Set[Ref.Party], identityProviderId: IdentityProviderId)(
      implicit loggingContext: LoggingContextWithTrace
  ): Future[Set[Ref.Party]]

  def filterExistingParties(parties: Set[Ref.Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Set[Ref.Party]]

}

object PartyRecordStore {
  type Result[T] = Either[Error, T]

  sealed trait Error

  final case class PartyNotFound(party: Ref.Party) extends Error
  final case class PartyRecordNotFoundFatal(party: Ref.Party) extends Error
  final case class PartyRecordExistsFatal(party: Ref.Party) extends Error
  final case class ConcurrentPartyUpdate(party: Ref.Party) extends Error
  final case class MaxAnnotationsSizeExceeded(party: Ref.Party) extends Error {
    def getReason: String = ResourceAnnotationValidator.AnnotationsSizeExceededError.reason
  }

}
