// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.server.api.validation.ResourceAnnotationValidation

import scala.concurrent.Future

case class PartyDetailsUpdate(
    party: Ref.Party,
    identityProviderId: IdentityProviderId,
    displayNameUpdate: Option[Option[String]],
    isLocalUpdate: Option[Boolean],
    metadataUpdate: ObjectMetaUpdate,
)

case class PartyRecordUpdate(
    party: Ref.Party,
    identityProviderId: IdentityProviderId,
    metadataUpdate: ObjectMetaUpdate,
)

trait PartyRecordStore {
  import PartyRecordStore._

  def createPartyRecord(partyRecord: PartyRecord)(implicit
      loggingContext: LoggingContext
  ): Future[Result[PartyRecord]]

  def updatePartyRecord(partyRecordUpdate: PartyRecordUpdate, ledgerPartyIsLocal: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Result[PartyRecord]]

  def getPartyRecordO(party: Ref.Party)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Option[PartyRecord]]]

  def partiesExist(parties: Set[Ref.Party], identityProviderId: IdentityProviderId)(implicit
      loggingContext: LoggingContext
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
    def getReason: String = ResourceAnnotationValidation.AnnotationsSizeExceededError.reason
  }

}
