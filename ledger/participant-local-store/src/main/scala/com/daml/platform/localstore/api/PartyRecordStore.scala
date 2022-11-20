// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.localstore.api

import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.platform.server.api.validation.ResourceAnnotationValidation

import scala.concurrent.Future

trait LedgerPartyExists {

  /** @return Whether a party is known to this participant
    */
  def exists(party: Ref.Party): Future[Boolean]
}

case class PartyDetailsUpdate(
    party: Ref.Party,
    displayNameUpdate: Option[Option[String]],
    isLocalUpdate: Option[Boolean],
    metadataUpdate: ObjectMetaUpdate,
    identityProviderIdUpdate: Option[IdentityProviderId] = None,
)

case class PartyRecordUpdate(
    party: Ref.Party,
    metadataUpdate: ObjectMetaUpdate,
    identityProviderIdUpdate: Option[IdentityProviderId],
)

trait PartyRecordStore {
  import PartyRecordStore._

  def createPartyRecord(partyRecord: PartyRecord)(implicit
      loggingContext: LoggingContext
  ): Future[Result[PartyRecord]]

  def updatePartyRecord(partyRecordUpdate: PartyRecordUpdate, ledgerPartyExists: LedgerPartyExists)(
      implicit loggingContext: LoggingContext
  ): Future[Result[PartyRecord]]

  def getPartyRecordO(party: Ref.Party)(implicit
      loggingContext: LoggingContext
  ): Future[Result[Option[PartyRecord]]]

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
