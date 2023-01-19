// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import com.daml.ledger.api.domain.IdentityProviderId

import java.sql.Connection
import com.daml.lf.data.Ref

trait PartyRecordStorageBackend extends ResourceVersionOps {

  def getPartyRecord(party: Ref.Party)(
      connection: Connection
  ): Option[PartyRecordStorageBackend.DbPartyRecord]

  def createPartyRecord(partyRecord: PartyRecordStorageBackend.DbPartyRecordPayload)(
      connection: Connection
  ): Int

  def getPartyAnnotations(internalId: Int)(connection: Connection): Map[String, String]

  def addPartyAnnotation(internalId: Int, key: String, value: String, updatedAt: Long)(
      connection: Connection
  ): Unit

  def deletePartyAnnotations(internalId: Int)(connection: Connection): Unit

  def filterExistingParties(
      parties: Set[Ref.Party],
      identityProviderId: Option[IdentityProviderId.Id],
  )(connection: Connection): Set[Ref.Party]

}

object PartyRecordStorageBackend {
  case class DbPartyRecordPayload(
      party: Ref.Party,
      identityProviderId: Option[IdentityProviderId.Id],
      resourceVersion: Long,
      createdAt: Long,
  )

  case class DbPartyRecord(
      internalId: Int,
      payload: DbPartyRecordPayload,
  )
}
