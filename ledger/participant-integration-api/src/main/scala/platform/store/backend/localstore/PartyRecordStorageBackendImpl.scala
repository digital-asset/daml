// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.localstore

import java.sql.Connection
import anorm.SqlParser.{int, long, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.ledger.api.domain.IdentityProviderId
import com.daml.lf.data.Ref

import scala.util.Try

object PartyRecordStorageBackendImpl extends PartyRecordStorageBackend {

  private val PartyRecordParser: RowParser[(Int, String, Option[String], Long, Long)] =
    int("internal_id") ~
      str("party") ~
      str("identity_provider_id").? ~
      long("resource_version") ~
      long("created_at") map {
        case internalId ~ party ~ identityProviderId ~ resourceVersion ~ createdAt =>
          (internalId, party, identityProviderId, resourceVersion, createdAt)
      }

  override def getPartyRecord(
      party: Ref.Party
  )(connection: Connection): Option[PartyRecordStorageBackend.DbPartyRecord] = {
    SQL"""
         SELECT
             internal_id,
             party,
             identity_provider_id,
             resource_version,
             created_at
         FROM participant_party_records
         WHERE
             party = ${party: String}
       """
      .as(PartyRecordParser.singleOpt)(connection)
      .map { case (internalId, party, identityProviderId, resourceVersion, createdAt) =>
        PartyRecordStorageBackend.DbPartyRecord(
          internalId = internalId,
          payload = PartyRecordStorageBackend.DbPartyRecordPayload(
            party = com.daml.platform.Party.assertFromString(party),
            identityProviderId = identityProviderId.map(IdentityProviderId.Id.assertFromString),
            resourceVersion = resourceVersion,
            createdAt = createdAt,
          ),
        )
      }
  }

  override def createPartyRecord(
      partyRecord: PartyRecordStorageBackend.DbPartyRecordPayload
  )(connection: Connection): Int = {
    val party = partyRecord.party: String
    val identityProviderId = partyRecord.identityProviderId.map(_.value): Option[String]
    val resourceVersion = partyRecord.resourceVersion
    val createdAt = partyRecord.createdAt
    val internalId: Try[Int] = SQL"""
         INSERT INTO participant_party_records (party, identity_provider_id, resource_version, created_at)
         VALUES ($party, $identityProviderId, $resourceVersion, $createdAt)
       """.executeInsert1("internal_id")(SqlParser.scalar[Int].single)(connection)
    internalId.get
  }

  override def getPartyAnnotations(internalId: Int)(connection: Connection): Map[String, String] = {
    ParticipantMetadataBackend.getAnnotations("participant_party_record_annotations")(internalId)(
      connection
    )
  }

  override def addPartyAnnotation(internalId: Int, key: String, value: String, updatedAt: Long)(
      connection: Connection
  ): Unit = {
    ParticipantMetadataBackend.addAnnotation("participant_party_record_annotations")(
      internalId,
      key,
      value,
      updatedAt,
    )(connection)
  }

  override def deletePartyAnnotations(internalId: Int)(connection: Connection): Unit = {
    ParticipantMetadataBackend.deleteAnnotations("participant_party_record_annotations")(
      internalId
    )(
      connection
    )
  }

  override def compareAndIncreaseResourceVersion(internalId: Int, expectedResourceVersion: Long)(
      connection: Connection
  ): Boolean = {
    ParticipantMetadataBackend.compareAndIncreaseResourceVersion("participant_party_records")(
      internalId,
      expectedResourceVersion,
    )(connection)
  }

  override def increaseResourceVersion(internalId: Int)(connection: Connection): Boolean = {
    ParticipantMetadataBackend.increaseResourceVersion("participant_party_records")(internalId)(
      connection
    )
  }

  override def updateIdentityProviderId(
      internalId: Int,
      identityProviderId: Option[IdentityProviderId.Id],
  )(connection: Connection): Boolean =
    ParticipantMetadataBackend.updateIdentityProviderId("participant_party_records")(
      internalId,
      identityProviderId,
    )(
      connection
    )
}
