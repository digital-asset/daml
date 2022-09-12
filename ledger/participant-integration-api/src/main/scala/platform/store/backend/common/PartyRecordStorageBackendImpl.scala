// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend.common

import java.sql.Connection

import com.daml.platform.store.backend.PartyRecordStorageBackend

import anorm.SqlParser.{int, long, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.lf.data.Ref

import scala.util.Try

class PartyRecordStorageBackendImpl extends PartyRecordStorageBackend {

  private val PartyRecordParser: RowParser[(Int, String, Long, Long)] =
    int("internal_id") ~
      str("party") ~
      long("resource_version") ~
      long("created_at") map { case internalId ~ party ~ resourceVersion ~ createdAt =>
        (internalId, party, resourceVersion, createdAt)
      }

  override def getPartyRecords(
  )(connection: Connection): Seq[PartyRecordStorageBackend.DbPartyRecord] = {
    import com.daml.platform.store.backend.common.SimpleSqlAsVectorOf._
    SQL"""
         SELECT
             internal_id,
             party,
             resource_version,
             created_at
         FROM participant_party_records
       """
      .asVectorOf(PartyRecordParser)(connection)
      .map { case (internalId, party, resourceVersion, createdAt) =>
        PartyRecordStorageBackend.DbPartyRecord(
          internalId = internalId,
          payload = PartyRecordStorageBackend.DbPartyRecordPayload(
            party = com.daml.platform.Party.assertFromString(party),
            resourceVersion = resourceVersion,
            createdAt = createdAt,
          ),
        )
      }
  }

  override def getPartyRecord(
      party: Ref.Party
  )(connection: Connection): Option[PartyRecordStorageBackend.DbPartyRecord] = {
    SQL"""
         SELECT
             internal_id,
             party,
             resource_version,
             created_at
         FROM participant_party_records
         WHERE
             party = ${party: String}
       """
      .as(PartyRecordParser.singleOpt)(connection)
      .map { case (internalId, party, resourceVersion, createdAt) =>
        PartyRecordStorageBackend.DbPartyRecord(
          internalId = internalId,
          payload = PartyRecordStorageBackend.DbPartyRecordPayload(
            party = com.daml.platform.Party.assertFromString(party),
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
    val resourceVersion = partyRecord.resourceVersion
    val createdAt = partyRecord.createdAt
    val internalId: Try[Int] = SQL"""
         INSERT INTO participant_party_records (party, resource_version, created_at)
         VALUES ($party, $resourceVersion, $createdAt)
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

}
