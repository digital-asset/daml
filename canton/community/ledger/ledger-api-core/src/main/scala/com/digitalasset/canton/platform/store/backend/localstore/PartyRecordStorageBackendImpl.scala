// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.localstore

import anorm.SqlParser.{int, long, str}
import anorm.{RowParser, SqlParser, SqlStringInterpolation, ~}
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.domain.IdentityProviderId
import com.digitalasset.canton.platform.store.backend.Conversions.party

import java.sql.Connection
import scala.util.Try

object PartyRecordStorageBackendImpl extends PartyRecordStorageBackend {

  private val PartyRecordParser: RowParser[(Int, Ref.Party, Option[String], Long, Long)] =
    int("internal_id") ~
      party("party") ~
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
            party = party,
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
    internalId.fold(throw _, identity)
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

  override def filterExistingParties(
      parties: Set[Ref.Party],
      identityProviderId: Option[IdentityProviderId.Id],
  )(connection: Connection): Set[Ref.Party] = if (parties.nonEmpty) {
    import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val filteredParties = cSQL"party in (${parties.map(_.toString)})"

    val filteredIdentityProviderId = identityProviderId match {
      case Some(id) => cSQL"identity_provider_id = ${id.value: String}"
      case None => cSQL"identity_provider_id is NULL"
    }
    SQL"""
         SELECT
             party
         FROM participant_party_records
         WHERE
             $filteredIdentityProviderId AND $filteredParties
       """
      .asVectorOf(party("party"))(connection)
      .toSet
  } else Set.empty

  override def filterExistingParties(
      parties: Set[Ref.Party]
  )(connection: Connection): Set[Ref.Party] = if (parties.nonEmpty) {
    import com.digitalasset.canton.platform.store.backend.common.SimpleSqlAsVectorOf.*
    import com.digitalasset.canton.platform.store.backend.common.ComposableQuery.SqlStringInterpolation
    val filteredParties = cSQL"party in (${parties.map(_.toString)})"

    SQL"""
         SELECT
             party
         FROM participant_party_records
         WHERE
             $filteredParties
         ORDER BY party
       """
      .asVectorOf(party("party"))(connection)
      .toSet
  } else Set.empty

  override def updatePartyRecordIdp(
      internalId: Int,
      identityProviderId: Option[IdentityProviderId.Id],
  )(connection: Connection): Boolean = {
    val idpId = identityProviderId.map(_.value): Option[String]
    val rowsUpdated =
      SQL"""
         UPDATE participant_party_records
         SET identity_provider_id = $idpId
         WHERE
             internal_id = ${internalId}
       """.executeUpdate()(connection)
    rowsUpdated == 1
  }
}
