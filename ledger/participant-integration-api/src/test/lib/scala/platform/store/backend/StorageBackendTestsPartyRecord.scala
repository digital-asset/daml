// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import java.sql.SQLException
import java.util.UUID

import com.daml.lf.data.Ref
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

private[backend] trait StorageBackendTestsPartyRecord
    extends Matchers
    with Inside
    with StorageBackendSpec
    with OptionValues
    with ParticipantResourceMetadataTests {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (party record)"

  private val zeroMicros: Long = 0

  private def tested = backend.participantPartyStorageBackend

  override def newResource(): TestedResource = new TestedResource {
    private val partyRecord = newDbPartyRecord()

    override def createResourceAndReturnInternalId(): Int = {
      val internalId = executeSql(tested.createPartyRecord(partyRecord))
      internalId
    }

    override def fetchResourceVersion(): Long = {
      executeSql(tested.getPartyRecord(partyRecord.party)).value.payload.resourceVersion
    }
  }

  override def resourceVersionTableName: String = "participant_party_records"

  override def resourceAnnotationsTableName: String = "participant_party_record_annotations"

  it should "handle created_at attribute correctly" in {
    val partyRecord = newDbPartyRecord(createdAt = 123)
    val _ = executeSql(tested.createPartyRecord(partyRecord))
    executeSql(tested.getPartyRecord(partyRecord.party)).map(_.payload.createdAt) shouldBe Some(123)
  }

  it should "create party record (createPartyRecord)" in {
    val partyRecord1 = newDbPartyRecord()
    val partyRecord2 = newDbPartyRecord()
    val internalId1 = executeSql(tested.createPartyRecord(partyRecord1))
    // Attempting to add a duplicate user
    assertThrows[SQLException](executeSql(tested.createPartyRecord(partyRecord1)))
    val internalId2 = executeSql(tested.createPartyRecord(partyRecord2))
    val _ =
      executeSql(tested.createPartyRecord(newDbPartyRecord()))
    internalId1 should not equal internalId2
  }

  it should "handle party record ops (getPartyRecord)" in {
    val partyRecord1 = newDbPartyRecord()
    val partyRecord2 = newDbPartyRecord()
    val _ = executeSql(tested.createPartyRecord(partyRecord1))
    val getExisting = executeSql(tested.getPartyRecord(partyRecord1.party))
    val getNonexistent = executeSql(tested.getPartyRecord(partyRecord2.party))
    getExisting.value.payload shouldBe partyRecord1
    getNonexistent shouldBe None
  }

  private def newDbPartyRecord(
      partyId: String = "",
      resourceVersion: Long = 0,
      createdAt: Long = zeroMicros,
  ): PartyRecordStorageBackend.DbPartyRecordPayload = {
    val uuid = UUID.randomUUID.toString
    val party = if (partyId != "") partyId else s"party_id_$uuid"
    PartyRecordStorageBackend.DbPartyRecordPayload(
      party = Ref.Party.assertFromString(party),
      resourceVersion = resourceVersion,
      createdAt = createdAt,
    )
  }

}
