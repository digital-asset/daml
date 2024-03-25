// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.LedgerString
import com.digitalasset.canton.ledger.api.domain.{
  IdentityProviderConfig,
  IdentityProviderId,
  JwksUrl,
}
import com.digitalasset.canton.platform.store.backend.localstore.PartyRecordStorageBackend
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, OptionValues}

import java.sql.SQLException
import java.util.UUID

private[backend] trait StorageBackendTestsPartyRecord
    extends Matchers
    with Inside
    with StorageBackendSpec
    with OptionValues
    with ParticipantResourceMetadataTests {
  this: AnyFlatSpec =>

  behavior of "StorageBackend (party record)"

  private val zeroMicros: Long = 0

  private val idpId = IdentityProviderId.Id(Ref.LedgerString.assertFromString("idp1"))
  private val idpConfig = IdentityProviderConfig(
    identityProviderId = idpId,
    isDeactivated = false,
    jwksUrl = JwksUrl("http//identityprovider.domain/"),
    issuer = "issuer",
    audience = Some("audience"),
  )

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

  override def resourceVersionTableName: String = "lapi_party_records"

  override def resourceAnnotationsTableName: String = "lapi_party_record_annotations"

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

  it should "filter parties within the same idp" in {
    val idpId = IdentityProviderId.Id(LedgerString.assertFromString("abc"))
    val _ = executeSql(
      backend.identityProviderStorageBackend.createIdentityProviderConfig(
        IdentityProviderConfig(
          identityProviderId = idpId,
          issuer = "issuer",
          jwksUrl = JwksUrl("http://daml.com/jwks.json"),
          audience = None,
        )
      )
    )
    val party1 = Ref.Party.assertFromString("party1")
    val party2 = Ref.Party.assertFromString("party2")
    val partyRecord1 = newDbPartyRecord(partyId = "party1")
    val partyRecord2 = newDbPartyRecord(
      partyId = "party2",
      identityProviderId = Some(idpId),
    )
    val _ = executeSql(tested.createPartyRecord(partyRecord1))
    val _ = executeSql(tested.createPartyRecord(partyRecord2))
    executeSql(
      tested.filterExistingParties(
        Set(),
        Some(IdentityProviderId.Id(LedgerString.assertFromString("cde"))),
      )
    ) shouldBe Set.empty

    executeSql(
      tested.filterExistingParties(
        Set(),
        None,
      )
    ) shouldBe Set.empty

    executeSql(
      tested.filterExistingParties(
        Set(party1, party2),
        None,
      )
    ) shouldBe Set(party1)

    executeSql(
      tested.filterExistingParties(
        Set(party1, party2),
        Some(idpId),
      )
    ) shouldBe Set(party2)
  }

  it should "update party's identityProviderId" in {
    executeSql(
      backend.identityProviderStorageBackend.createIdentityProviderConfig(
        idpConfig
      )
    )
    // create with the default idp
    val pr = newDbPartyRecord(
      createdAt = 123,
      partyId = "party",
      identityProviderId = IdentityProviderId.Default.toDb,
    )
    val internalId = executeSql(tested.createPartyRecord(pr))
    executeSql(
      (tested.getPartyRecord(pr.party))
    ).value.payload.identityProviderId shouldBe IdentityProviderId.Default.toDb
    // update to idp1
    executeSql(
      tested.updatePartyRecordIdp(internalId, identityProviderId = idpId.toDb)
    ) shouldBe true
    executeSql(
      (tested.getPartyRecord(pr.party))
    ).value.payload.identityProviderId shouldBe idpId.toDb
    // update to idp1 again
    executeSql(
      tested.updatePartyRecordIdp(internalId, identityProviderId = idpId.toDb)
    ) shouldBe true
    executeSql(
      (tested.getPartyRecord(pr.party))
    ).value.payload.identityProviderId shouldBe idpId.toDb
    // update to the default idp
    executeSql(
      tested.updatePartyRecordIdp(internalId, identityProviderId = IdentityProviderId.Default.toDb)
    ) shouldBe true
    executeSql(
      (tested.getPartyRecord(pr.party))
    ).value.payload.identityProviderId shouldBe IdentityProviderId.Default.toDb
    // update on non-existent user
    executeSql(
      tested.updatePartyRecordIdp(100000, identityProviderId = IdentityProviderId.Default.toDb)
    ) shouldBe false
  }

  private def newDbPartyRecord(
      partyId: String = "",
      resourceVersion: Long = 0,
      createdAt: Long = zeroMicros,
      identityProviderId: Option[IdentityProviderId.Id] = None,
  ): PartyRecordStorageBackend.DbPartyRecordPayload = {
    val uuid = UUID.randomUUID.toString
    val party = if (partyId != "") partyId else s"party_id_$uuid"
    PartyRecordStorageBackend.DbPartyRecordPayload(
      party = Ref.Party.assertFromString(party),
      identityProviderId = identityProviderId,
      resourceVersion = resourceVersion,
      createdAt = createdAt,
    )
  }

}
