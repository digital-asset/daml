// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.canton.crypto.HashAlgorithm.Sha256
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch.{
  IdRange,
  Ids,
}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.{
  RawAcsDeltaEventLegacy,
  RawCreatedEventLegacy,
  RawLedgerEffectsEventLegacy,
}
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForUpdatesAcsDeltaLegacy,
  EventPayloadSourceForUpdatesLedgerEffectsLegacy,
}
import com.digitalasset.daml.lf.data.{Ref, Time}
import com.google.protobuf.ByteString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertion, OptionValues}

import scala.reflect.ClassTag

private[backend] trait StorageBackendTestsTransactionStreamsEvents
    extends Matchers
    with OptionValues
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues.*

  val contractId1 = hashCid("#1")
  val contractId2 = hashCid("#2")
  val contractId3 = hashCid("#3")
  val contractId4 = hashCid("#4")
  val signatory = Ref.Party.assertFromString("party")
  val someParty = Ref.Party.assertFromString(signatory)

  behavior of "StorageBackend events"

  it should "return the correct created_at" in {

    val create = dtoCreateLegacy(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contractId1,
      signatory = signatory,
    )

    ingestDtos(Vector(create))

    testCreatedAt(
      partiesO = Some(Set(someParty)),
      expectedCreatedAt = someTime,
    )

    testCreatedAt(
      partiesO = None,
      expectedCreatedAt = someTime,
    )
  }

  def testExternalTransactionHash(hash: Option[Array[Byte]]) = {
    val creates = Vector(
      dtoCreateLegacy(
        offset(1),
        1L,
        contractId = contractId1,
        signatory = signatory,
        externalTransactionHash = hash,
      ),
      dtoExerciseLegacy(
        offset(1),
        2L,
        consuming = true,
        contractId = contractId2,
        signatory = signatory,
        externalTransactionHash = hash,
      ),
      dtoExerciseLegacy(
        offset(1),
        2L,
        consuming = false,
        contractId = contractId3,
        signatory = signatory,
        externalTransactionHash = hash,
      ),
    )

    ingestDtos(creates)

    val someParty = Ref.Party.assertFromString(signatory)
    val filterParties = Some(Set(someParty))
    def flatTransactionEvents(target: EventPayloadSourceForUpdatesAcsDeltaLegacy) = executeSql(
      backend.event.fetchEventPayloadsAcsDeltaLegacy(
        target
      )(eventSequentialIds = Ids(Seq(1L, 2L, 3L, 4L)), filterParties)
    )
    def transactionTreeEvents(target: EventPayloadSourceForUpdatesLedgerEffectsLegacy) = executeSql(
      backend.event.fetchEventPayloadsLedgerEffectsLegacy(
        target
      )(eventSequentialIds = Ids(Seq(1L, 2L, 3L, 4L)), filterParties)
    )

    def byteArrayToHash(array: Array[Byte]) = Hash.tryFromByteStringRaw(ByteString.copyFrom(array))

    val expectedHash = hash.map(byteArrayToHash)

    flatTransactionEvents(EventPayloadSourceForUpdatesAcsDeltaLegacy.Create)
      .map(
        _.externalTransactionHash
      )
      .loneElement
      .map(byteArrayToHash) shouldBe expectedHash
    flatTransactionEvents(EventPayloadSourceForUpdatesAcsDeltaLegacy.Consuming)
      .map(
        _.externalTransactionHash
      )
      .loneElement
      .map(byteArrayToHash) shouldBe expectedHash
    transactionTreeEvents(EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create)
      .map(
        _.externalTransactionHash
      )
      .loneElement
      .map(byteArrayToHash) shouldBe expectedHash
    transactionTreeEvents(EventPayloadSourceForUpdatesLedgerEffectsLegacy.Consuming)
      .map(
        _.externalTransactionHash
      )
      .loneElement
      .map(byteArrayToHash) shouldBe expectedHash
    transactionTreeEvents(EventPayloadSourceForUpdatesLedgerEffectsLegacy.NonConsuming)
      .map(
        _.externalTransactionHash
      )
      .loneElement
      .map(byteArrayToHash) shouldBe expectedHash
  }

  it should "return empty external transaction hash" in {
    testExternalTransactionHash(None)
  }

  it should "return defined external transaction hash" in {
    testExternalTransactionHash(
      Some(
        Hash
          .digest(HashPurpose.PreparedSubmission, ByteString.copyFromUtf8("mock_hash"), Sha256)
          .unwrap
          .toByteArray
      )
    )
  }

  it should "return the correct stream contents for acs" in {
    val creates = Vector(
      dtoCreateLegacy(offset(1), 1L, contractId = contractId1, signatory = signatory),
      dtoCreateLegacy(offset(1), 2L, contractId = contractId2, signatory = signatory),
      dtoCreateLegacy(offset(1), 3L, contractId = contractId3, signatory = signatory),
      dtoCreateLegacy(offset(1), 4L, contractId = contractId4, signatory = signatory),
    )

    ingestDtos(creates)

    val someParty = Ref.Party.assertFromString(signatory)
    val (
      flatTransactionEvents,
      flatTransactionEventsRange,
      transactionTreeEvents,
      transactionTreeEventsRange,
      acs,
    ) = fetch(Some(Set(someParty)))

    flatTransactionEvents.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    flatTransactionEvents.map(_.event).collect { case created: RawCreatedEventLegacy =>
      created.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4)

    transactionTreeEvents.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    transactionTreeEvents.map(_.event).collect { case created: RawCreatedEventLegacy =>
      created.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4)

    acs.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)

    flatTransactionEventsRange.map(_.eventSequentialId) shouldBe
      flatTransactionEvents.map(_.eventSequentialId)

    transactionTreeEventsRange.map(_.eventSequentialId) shouldBe
      transactionTreeEvents.map(_.eventSequentialId)

    val (
      flatTransactionEventsSuperReader,
      flatTransactionEventsSuperReaderRange,
      transactionTreeEventsSuperReader,
      transactionTreeEventsSuperReaderRange,
      acsSuperReader,
    ) = fetch(None)

    flatTransactionEventsSuperReader.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    flatTransactionEventsSuperReader.map(_.event).collect { case created: RawCreatedEventLegacy =>
      created.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4)

    transactionTreeEventsSuperReader.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    transactionTreeEventsSuperReader.map(_.event).collect { case created: RawCreatedEventLegacy =>
      created.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4)

    acsSuperReader.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)

    flatTransactionEventsSuperReaderRange.map(_.eventSequentialId) shouldBe
      flatTransactionEventsSuperReader.map(_.eventSequentialId)

    transactionTreeEventsSuperReaderRange.map(_.eventSequentialId) shouldBe
      transactionTreeEventsSuperReader.map(_.eventSequentialId)
  }

  private def ingestDtos(creates: Vector[DbDto]) = {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(creates, _))
    executeSql(updateLedgerEnd(offset(1), creates.size.toLong))
  }

  private def fetch(filterParties: Option[Set[Ref.Party]]) = {

    val flatTransactionEvents = executeSql(
      backend.event.fetchEventPayloadsAcsDeltaLegacy(
        EventPayloadSourceForUpdatesAcsDeltaLegacy.Create
      )(eventSequentialIds = Ids(Seq(1L, 2L, 3L, 4L)), filterParties)
    )
    val flatTransactionEventsRange = executeSql(
      backend.event.fetchEventPayloadsAcsDeltaLegacy(
        EventPayloadSourceForUpdatesAcsDeltaLegacy.Create
      )(eventSequentialIds = IdRange(1L, 4L), filterParties)
    )
    val transactionTreeEvents = executeSql(
      backend.event.fetchEventPayloadsLedgerEffectsLegacy(
        EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create
      )(eventSequentialIds = Ids(Seq(1L, 2L, 3L, 4L)), filterParties)
    )
    val transactionTreeEventsRange = executeSql(
      backend.event.fetchEventPayloadsLedgerEffectsLegacy(
        EventPayloadSourceForUpdatesLedgerEffectsLegacy.Create
      )(eventSequentialIds = IdRange(1L, 4L), filterParties)
    )
    val acs = executeSql(
      backend.event.activeContractCreateEventBatchLegacy(Seq(1L, 2L, 3L, 4L), filterParties, 4L)
    )
    (
      flatTransactionEvents,
      flatTransactionEventsRange,
      transactionTreeEvents,
      transactionTreeEventsRange,
      acs,
    )
  }

  private def testCreatedAt(
      partiesO: Option[Set[Ref.Party]],
      expectedCreatedAt: Time.Timestamp,
  ): Assertion = {
    val (
      flatTransactionEvents,
      flatTransactionEventsRange,
      transactionTreeEvents,
      transactionTreeEventsRange,
      acs,
    ) = fetch(partiesO)

    extractCreatedAtFrom[RawCreatedEventLegacy, RawAcsDeltaEventLegacy](
      in = flatTransactionEvents,
      createdAt = _.ledgerEffectiveTime,
    ) shouldBe expectedCreatedAt

    extractCreatedAtFrom[RawCreatedEventLegacy, RawLedgerEffectsEventLegacy](
      in = transactionTreeEvents,
      createdAt = _.ledgerEffectiveTime,
    ) shouldBe expectedCreatedAt

    acs.head.rawCreatedEvent.ledgerEffectiveTime shouldBe expectedCreatedAt

    flatTransactionEventsRange.map(_.eventSequentialId) shouldBe
      flatTransactionEvents.map(_.eventSequentialId)

    transactionTreeEventsRange.map(_.eventSequentialId) shouldBe
      transactionTreeEvents.map(_.eventSequentialId)
  }

  private def extractCreatedAtFrom[O: ClassTag, E >: O](
      in: Seq[EventStorageBackend.Entry[E]],
      createdAt: O => Time.Timestamp,
  ): Time.Timestamp = {
    in.size shouldBe 1
    in.head.event match {
      case o: O => createdAt(o)
      case _ =>
        fail(
          s"Expected created event of type ${implicitly[reflect.ClassTag[O]].runtimeClass.getSimpleName}"
        )
    }
  }
}
