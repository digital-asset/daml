// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.canton.platform.store.backend.common.{
  EventPayloadSourceForFlatTx,
  EventPayloadSourceForTreeTx,
}
import com.digitalasset.canton.platform.store.dao.events.Raw.{FlatEvent, TreeEvent}
import com.google.protobuf.timestamp.Timestamp
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

    val create = dtoCreate(
      offset = offset(1),
      eventSequentialId = 1L,
      contractId = contractId1,
      signatory = signatory,
    )

    ingestDtos(Vector(create))

    testCreatedAt(
      partiesO = Some(Set(someParty)),
      expectedCreatedAt = Timestamp(someTime.toInstant),
    )

    testCreatedAt(
      partiesO = None,
      expectedCreatedAt = Timestamp(someTime.toInstant),
    )
  }

  it should "return the correct stream contents for acs" in {
    val creates = Vector(
      dtoCreate(offset(1), 1L, contractId = contractId1, signatory = signatory),
      dtoCreate(offset(1), 2L, contractId = contractId2, signatory = signatory),
      dtoCreate(offset(1), 3L, contractId = contractId3, signatory = signatory),
      dtoCreate(offset(1), 4L, contractId = contractId4, signatory = signatory),
    )

    ingestDtos(creates)

    val someParty = Ref.Party.assertFromString(signatory)
    val (
      flatTransactionEvents,
      transactionTreeEvents,
      _flatTransaction,
      _transactionTree,
      acs,
    ) = fetch(Some(Set(someParty)))

    flatTransactionEvents.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    flatTransactionEvents.map(_.event).collect { case created: FlatEvent.Created =>
      created.partial.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4).map(_.coid)

    transactionTreeEvents.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    transactionTreeEvents.map(_.event).collect { case created: TreeEvent.Created =>
      created.partial.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4).map(_.coid)

    acs.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)

    val (
      flatTransactionEventsSuperReader,
      transactionTreeEventsSuperReader,
      _,
      _,
      acsSuperReader,
    ) = fetch(None)

    flatTransactionEventsSuperReader.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    flatTransactionEventsSuperReader.map(_.event).collect { case created: FlatEvent.Created =>
      created.partial.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4).map(_.coid)

    transactionTreeEventsSuperReader.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)
    transactionTreeEventsSuperReader.map(_.event).collect { case created: TreeEvent.Created =>
      created.partial.contractId
    } shouldBe Vector(contractId1, contractId2, contractId3, contractId4).map(_.coid)

    acsSuperReader.map(_.eventSequentialId) shouldBe Vector(1L, 2L, 3L, 4L)

  }

  private def ingestDtos(creates: Vector[DbDto.EventCreate]) = {
    executeSql(backend.parameter.initializeParameters(someIdentityParams, loggerFactory))
    executeSql(ingest(creates, _))
    executeSql(updateLedgerEnd(offset(1), creates.size.toLong))
  }

  private def fetch(filterParties: Option[Set[Ref.Party]]) = {

    val flatTransactionEvents = executeSql(
      backend.event.transactionStreamingQueries.fetchEventPayloadsFlat(
        EventPayloadSourceForFlatTx.Create
      )(eventSequentialIds = Seq(1L, 2L, 3L, 4L), filterParties)
    )
    val transactionTreeEvents = executeSql(
      backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
        EventPayloadSourceForTreeTx.Create
      )(eventSequentialIds = Seq(1L, 2L, 3L, 4L), filterParties)
    )
    val flatTransaction = executeSql(
      backend.event.transactionPointwiseQueries
        .fetchFlatTransactionEvents(1L, 1L, filterParties.getOrElse(Set.empty))
    )
    val transactionTree = executeSql(
      backend.event.transactionPointwiseQueries
        .fetchTreeTransactionEvents(1L, 1L, filterParties.getOrElse(Set.empty))
    )
    val acs = executeSql(
      backend.event.activeContractCreateEventBatch(Seq(1L, 2L, 3L, 4L), filterParties, 4L)
    )
    (
      flatTransactionEvents,
      transactionTreeEvents,
      flatTransaction,
      transactionTree,
      acs,
    )
  }

  private def testCreatedAt(
      partiesO: Option[Set[Ref.Party]],
      expectedCreatedAt: Timestamp,
  ): Assertion = {
    val (
      flatTransactionEvents,
      transactionTreeEvents,
      flatTransaction,
      transactionTree,
      acs,
    ) = fetch(partiesO)

    extractCreatedAtFrom[FlatEvent.Created, FlatEvent](
      in = flatTransactionEvents,
      createdAt = _.partial.createdAt,
    ) shouldBe expectedCreatedAt

    extractCreatedAtFrom[FlatEvent.Created, FlatEvent](
      in = flatTransaction,
      createdAt = _.partial.createdAt,
    ) shouldBe expectedCreatedAt

    extractCreatedAtFrom[TreeEvent.Created, TreeEvent](
      in = transactionTreeEvents,
      createdAt = _.partial.createdAt,
    ) shouldBe expectedCreatedAt

    extractCreatedAtFrom[TreeEvent.Created, TreeEvent](
      in = transactionTree,
      createdAt = _.partial.createdAt,
    ) shouldBe expectedCreatedAt

    acs.head.rawCreatedEvent.ledgerEffectiveTime.micros.shouldBe(
      (expectedCreatedAt.seconds * 1000000) + (expectedCreatedAt.nanos / 1000)
    )
  }

  private def extractCreatedAtFrom[O: ClassTag, E >: O](
      in: Seq[EventStorageBackend.Entry[E]],
      createdAt: O => Option[Timestamp],
  ): Timestamp = {
    in.size shouldBe 1
    in.head.event match {
      case o: O => createdAt(o).value
      case _ =>
        fail(
          s"Expected created event of type ${implicitly[reflect.ClassTag[O]].runtimeClass.getSimpleName}"
        )
    }
  }
}
