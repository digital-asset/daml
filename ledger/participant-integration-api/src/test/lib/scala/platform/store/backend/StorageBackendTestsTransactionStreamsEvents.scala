// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.backend

import com.daml.ledger.api.v1.contract_metadata.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.{Bytes, Ref}
import com.daml.platform.store.backend.EventStorageBackend.FilterParams
import com.daml.platform.store.backend.common.{
  EventPayloadSourceForFlatTx,
  EventPayloadSourceForTreeTx,
}
import com.daml.platform.store.dao.events.Raw.{FlatEvent, TreeEvent}
import com.google.protobuf.ByteString
import com.google.protobuf.timestamp.Timestamp
import org.scalatest.{Assertion, OptionValues}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

// TODO: Complete test suite for asserting flat/tree/ACS streams contents
private[backend] trait StorageBackendTestsTransactionStreamsEvents
    extends Matchers
    with OptionValues
    with StorageBackendSpec {
  this: AnyFlatSpec =>

  import StorageBackendTestValues._

  behavior of "StorageBackend events"

  it should "return the correct create contract metadata" in {
    val signatory = "party"
    val driverMetadata = Bytes.assertFromString("00abcd")
    val someHash = Hash.assertFromString("00" * 31 + "ff")

    testCreateContractMetadata(
      signatory,
      dtoCreate(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        driverMetadata = Some(driverMetadata.toByteArray),
        signatory = signatory,
        keyHash = Some(someHash.toHexString),
      ),
      ContractMetadata(
        createdAt = Some(Timestamp(someTime.toInstant)),
        contractKeyHash = someHash.bytes.toByteString,
        driverMetadata = driverMetadata.toByteString,
      ),
    )
  }

  it should "allow missing contract key hash and driver metadata" in {
    val signatory = "party"

    testCreateContractMetadata(
      signatory,
      dtoCreate(
        offset = offset(1),
        eventSequentialId = 1L,
        contractId = hashCid("#1"),
        driverMetadata = None,
        signatory = signatory,
        keyHash = None,
      ),
      ContractMetadata(
        createdAt = Some(Timestamp(someTime.toInstant)),
        contractKeyHash = ByteString.EMPTY,
        driverMetadata = ByteString.EMPTY,
      ),
    )
  }

  private def testCreateContractMetadata(
      signatory: String,
      create: DbDto.EventCreate,
      expectedCreateContractMetadata: ContractMetadata,
  ): Assertion = {
    val someParty = Ref.Party.assertFromString(signatory)

    val createTransactionId = dtoTransactionId(create)

    executeSql(backend.parameter.initializeParameters(someIdentityParams))
    executeSql(ingest(Vector(create), _))
    executeSql(updateLedgerEnd(offset(1), 1L))

    val filter = FilterParams(Set(someParty), Set.empty)

    val flatTransactionEvents = executeSql(
      backend.event.transactionStreamingQueries.fetchEventPayloadsFlat(
        EventPayloadSourceForFlatTx.Create
      )(eventSequentialIds = Seq(1L), Set(someParty))
    )
    val transactionTreeEvents = executeSql(
      backend.event.transactionStreamingQueries.fetchEventPayloadsTree(
        EventPayloadSourceForTreeTx.Create
      )(eventSequentialIds = Seq(1L), Set(someParty))
    )
    val flatTransaction = executeSql(backend.event.flatTransaction(createTransactionId, filter))
    val transactionTree = executeSql(backend.event.transactionTree(createTransactionId, filter))
    val acs = executeSql(backend.event.activeContractEventBatch(Seq(1L), Set(someParty), 1L))

    extractContractMetadataFrom[FlatEvent.Created, FlatEvent](
      in = flatTransactionEvents,
      contractMetadata = _.partial.metadata,
    ) shouldBe expectedCreateContractMetadata

    extractContractMetadataFrom[FlatEvent.Created, FlatEvent](
      in = flatTransaction,
      contractMetadata = _.partial.metadata,
    ) shouldBe expectedCreateContractMetadata

    extractContractMetadataFrom[TreeEvent.Created, TreeEvent](
      in = transactionTreeEvents,
      contractMetadata = _.partial.metadata,
    ) shouldBe expectedCreateContractMetadata

    extractContractMetadataFrom[TreeEvent.Created, TreeEvent](
      in = transactionTree,
      contractMetadata = _.partial.metadata,
    ) shouldBe expectedCreateContractMetadata

    extractContractMetadataFrom[FlatEvent.Created, FlatEvent](
      in = acs,
      contractMetadata = _.partial.metadata,
    ) shouldBe expectedCreateContractMetadata
  }

  private def extractContractMetadataFrom[O: ClassTag, E >: O](
      in: Seq[EventStorageBackend.Entry[E]],
      contractMetadata: O => Option[ContractMetadata],
  ): ContractMetadata = {
    in.size shouldBe 1
    in.head.event match {
      case o: O => contractMetadata(o).value
      case _ =>
        fail(
          s"Expected created event of type ${implicitly[reflect.ClassTag[O]].runtimeClass.getSimpleName}"
        )
    }
  }
}
