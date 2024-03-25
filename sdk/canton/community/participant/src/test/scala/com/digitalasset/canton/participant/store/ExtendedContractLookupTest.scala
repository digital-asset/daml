// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.parallel.*
import com.daml.lf.value.Value.{ValueText, ValueUnit}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  transactionId,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, LfPartyId, RequestCounter}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ExtendedContractLookupTest extends AsyncWordSpec with BaseTest {

  import com.digitalasset.canton.protocol.ExampleTransactionFactory.suffixedId

  object dummyAuthenticator extends SerializableContractAuthenticator {
    override def authenticate(contract: SerializableContract): Either[String, Unit] = Right(())
    override def verifyMetadata(
        contract: SerializableContract,
        metadata: ContractMetadata,
    ): Either[String, Unit] = Right(())
  }

  val coid00: LfContractId = suffixedId(0, 0)
  val coid01: LfContractId = suffixedId(0, 1)
  val coid10: LfContractId = suffixedId(1, 0)
  val coid11: LfContractId = suffixedId(1, 1)
  val coid20: LfContractId = suffixedId(2, 0)
  val coid21: LfContractId = suffixedId(2, 1)

  val let0: CantonTimestamp = CantonTimestamp.Epoch
  val let1: CantonTimestamp = CantonTimestamp.ofEpochMilli(1)

  val alice: LfPartyId = LfPartyId.assertFromString("alice")
  val bob: LfPartyId = LfPartyId.assertFromString("bob")
  val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
  val david: LfPartyId = LfPartyId.assertFromString("david")
  val eleonore: LfPartyId = LfPartyId.assertFromString("eleonore")

  val rc0 = RequestCounter(0)
  val rc1 = RequestCounter(1)
  val rc2 = RequestCounter(2)

  def mk(
      entries: (
          LfContractId,
          LfContractInst,
          ContractMetadata,
          CantonTimestamp,
          RequestCounter,
          Option[TransactionId],
      )*
  ): Future[ContractLookup] = {
    val store = new InMemoryContractStore(loggerFactory)
    entries
      .parTraverse_ {
        case (id, contractInstance, metadata, ledgerTime, requestCounter, None) =>
          store.storeDivulgedContract(
            requestCounter,
            asSerializable(id, contractInstance, metadata, ledgerTime),
          )
        case (
              id,
              contractInstance,
              metadata,
              ledgerTime,
              requestCounter,
              Some(creatingTransactionId),
            ) =>
          store.storeCreatedContract(
            requestCounter,
            creatingTransactionId,
            asSerializable(id, contractInstance, metadata, ledgerTime),
          )
      }
      .map((_: Unit) => store)
  }

  "ExtendedContractLookup" should {

    val instance0 = contractInstance()
    val instance0Template = instance0.unversioned.template
    val instance1 = contractInstance()
    val transactionId0 = transactionId(0)
    val transactionId1 = transactionId(1)
    val transactionId2 = transactionId(2)
    val key00: LfGlobalKey =
      LfGlobalKey.build(instance0Template, ValueUnit).value
    val key1: LfGlobalKey =
      LfGlobalKey.build(instance0Template, ValueText("abc")).value
    val forbiddenKey: LfGlobalKey =
      LfGlobalKey
        .build(instance0Template, ValueText("forbiddenKey"))
        .value
    val alice = LfPartyId.assertFromString("alice")
    val bob = LfPartyId.assertFromString("bob")
    val metadata00 = ContractMetadata.tryCreate(
      signatories = Set(alice),
      stakeholders = Set(alice),
      Some(ExampleTransactionFactory.globalKeyWithMaintainers(key00, Set(alice))),
    )
    val metadata1 =
      ContractMetadata.tryCreate(signatories = Set(alice), stakeholders = Set(alice), None)
    val metadata2 =
      ContractMetadata.tryCreate(signatories = Set(alice), stakeholders = Set(alice, bob), None)

    val preloadedStoreF = mk(
      (coid00, instance0, metadata00, let0, rc0, None),
      (coid01, instance1, metadata1, let1, rc1, Some(transactionId1)),
      (coid01, instance1, metadata1, let1, rc1, None),
      (coid10, instance1, metadata1, let0, rc2, Some(transactionId2)),
    )

    val overwrites = Map(
      coid01 -> StoredContract.fromCreatedContract(
        asSerializable(coid01, instance0, metadata2, let0),
        rc2,
        transactionId0,
      ),
      coid20 -> StoredContract
        .fromDivulgedContract(asSerializable(coid20, instance0, metadata2, let1), rc1),
      coid21 -> StoredContract.fromCreatedContract(
        asSerializable(coid21, instance0, metadata2, let0),
        rc1,
        transactionId1,
      ),
    )

    val extendedStoreF = preloadedStoreF.map(
      new ExtendedContractLookup(
        _,
        overwrites,
        Map(key00 -> Some(coid00), key1 -> None),
        dummyAuthenticator,
      )
    )

    "return un-overwritten contracts" in {
      for {
        preloadedStore <- preloadedStoreF
        extendedStore <- extendedStoreF
        _ <- List(coid00, coid10).parTraverse_ { coid =>
          for {
            resultExtended <- extendedStore.lookup(coid).value
            resultBacking <- preloadedStore.lookup(coid).value
          } yield assert(resultExtended == resultBacking)
        }
      } yield succeed
    }

    "not make up contracts" in {
      for {
        extendedStore <- extendedStoreF
        result <- extendedStore.lookup(coid11).value
      } yield {
        assert(result.isEmpty)
      }
    }

    "find an overwritten contract" in {
      for {
        extendedStore <- extendedStoreF
        result <- extendedStore.lookup(coid01).value
      } yield {
        assert(result.contains(overwrites(coid01)))
      }
    }

    "find an additional divulged contract" in {
      for {
        extendedStore <- extendedStoreF
        result <- extendedStore.lookup(coid20).value
      } yield {
        assert(result.contains(overwrites(coid20)))
      }
    }

    "find an additional created contract" in {
      for {
        extendedStore <- extendedStoreF
        result <- extendedStore.lookup(coid21).value
      } yield {
        assert(result.contains(overwrites(coid21)))
      }
    }

    "complain about inconsistent contract ids" in {
      val contract = StoredContract.fromDivulgedContract(
        asSerializable(coid01, instance1, metadata1, let0),
        rc0,
      )
      for {
        preloadedStore <- preloadedStoreF
      } yield {
        assertThrows[IllegalArgumentException](
          new ExtendedContractLookup(
            preloadedStore,
            Map(coid10 -> contract),
            Map.empty,
            dummyAuthenticator,
          )
        )
      }
    }

    "find exactly the keys in the provided map" in {
      for {
        extendedStore <- extendedStoreF
        result00 <- valueOrFail(extendedStore.lookupKey(key00))(show"lookup $key00")
        result1 <- valueOrFail(extendedStore.lookupKey(key1))(show"lookup $key1")
        forbidden <- extendedStore.lookupKey(forbiddenKey).value
      } yield {
        result00 shouldBe Some(coid00)
        result1 shouldBe None
        forbidden shouldBe None
      }
    }
  }
}
