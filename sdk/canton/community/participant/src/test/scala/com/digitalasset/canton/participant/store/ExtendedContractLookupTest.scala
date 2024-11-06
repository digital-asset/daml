// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.SerializableContractAuthenticator
import com.digitalasset.canton.participant.store.ExtendedContractLookupTest.Divulgence
import com.digitalasset.canton.participant.store.memory.InMemoryContractStore
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  asSerializable,
  contractInstance,
  packageName,
}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, LfPartyId, RequestCounter}
import com.digitalasset.daml.lf.value.Value.{ValueText, ValueUnit}
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class ExtendedContractLookupTest extends AsyncWordSpec with BaseTest {

  import com.digitalasset.canton.protocol.ExampleTransactionFactory.suffixedId

  object dummyAuthenticator extends SerializableContractAuthenticator {
    override def authenticate(contract: SerializableContract): Either[String, Unit] = Either.unit
    override def verifyMetadata(
        contract: SerializableContract,
        metadata: ContractMetadata,
    ): Either[String, Unit] = Either.unit
  }

  val coid00: LfContractId = suffixedId(0, 0)
  private val coid01: LfContractId = suffixedId(0, 1)
  private val coid10: LfContractId = suffixedId(1, 0)
  private val coid11: LfContractId = suffixedId(1, 1)
  private val coid20: LfContractId = suffixedId(2, 0)
  private val coid21: LfContractId = suffixedId(2, 1)

  private val let0: CantonTimestamp = CantonTimestamp.Epoch
  private val let1: CantonTimestamp = CantonTimestamp.ofEpochMilli(1)

  private val rc0 = RequestCounter(0)
  private val rc1 = RequestCounter(1)
  private val rc2 = RequestCounter(2)

  private def mk(
      entries: (
          LfContractId,
          LfContractInst,
          ContractMetadata,
          CantonTimestamp,
          RequestCounter,
          Divulgence,
      )*
  ): Future[ContractLookup] = {
    val store = new InMemoryContractStore(loggerFactory)
    entries
      .parTraverse_ {
        case (id, contractInstance, metadata, ledgerTime, requestCounter, Divulgence(true)) =>
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
              Divulgence(false),
            ) =>
          store.storeCreatedContract(
            requestCounter,
            asSerializable(id, contractInstance, metadata, ledgerTime),
          )
      }
      .map((_: Unit) => store)
  }

  "ExtendedContractLookup" should {

    val instance0 = contractInstance()
    val instance0Template = instance0.unversioned.template
    val instance1 = contractInstance()
    val key00: LfGlobalKey =
      LfGlobalKey.build(instance0Template, ValueUnit, packageName).value
    val key1: LfGlobalKey =
      LfGlobalKey.build(instance0Template, ValueText("abc"), packageName).value
    val forbiddenKey: LfGlobalKey =
      LfGlobalKey
        .build(instance0Template, ValueText("forbiddenKey"), packageName)
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
      (coid00, instance0, metadata00, let0, rc0, Divulgence(true)),
      (coid01, instance1, metadata1, let1, rc1, Divulgence(false)),
      (coid01, instance1, metadata1, let1, rc1, Divulgence(true)),
      (coid10, instance1, metadata1, let0, rc2, Divulgence(false)),
    )

    val overwrites = Map(
      coid01 -> StoredContract.fromCreatedContract(
        asSerializable(coid01, instance0, metadata2, let0),
        rc2,
      ),
      coid20 -> StoredContract
        .fromDivulgedContract(asSerializable(coid20, instance0, metadata2, let1), rc1),
      coid21 -> StoredContract.fromCreatedContract(
        asSerializable(coid21, instance0, metadata2, let0),
        rc1,
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

object ExtendedContractLookupTest {

  /** @param value True iff the contract is a divulged contract
    */
  final case class Divulgence(value: Boolean) extends AnyVal
}
