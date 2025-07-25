// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.syntax.either.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.ContractAuthenticator
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.packageName
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{BaseTest, FailOnShutdown, LfPartyId}
import com.digitalasset.daml.lf.transaction.{CreationTime, FatContractInstance}
import com.digitalasset.daml.lf.value.Value.{ValueText, ValueUnit}
import org.scalatest.wordspec.AsyncWordSpec

class ExtendedContractLookupTest extends AsyncWordSpec with BaseTest with FailOnShutdown {

  import com.digitalasset.canton.protocol.ExampleTransactionFactory.suffixedId

  private object dummyAuthenticator extends ContractAuthenticator {
    override def authenticateSerializable(contract: SerializableContract): Either[String, Unit] =
      Either.unit
    override def authenticateFat(contract: FatContractInstance): Either[String, Unit] = Either.unit
    override def verifyMetadata(
        contract: GenContractInstance,
        metadata: ContractMetadata,
    ): Either[String, Unit] = Either.unit
  }

  private val coid00: LfContractId = suffixedId(0, 0)
  private val coid01: LfContractId = suffixedId(0, 1)
  private val coid10: LfContractId = suffixedId(1, 0)
  private val coid11: LfContractId = suffixedId(1, 1)
  private val coid20: LfContractId = suffixedId(2, 0)
  private val coid21: LfContractId = suffixedId(2, 1)

  private val let0: CantonTimestamp = CantonTimestamp.Epoch
  private val let1: CantonTimestamp = CantonTimestamp.ofEpochMilli(1)

  "ExtendedContractLookup" should {

    val instance0Template = ExampleContractFactory.templateId
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

    val metadata2 =
      ContractMetadata.tryCreate(signatories = Set(alice), stakeholders = Set(alice, bob), None)

    val overwrites = Map(
      coid01 -> ExampleContractFactory.build(
        overrideContractId = Some(coid01),
        signatories = metadata2.signatories,
        stakeholders = metadata2.stakeholders,
        createdAt = CreationTime.CreatedAt(let0.toLf),
      ),
      coid20 -> ExampleContractFactory.build(
        overrideContractId = Some(coid20),
        signatories = metadata2.signatories,
        stakeholders = metadata2.stakeholders,
        createdAt = CreationTime.CreatedAt(let1.toLf),
      ),
      coid21 -> ExampleContractFactory.build(
        overrideContractId = Some(coid21),
        signatories = metadata2.signatories,
        stakeholders = metadata2.stakeholders,
        createdAt = CreationTime.CreatedAt(let0.toLf),
      ),
    )

    val extendedStore = new ExtendedContractLookup(
      overwrites,
      Map(key00 -> Some(coid00), key1 -> None),
      dummyAuthenticator,
    )

    "not make up contracts" in {
      for {
        result <- extendedStore.lookup(coid11).value
      } yield {
        assert(result.isEmpty)
      }
    }

    "find a contract" in {
      for {
        result <- extendedStore.lookup(coid01).value
      } yield {
        assert(result.contains(overwrites(coid01)))
      }
    }

    "find an additional created contract" in {
      for {
        result <- extendedStore.lookup(coid21).value
      } yield {
        assert(result.contains(overwrites(coid21)))
      }
    }

    "complain about inconsistent contract ids" in {
      val contract =
        ExampleContractFactory.build(overrideContractId = Some(coid01))

      assertThrows[IllegalArgumentException](
        new ExtendedContractLookup(
          Map(coid10 -> contract),
          Map.empty,
          dummyAuthenticator,
        )
      )
    }

    "find exactly the keys in the provided map" in {
      for {
        result00 <- valueOrFailUS(extendedStore.lookupKey(key00))(show"lookup $key00")
        result1 <- valueOrFailUS(extendedStore.lookupKey(key1))(show"lookup $key1")
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

  /** @param value
    *   True iff the contract is a divulged contract
    */
  final case class Divulgence(value: Boolean) extends AnyVal
}
