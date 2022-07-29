// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command.{ContractMetadata, DisclosedContract}
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{IdString, Party}
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.daml.lf.language.Ast
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ValueInt64, ValueList, ValueParty, ValueRecord}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class PreprocessorSpec extends AnyWordSpec with Inside with Matchers {

  import PreprocessorSpec._
  import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}

  "preprocessor" should {
    "returns correct result when resuming" in {
      val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
      val intermediaryResult = preprocessor
        .translateValue(
          Ast.TTyCon("Mod:Record"),
          ValueRecord("", ImmArray("owners" -> parties, "data" -> ValueInt64(42))),
        )
      intermediaryResult shouldBe a[ResultNeedPackage[_]]
      val finalResult = intermediaryResult.consume(_ => None, pkgs.get, _ => None)
      finalResult shouldBe a[Right[_, _]]
    }

    "returns correct error when resuming" in {
      val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
      val intermediaryResult = preprocessor
        .translateValue(
          Ast.TTyCon("Mod:Record"),
          ValueRecord(
            "",
            ImmArray("owners" -> parties, "wrong_field" -> ValueInt64(42)),
          ),
        )
      intermediaryResult shouldBe a[ResultNeedPackage[_]]
      val finalResult = intermediaryResult.consume(_ => None, pkgs.get, _ => None)
      inside(finalResult) { case Left(Error.Preprocessing(error)) =>
        error shouldBe a[Error.Preprocessing.TypeMismatch]
      }
    }

    "disclosure preprocessing" should {
      "disclosed contract key has no hash, but its template has a key" in {
        val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
        val result =
          preprocessor.preprocessDisclosedContracts(ImmArray(disclosedHouseContractNoHash))

        inside(result) {
          case ResultError(
                Error.Preprocessing(Error.Preprocessing.BadDisclosedContract(message))
              ) =>
            message shouldBe s"Disclosed contract template $houseTemplateId has a key defined, but the disclosed contract $contractId has no key hash"
        }
      }

      "disclosed contract key has a hash, but its template has no keys" in {
        val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
        val result = preprocessor.preprocessDisclosedContracts(ImmArray(disclosedHouseContract))

        inside(result) {
          case ResultError(
                Error.Preprocessing(Error.Preprocessing.BadDisclosedContract(message))
              ) =>
            message shouldBe s"Disclosed contract template $houseTemplateId has no key defined, but the disclosed contract $contractId has a key hash"
        }
      }

      "duplicate disclosed contract IDs" in {
        val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
        val result = preprocessor.preprocessDisclosedContracts(
          ImmArray(disclosedHouseContract, disclosedHouseContract)
        )

        inside(result) {
          case ResultError(
                Error.Preprocessing(Error.Preprocessing.BadDisclosedContract(message))
              ) =>
            message shouldBe s"Duplicate contract ID: $disclosureContractId"
        }
      }

      "duplicate disclosed contract key hashes" in {
        val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
        val result = preprocessor.preprocessDisclosedContracts(
          ImmArray(disclosedHouseContract, disclosedHouseContractWithDuplicateKey)
        )

        inside(result) {
          case ResultError(
                Error.Preprocessing(Error.Preprocessing.BadDisclosedContract(message))
              ) =>
            message shouldBe s"Duplicate contract key hash for contract ID: $altDisclosureContractId"
        }
      }
    }
  }
}

object PreprocessorSpec {

  import com.daml.lf.testing.parser.Implicits._

  implicit val defaultPackageId: Ref.PackageId =
    defaultParserParameters.defaultPackageId

  val pkg =
    p"""
        module Mod {

          record @serializable Record = { owners: List Party, data : Int64 };

          template (this : Record) = {
            precondition True;
            signatories Mod:Record {owners} this;
            observers Mod:Record {owners} this;
            agreement "Agreement";
            key @(List Party) (Mod:Record {owners} this) (\ (parties: List Party) -> parties);
          };

        }
    """
  val pkgs = Map(defaultPackageId -> pkg)
  val alice = Ref.Party.assertFromString("Alice")
  val parties = ValueList(FrontStack(ValueParty(alice)))
  val testKeyName: String = "test-key"
  val contractId: ContractId = Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-contract-id"))
  val disclosureContractId: ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-disclosure-contract-id"))
  val altDisclosureContractId: ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test-alternative-disclosure-contract-id"))
  val disclosureParty: IdString.Party = Ref.Party.assertFromString("disclosureParty")
  val maintainerParty: IdString.Party = Ref.Party.assertFromString("maintainerParty")
  val houseTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:House")
  val invalidTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:Invalid")
  val houseTemplateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:TestMod:House")
  val caveTemplateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:TestMod:Cave")
  val disclosedHouseContract: DisclosedContract =
    buildDisclosedHouseContract(disclosureContractId, disclosureParty, maintainerParty)
  val disclosedHouseContractInvalidTemplate: DisclosedContract = buildDisclosedHouseContract(
    contractId,
    disclosureParty,
    maintainerParty,
    templateId = invalidTemplateId,
    withHash = false,
  )
  val disclosedCaveContractInvalidTemplate: DisclosedContract = buildDisclosedCaveContract(
    contractId,
    disclosureParty,
    templateId = invalidTemplateId,
  )
  val disclosedHouseContractNoHash: DisclosedContract =
    buildDisclosedHouseContract(contractId, disclosureParty, maintainerParty, withHash = false)
  val disclosedHouseContractWithDuplicateKey: DisclosedContract =
    buildDisclosedHouseContract(altDisclosureContractId, disclosureParty, maintainerParty)

  def buildDisclosedHouseContract(
      contractId: ContractId,
      owner: Party,
      maintainer: Party,
      templateId: Ref.Identifier = houseTemplateId,
      withHash: Boolean = true,
  ): DisclosedContract = {
    val key = Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueText(testKeyName),
        None -> Value.ValueList(FrontStack.from(ImmArray(Value.ValueParty(maintainer)))),
      ),
    )
    val keyHash: Option[Hash] =
      if (withHash) Some(crypto.Hash.assertHashContractKey(houseTemplateType, key)) else None

    DisclosedContract(
      templateId,
      contractId,
      Value.ValueRecord(
        Some(templateId),
        ImmArray(
          Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(owner),
          Some(Ref.Name.assertFromString("key_maintainer")) -> Value.ValueParty(maintainer),
        ),
      ),
      ContractMetadata(Time.Timestamp.now(), keyHash, ImmArray.Empty),
    )
  }

  def buildDisclosedCaveContract(
      contractId: ContractId,
      owner: Party,
      templateId: Ref.Identifier = caveTemplateId,
  ): DisclosedContract = {
    DisclosedContract(
      templateId,
      contractId,
      Value.ValueRecord(
        Some(templateId),
        ImmArray(Some(Ref.Name.assertFromString("owner")) -> Value.ValueParty(owner)),
      ),
      ContractMetadata(Time.Timestamp.now(), None, ImmArray.Empty),
    )
  }
}
