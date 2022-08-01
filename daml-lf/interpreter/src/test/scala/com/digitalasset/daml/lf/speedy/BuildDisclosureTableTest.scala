// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.command.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{IdString, Party}
import com.daml.lf.data.{FrontStack, ImmArray, Ref, Time}
import com.daml.lf.interpretation.Error.DisclosurePreprocessing
import com.daml.lf.speedy.SValue.SContractId
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import com.daml.lf.testing.parser.Implicits._

class BuildDisclosureTableTest extends AnyFreeSpec with Inside with Matchers {

  import BuildDisclosureTableTest._

  "buildDisclosureTable" - {
    "disclosure preprocessing" - {
      "template does not exist" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(ImmArray(disclosedHouseContractInvalidTemplate), pkg.pkgInterface)
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.NonExistentTemplate(
              disclosedHouseContractInvalidTemplate.templateId
            )
          )
        )
      }

      "disclosed contract key has no hash" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(ImmArray(disclosedHouseContractNoHash), pkg.pkgInterface)
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.NonExistentDisclosedContractKeyHash(
              contractId,
              disclosedHouseContractNoHash.templateId,
            )
          )
        )
      }

      "duplicate disclosed contract IDs" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(
            ImmArray(disclosedHouseContract, disclosedHouseContract),
            pkg.pkgInterface,
          )
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.DuplicateContractIds(houseTemplateId)
          )
        )
      }

      "duplicate disclosed contract key hashes" in {
        val error = intercept[SError.SErrorDamlException] {
          Speedy.buildDiscTable(
            ImmArray(disclosedHouseContract, disclosedHouseContractWithDuplicateKey),
            pkg.pkgInterface,
          )
        }

        error shouldBe SError.SErrorDamlException(
          DisclosurePreprocessing(
            DisclosurePreprocessing.DuplicateContractKeys(houseTemplateId)
          )
        )

      }
    }
  }
}

object BuildDisclosureTableTest {

  val testKeyName: String = "test-key"
  val pkg: PureCompiledPackages = SpeedyTestLib.typeAndCompile(
    p"""
       module TestMod {

         record @serializable Key = { label: Text, maintainers: List Party };

         record @serializable House = { owner: Party, key_maintainer: Party };
         template(this: House) = {
           precondition True;
           signatories (TestMod:listOf @Party (TestMod:House {owner} this));
           observers (Nil @Party);
           agreement "Agreement for TestMod:House";

           choice Destroy (self) (arg: Unit): Unit,
             controllers (TestMod:listOf @Party (TestMod:House {owner} this)),
             observers Nil @Party
             to upure @Unit ();

           key @TestMod:Key
              (TestMod:Key { label = "test-key", maintainers = (TestMod:listOf @Party (TestMod:House {key_maintainer} this)) })
              (\(key: TestMod:Key) -> (TestMod:Key {maintainers} key));
         };

         record @serializable Cave = { owner: Party };
         template(this: Cave) = {
           precondition True;
           signatories (TestMod:listOf @Party (TestMod:Cave {owner} this));
           observers (Nil @Party);
           agreement "Agreement for TestMod:Cave";

           choice Destroy (self) (arg: Unit): Unit,
             controllers (TestMod:listOf @Party (TestMod:Cave {owner} this)),
             observers Nil @Party
             to upure @Unit ();
         };

         val destroyHouse: ContractId TestMod:House -> Update Unit =
           \(contractId: ContractId TestMod:House) ->
             exercise @TestMod:House Destroy contractId ();

         val destroyCave: ContractId TestMod:Cave -> Update Unit =
           \(contractId: ContractId TestMod:Cave) ->
             exercise @TestMod:Cave Destroy contractId ();

         val listOf: forall(t:*). t -> List t =
           /\(t:*). \(x: t) ->
             Cons @t [x] (Nil @t);

         val optToList: forall(t:*). Option t -> List t  =
           /\(t:*). \(opt: Option t) ->
             case opt of
                 None -> Nil @t
               | Some x -> Cons @t [x] (Nil @t);
       }
       """
  )
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
  // FIXME: needed?
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
      SContractId(contractId),
      SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("owner"), Ref.Name.assertFromString("key_maintainer")),
        ArrayList(SValue.SParty(owner), SValue.SParty(maintainer)),
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
      SContractId(contractId),
      SValue.SRecord(
        templateId,
        ImmArray(Ref.Name.assertFromString("owner")),
        ArrayList(SValue.SParty(owner)),
      ),
      ContractMetadata(Time.Timestamp.now(), None, ImmArray.Empty),
    )
  }
}
