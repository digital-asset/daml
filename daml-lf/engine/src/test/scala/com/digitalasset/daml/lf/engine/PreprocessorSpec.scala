// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.lf.command.ContractMetadata
import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.Party
import com.daml.lf.data.{Bytes, FrontStack, ImmArray, Ref, Time}
import com.daml.lf.language.Ast
import com.daml.lf.speedy.{ArrayList, SValue}
import com.daml.lf.value.Value.{ContractId, ValueInt64, ValueList, ValueParty, ValueRecord}
import org.scalatest.{Inside, Inspectors}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.daml.lf.testing.parser.Implicits._
import com.daml.lf.transaction.test.TransactionBuilder.Implicits.{defaultPackageId => _, _}
import com.daml.lf.value.Value
import org.scalatest.matchers.{MatchResult, Matcher}

class PreprocessorSpec extends AnyWordSpec with Inside with Matchers with Inspectors {

  import PreprocessorSpec._

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

    "preprocessDisclosedContracts" should {
      "normalised contracts are accepted" in {
        val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
        val intermediaryResult =
          preprocessor.preprocessDisclosedContracts(ImmArray(normalizedContract))

        intermediaryResult shouldBe a[ResultNeedPackage[_]]

        val finalResult = intermediaryResult.consume(_ => None, pkgs.get, _ => None)

        inside(finalResult) { case Right(disclosedContracts) =>
          all(disclosedContracts.toList) should acceptDisclosedContract
        }
      }

      "non-normalized contracts are accepted" in {
        val preprocessor = new preprocessing.Preprocessor(ConcurrentCompiledPackages())
        val intermediaryResult =
          preprocessor.preprocessDisclosedContracts(ImmArray(nonNormalizedContract))

        intermediaryResult shouldBe a[ResultNeedPackage[_]]

        val finalResult = intermediaryResult.consume(_ => None, pkgs.get, _ => None)

        inside(finalResult) { case Right(disclosedContracts) =>
          all(disclosedContracts.toList) should acceptDisclosedContract
        }
      }
    }
  }
}

object PreprocessorSpec {

  implicit val defaultPackageId: Ref.PackageId = defaultParserParameters.defaultPackageId

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
  val alice: Party = Ref.Party.assertFromString("Alice")
  val parties: ValueList = ValueList(FrontStack(ValueParty(alice)))
  val testKeyName: String = "test-key"
  val contractId: ContractId =
    Value.ContractId.V1.assertBuild(
      crypto.Hash.hashPrivateKey("test-contract-id"),
      Bytes.assertFromString("deadbeef"),
    )
  val templateId: Ref.Identifier = Ref.Identifier.assertFromString("-pkgId-:Mod:Record")
  val templateType: Ref.TypeConName = Ref.TypeConName.assertFromString("-pkgId-:Mod:Record")
  val key: Value.ValueRecord = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueText(testKeyName),
      None -> Value.ValueList(FrontStack.from(ImmArray(ValueParty(alice)))),
    ),
  )
  val keyHash: Hash = crypto.Hash.assertHashContractKey(templateType, key)
  val normalizedContract: command.DisclosedContract =
    buildDisclosedContract(keyHash, withNormalization = true)
  val nonNormalizedContract: command.DisclosedContract =
    buildDisclosedContract(keyHash, withNormalization = false)

  def buildDisclosedContract(
      keyHash: Hash,
      withNormalization: Boolean,
  ): command.DisclosedContract = {
    command.DisclosedContract(
      templateId,
      contractId,
      Value.ValueRecord(
        if (withNormalization) None else Some(templateId),
        ImmArray(
          (if (withNormalization) None else Some(Ref.Name.assertFromString("owners"))) -> parties,
          (if (withNormalization) None else Some(Ref.Name.assertFromString("data"))) -> Value
            .ValueInt64(42L),
        ),
      ),
      ContractMetadata(Time.Timestamp.now(), Some(keyHash), ImmArray.Empty),
    )
  }

  @SuppressWarnings(
    Array(
      "org.wartremover.warts.Product",
      "org.wartremover.warts.Serializable",
      "org.wartremover.warts.JavaSerializable",
    )
  )
  def acceptDisclosedContract: Matcher[speedy.DisclosedContract] = Matcher { disclosedContract =>
    val testResult = disclosedContract.argument match {
      case SValue.SRecord(`templateId`, fields, values) =>
        fields == ImmArray(
          Ref.Name.assertFromString("owners"),
          Ref.Name.assertFromString("data"),
        ) &&
        values == ArrayList(
          SValue.SList(FrontStack(SValue.SParty(alice))),
          SValue.SInt64(42L),
        )

      case _ =>
        false
    }

    MatchResult(
      testResult,
      s"Failed to accept disclosed contract: $disclosedContract",
      s"Failed to accept disclosed contract: $disclosedContract",
    )
  }
}
