// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.{Compiler, SValue}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  Node,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.{Value => V}
import org.scalatest.{EitherValues, Inside}
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq

/** Tests for [[Engine.validateContractInstance]]. This method is a proxy for [fetchTemplate] which is
  * already thoroughly tested in [[EngineTest]] and [[com.digitalasset.daml.lf.speedy.ValueTranslatorSpec]] so we only
  * test here that things are properly wired. For instance, we don't test every single way a contract can be ill-typed.
  */
class ValidateContractInstanceSpec
    extends AnyWordSpec
    with EitherValues
    with Matchers
    with TableDrivenPropertyChecks
    with Inside {

  implicit def logContext: LoggingContext = LoggingContext.ForTesting

  val pkgId1 = Ref.PackageId.assertFromString("-packageId1-")
  val pkgId2 = Ref.PackageId.assertFromString("-packageId2-")
  val pkgId3 = Ref.PackageId.assertFromString("-packageId3-")
  val pkgId4 = Ref.PackageId.assertFromString("-packageId4-")
  val pkgId5 = Ref.PackageId.assertFromString("-packageId5-")
  val pkgId6 = Ref.PackageId.assertFromString("-packageId6-")

  val defaultParserParameters: ParserParameters[this.type] = ParserParameters.default

  val pkg1 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId1
    )
    p""" metadata ( 'test-pkg' : '1.0.0' )
        module M {
          record @serializable T = { p1: Party, p2: Party, cid: ContractId M:T };
          template (this : T) = {
            precondition True;
            signatories Cons @Party [M:T {p1} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  // A valid upgrade of pkg1
  val pkg2 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId2
    )
    p""" metadata ( 'test-pkg' : '2.0.0' )
        module M {
          record @serializable T = { p1: Party, p2: Party, cid: ContractId M:T, extra: Option Int64 };
          template (this : T) = {
            precondition True;
            signatories Cons @Party [M:T {p1} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  // An invalid upgrade of pkg1: the type of T is not compatible with that of v1
  val pkg3 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId3
    )
    p""" metadata ( 'test-pkg' : '2.0.0' )
        module M {
          record @serializable T = { p1: Party, p2: Party, cid: ContractId M:T, extra: Int64 };
          template (this : T) = {
            precondition True;
            signatories Cons @Party [M:T {p1} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  // An invalid upgrade of pkg1: the precondition is false
  val pkg4 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId4
    )
    p""" metadata ( 'test-pkg' : '2.0.0' )
        module M {
          record @serializable T = { p1: Party, p2: Party, cid: ContractId M:T };
          template (this : T) = {
            precondition False;
            signatories Cons @Party [M:T {p1} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  // An invalid upgrade of pkg1: the signatories differ
  val pkg5 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId5
    )
    p""" metadata ( 'test-pkg' : '2.0.0' )
        module M {
          record @serializable T = { p1: Party, p2: Party, cid: ContractId M:T };
          template (this : T) = {
            precondition False;
            signatories Cons @Party [M:T {p2} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  // An invalid upgrade of pkg1: the observers differ
  val pkg6 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId6
    )
    p""" metadata ( 'test-pkg' : '2.0.0' )
        module M {
          record @serializable T = { p1: Party, p2: Party, cid: ContractId M:T };
          template (this : T) = {
            precondition False;
            signatories Cons @Party [M:T {p1} this] (Nil @Party);
            observers Cons @Party [M:T {p2} this] (Nil @Party);
          };
        }
    """
  }

  val compilerConfig = Compiler.Config.Default
  val compiledPkgs = PureCompiledPackages.build(Map(pkgId1 -> pkg1, pkgId2 -> pkg2), compilerConfig)

  private def newEngine = new Engine(
    EngineConfig(LanguageVersion.stableLfVersionsRange)
  )

  val alice = Party.assertFromString("alice")
  val bob = Party.assertFromString("bob")
  val packageName = Ref.PackageName.assertFromString("test-pkg")
  val templateId = Ref.Identifier(pkgId1, Ref.QualifiedName.assertFromString("M:T"))
  val cid0 = newSuffixedCid
  val cid1 = newSuffixedCid
  def createArg(cidInContractArg: V.ContractId) =
    V.ValueRecord(
      None,
      ImmArray(
        None -> V.ValueParty(alice),
        None -> V.ValueParty(bob),
        None -> V.ValueContractId(cidInContractArg),
      ),
    )
  val createNode = Node.Create(
    coid = TransactionBuilder.newCid,
    packageName = packageName,
    templateId = Ref.Identifier(pkgId1, Ref.QualifiedName.assertFromString("M:T")),
    arg = createArg(cid0),
    signatories = Set(alice),
    stakeholders = Set(alice),
    keyOpt = None,
    version = SerializationVersion.minVersion,
  )
  val contractInstance = FatContractInstance.fromCreateNode(
    createNode,
    CreationTime.CreatedAt(Time.Timestamp.now()),
    Bytes.Empty,
  )
  val cidMapping = Map(cid0 -> cid1).withDefault(identity)

  val expectedLegacyHash = Hash
    .hashContractInstance(templateId, createArg(cid1), packageName, upgradeFriendly = false)
    .value
  val expectedUpgradeFriendlyHash = Hash
    .hashContractInstance(templateId, createArg(cid1), packageName, upgradeFriendly = true)
    .value
  val expectedTypedNormalFormHash = SValueHash
    .hashContractInstance(
      packageName,
      templateId.qualifiedName,
      SValue.SRecord(
        templateId,
        ImmArray(
          Ref.Name.assertFromString("p1"),
          Ref.Name.assertFromString("p2"),
          Ref.Name.assertFromString("cid"),
        ),
        ArraySeq(SValue.SParty(alice), SValue.SParty(bob), SValue.SContractId(cid1)),
      ),
    )
    .value

  "validateContractInstance" should {

    "succeed on valid upgrades" in {

      val hashes = Table(
        ("hashingMethod", "expectedHash"),
        (Hash.HashingMethod.Legacy, expectedLegacyHash),
        (Hash.HashingMethod.UpgradeFriendly, expectedUpgradeFriendlyHash),
        (Hash.HashingMethod.TypedNormalForm, expectedTypedNormalFormHash),
      )

      val targetPackageIds = Table(
        ("targetPackageId", "targetPackage"),
        (pkgId1, pkg1),
        (pkgId2, pkg2),
      )

      forEvery(hashes) { (hashingMethod, expectedHash) =>
        forEvery(targetPackageIds) { (targetPackageId, targetPackage) =>
          var idValidatorCalledWithExpectedHash = false
          val result = newEngine
            .validateContractInstance(
              contractInstance,
              targetPackageId,
              cidMapping,
              hashingMethod,
              idValidator = { h =>
                idValidatorCalledWithExpectedHash = (h == expectedHash)
                idValidatorCalledWithExpectedHash
              },
            )
            .consume(pkgs = Map(targetPackageId -> targetPackage))

          idValidatorCalledWithExpectedHash shouldBe true
          result shouldBe Right(Right(()))
        }
      }
    }

    "return a ResultDone(Left(_)) when authentication fails" in {

      val hashes = Table(
        "hashingMethod",
        Hash.HashingMethod.Legacy,
        Hash.HashingMethod.UpgradeFriendly,
        Hash.HashingMethod.TypedNormalForm,
      )

      val targetPackageIds = Table(
        ("targetPackageId", "targetPackage"),
        (pkgId1, pkg1),
        (pkgId2, pkg2),
      )

      forEvery(hashes) { hashingMethod =>
        forEvery(targetPackageIds) { (targetPackageId, targetPackage) =>
          val result = newEngine
            .validateContractInstance(
              contractInstance,
              targetPackageId,
              cidMapping,
              hashingMethod,
              idValidator = _ => false, // We pretend that the authentication always fails
            )
            .consume(pkgs = Map(targetPackageId -> targetPackage))

          inside(result) { case Right(res) =>
            res shouldBe a[Left[_, _]]
          }
        }
      }
    }

    "return a ResultDone(Left(_)) when type-checking fails" in {

      val hashes = Table(
        "hashingMethod",
        Hash.HashingMethod.Legacy,
        Hash.HashingMethod.UpgradeFriendly,
        Hash.HashingMethod.TypedNormalForm,
      )

      forEvery(hashes) { hashingMethod =>
        val result = newEngine
          .validateContractInstance(
            contractInstance,
            pkgId3, // This package cannot type-check contractInstance
            cidMapping,
            hashingMethod,
            idValidator = _ => true,
          )
          .consume(pkgs = Map(pkgId3 -> pkg3))

        inside(result) { case Right(res) =>
          res shouldBe a[Left[_, _]]
        }
      }
    }

    "return a ResultDone(Left(_)) when the ensure clause evaluates to false" in {

      val hashes = Table(
        "hashingMethod",
        Hash.HashingMethod.Legacy,
        Hash.HashingMethod.UpgradeFriendly,
        Hash.HashingMethod.TypedNormalForm,
      )

      forEvery(hashes) { hashingMethod =>
        val result = newEngine
          .validateContractInstance(
            contractInstance,
            pkgId4, // The precondition of template T evaluates to false in pkg4
            cidMapping,
            hashingMethod,
            idValidator = _ => true,
          )
          .consume(pkgs = Map(pkgId4 -> pkg4))

        inside(result) { case Right(res) =>
          res shouldBe a[Left[_, _]]
        }
      }
    }

    "return a ResultDone(Left(_)) when the signatories change" in {

      val hashes = Table(
        "hashingMethod",
        Hash.HashingMethod.Legacy,
        Hash.HashingMethod.UpgradeFriendly,
        Hash.HashingMethod.TypedNormalForm,
      )

      forEvery(hashes) { hashingMethod =>
        val result = newEngine
          .validateContractInstance(
            contractInstance,
            pkgId5, // The signatories differ from those of contractInstance
            cidMapping,
            hashingMethod,
            idValidator = _ => true,
          )
          .consume(pkgs = Map(pkgId5 -> pkg5))

        inside(result) { case Right(res) =>
          res shouldBe a[Left[_, _]]
        }
      }
    }

    "return a ResultDone(Left(_)) when the observers change" in {

      val hashes = Table(
        "hashingMethod",
        Hash.HashingMethod.Legacy,
        Hash.HashingMethod.UpgradeFriendly,
        Hash.HashingMethod.TypedNormalForm,
      )

      forEvery(hashes) { hashingMethod =>
        val result = newEngine
          .validateContractInstance(
            contractInstance,
            pkgId6, // The observers differ from those of contractInstance
            cidMapping,
            hashingMethod,
            idValidator = _ => true,
          )
          .consume(pkgs = Map(pkgId6 -> pkg6))

        inside(result) { case Right(res) =>
          res shouldBe a[Left[_, _]]
        }
      }
    }

    "missing package is reported as a SResultError" in {

      val hashes = Table(
        "hashingMethod",
        Hash.HashingMethod.Legacy,
        Hash.HashingMethod.UpgradeFriendly,
        Hash.HashingMethod.TypedNormalForm,
      )

      forEvery(hashes) { hashingMethod =>
        val result = newEngine
          .validateContractInstance(
            contractInstance,
            pkgId1,
            cidMapping,
            hashingMethod,
            idValidator = _ => true,
          )
          .consume(pkgs = Map.empty) // We reply with "not found" to any NeedPackage question

        result shouldBe a[Left[_, _]] // consume reports ResultError as a Left
      }
    }
  }

  private def newSuffixedCid: V.ContractId = TransactionBuilder.newCid
    .suffixCid(
      _ => Bytes.assertFromString("00"),
      _ => Bytes.assertFromString("00"),
    )
    .value
}
