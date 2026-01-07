// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.daml.lf.crypto.Hash.HashingMethod
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.{Compiler, SValue}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{Node, SerializationVersion}
import com.digitalasset.daml.lf.value.{Value => V}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq

/** Tests for [[Engine.hashCreateNode]]. */
class HashCreateNodeSpec extends AnyWordSpec with EitherValues with Matchers {

  implicit val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.default[this.type]

  val pkgId = defaultParserParameters.defaultPackageId
  val pkg = {
    p""" metadata ( 'test-pkg' : '1.0.0' )
        module M {
          record @serializable T = { p: Party, cid : ContractId M:T, trailer : Option Int64 };
          template (this : T) = {
            precondition True;
            signatories Cons @Party [M:T {p} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  val compilerConfig = Compiler.Config.Default
  val compiledPkgs = PureCompiledPackages.build(Map(pkgId -> pkg), compilerConfig)

  private def newEngine = new Engine(
    EngineConfig(LanguageVersion.stableLfVersionsRange)
  )

  val alice = Party.assertFromString("Party")
  val packageName = Ref.PackageName.assertFromString("-test-pkg-")
  val templateId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:T"))
  val cid0 = newSuffixedCid
  val cid1 = newSuffixedCid
  def createArg(cidInContractArg: V.ContractId) =
    V.ValueRecord(
      None,
      ImmArray(None -> V.ValueParty(alice), None -> V.ValueContractId(cidInContractArg)),
    )
  def createArgWithTrailingNone(cidInContractArg: V.ContractId) =
    V.ValueRecord(
      None,
      ImmArray(
        None -> V.ValueParty(alice),
        None -> V.ValueContractId(cidInContractArg),
        None -> V.ValueOptional(None),
      ),
    )
  def createNode(arg: V) = Node.Create(
    coid = TransactionBuilder.newCid,
    packageName = Ref.PackageName.assertFromString("-test-pkg-"),
    templateId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:T")),
    arg = arg,
    signatories = Set(alice),
    stakeholders = Set(alice),
    keyOpt = None,
    version = SerializationVersion.minVersion,
  )
  val cidMapping = Map(cid0 -> cid1).withDefault(identity)

  "hashCreateNode" should {

    "compute the expected hash for HashingMethod.Legacy" in {
      val expectedHash = Hash
        .hashContractInstance(
          templateId,
          createArg(cid1),
          packageName,
          upgradeFriendly = false,
        )
        .value

      newEngine
        .hashCreateNode(createNode(createArg(cid0)), cidMapping, HashingMethod.Legacy)
        .consume() shouldBe Right(
        expectedHash
      )
    }

    "compute the expected hash for HashingMethod.UpgradeFriendly" in {
      val expectedHash = Hash
        .hashContractInstance(
          templateId,
          createArg(cid1),
          packageName,
          upgradeFriendly = true,
        )
        .value

      newEngine
        .hashCreateNode(createNode(createArg(cid0)), cidMapping, HashingMethod.UpgradeFriendly)
        .consume() shouldBe Right(
        expectedHash
      )
    }

    "compute the expected hash for HashingMethod.TypedNormalForm" in {
      val expectedHash = SValueHash
        .hashContractInstance(
          packageName,
          templateId.qualifiedName,
          SValue.SRecord(
            templateId,
            ImmArray(Ref.Name.assertFromString("p"), Ref.Name.assertFromString("cid")),
            ArraySeq(SValue.SParty(alice), SValue.SContractId(cid1)),
          ),
        )
        .value

      newEngine
        .hashCreateNode(createNode(createArg(cid0)), cidMapping, HashingMethod.TypedNormalForm)
        .consume(pkgs = Map(pkgId -> pkg)) shouldBe Right(expectedHash)
    }

    "ill-typed contract is reported as a SResultError" in {
      newEngine
        .hashCreateNode(
          createNode(createArg(cid0)).copy(arg = V.ValueUnit),
          cidMapping,
          HashingMethod.TypedNormalForm,
        )
        .consume(pkgs = Map(pkgId -> pkg)) shouldBe a[Left[_, _]]
    }

    "contract with trailing nones is reported as a SResultError" in {
      newEngine
        .hashCreateNode(
          createNode(createArgWithTrailingNone(cid0)),
          cidMapping,
          HashingMethod.TypedNormalForm,
        )
        .consume(pkgs = Map(pkgId -> pkg)) shouldBe a[Left[_, _]]
    }

    "missing package is reported as a SResultError" in {
      newEngine
        .hashCreateNode(
          createNode(createArg(cid0)),
          cidMapping,
          HashingMethod.TypedNormalForm,
        )
        .consume(pkgs = Map.empty) shouldBe a[Left[_, _]]
    }
  }

  private def newSuffixedCid: V.ContractId = TransactionBuilder.newCid
    .suffixCid(
      _ => Bytes.assertFromString("00"),
      _ => Bytes.assertFromString("00"),
    )
    .value
}
