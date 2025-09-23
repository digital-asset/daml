// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.crypto.Hash.HashingMethod
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.{LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.speedy.{Compiler, SValue}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{Node, TransactionVersion}
import com.digitalasset.daml.lf.value.{Value => V}
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq

class HashCreateNodeSpecV2 extends HashCreateNodeSpec(LanguageMajorVersion.V2)

class HashCreateNodeSpec(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with EitherValues
    with Matchers {

  val pkgId1 = Ref.PackageId.assertFromString("-packageId1-")
  val pkgId2 = Ref.PackageId.assertFromString("-packageId2-")

  val defaultParserParameters: ParserParameters[this.type] =
    ParserParameters.defaultFor[this.type](majorLanguageVersion)

  val pkg1 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId1
    )
    p""" metadata ( 'test-pkg' : '1.0.0' )
        module M {
          record @serializable T = { p: Party };
          template (this : T) = {
            precondition True;
            signatories Cons @Party [M:T {p} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  val pkg2 = {
    implicit def parserParameters: ParserParameters[this.type] = defaultParserParameters.copy(
      defaultPackageId = pkgId2
    )
    p""" metadata ( 'test-pkg' : '2.0.0' )
        module M {
          record @serializable T = { p: Party, extra: Option  };
          template (this : T) = {
            precondition True;
            signatories Cons @Party [M:T {p} this] (Nil @Party);
            observers Nil @Party;
          };
        }
    """
  }

  val compilerConfig = Compiler.Config.Default(majorLanguageVersion)
  val compiledPkgs = PureCompiledPackages.build(Map(pkgId1 -> pkg1, pkgId2 -> pkg2), compilerConfig)

  // private[this] implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private def newEngine = new Engine(
    EngineConfig(LanguageVersion.StableVersions(majorLanguageVersion))
  )

  val alice = Party.assertFromString("Party")
  val packageName = Ref.PackageName.assertFromString("-test-pkg-")
  val templateId = Ref.Identifier(pkgId1, Ref.QualifiedName.assertFromString("M:T"))
  val createArg = V.ValueRecord(None, ImmArray(None -> V.ValueParty(alice)))
  val createNode = Node.Create(
    coid = TransactionBuilder.newCid,
    packageName = Ref.PackageName.assertFromString("-test-pkg-"),
    templateId = Ref.Identifier(pkgId1, Ref.QualifiedName.assertFromString("M:T")),
    arg = createArg,
    signatories = Set(alice),
    stakeholders = Set(alice),
    keyOpt = None,
    version = TransactionVersion.minVersion,
  )

  "hash create node" should {
    "compute the expected hash for HashingMethod.Legacy" in {
      val expectedHash = Hash
        .hashContractInstance(templateId, createArg, packageName, upgradeFriendly = false)
        .value

      newEngine.hashCreateNode(createNode, HashingMethod.Legacy).consume() shouldBe Right(
        expectedHash
      )
    }

    "compute the expected hash for HashingMethod.UpgradeFriendly" in {
      val expectedHash = Hash
        .hashContractInstance(templateId, createArg, packageName, upgradeFriendly = true)
        .value

      newEngine.hashCreateNode(createNode, HashingMethod.UpgradeFriendly).consume() shouldBe Right(
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
            ImmArray(Ref.Name.assertFromString("p")),
            ArraySeq(SValue.SParty(alice)),
          ),
        )
        .value

      newEngine
        .hashCreateNode(createNode, HashingMethod.TypedNormalForm)
        .consume(pkgs = Map(pkgId1 -> pkg1)) shouldBe Right(expectedHash)
    }
  }
}
