// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.io.File
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.{Node, NodeId, SubmittedTransaction, Transaction}
import com.daml.lf.value.Value._
import com.daml.lf.command.ReplayCommand
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import com.daml.logging.LoggingContext
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.EitherValues
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class ReinterpretTestV1 extends ReinterpretTest(LanguageMajorVersion.V1)
//class ReinterpretTestV2 extends ReinterpretTest(LanguageMajorVersion.V2)

class ReinterpretTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import ReinterpretTest._

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)

  private val party = Party.assertFromString("Party")

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    (packages.main._1, packages.main._2, packages.all.toMap)
  }

  private val (miniTestsPkgId, miniTestsPkg, allPackages) = loadPackage(
    s"daml-lf/tests/ReinterpretTests-v${majorLanguageVersion.pretty}.dar"
  )

  private val defaultContracts: Map[ContractId, VersionedContractInstance] =
    Map(
      toContractId("ReinterpretTests:MySimple:1") ->
        assertAsVersionedContract(
          ContractInstance(
            miniTestsPkg.nameVersion,
            TypeConName(miniTestsPkgId, "ReinterpretTests:MySimple"),
            ValueRecord(
              Some(Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")),
              ImmArray((Some[Name]("p"), ValueParty(party))),
            ),
          )
        )
    )

  private val engine = new Engine(
    EngineConfig(
      allowedLanguageVersions = language.LanguageVersion.AllVersions(majorLanguageVersion),
      requireSuffixedGlobalContractId = true,
    )
  )

  def Top(xs: Shape*) = Shape.Top(xs.toList)
  def Exercise(xs: Shape*) = Shape.Exercise(xs.toList)
  def Rollback(xs: Shape*) = Shape.Rollback(xs.toList)
  def Create() = Shape.Create()

  val submitters = Set(party)
  val time = Time.Timestamp.now()
  val seed = hash("ReinterpretTests")

  private def reinterpretCommand(theCommand: ReplayCommand): Either[Error, SubmittedTransaction] = {
    val res = engine
      .reinterpret(
        submitters,
        theCommand,
        Some(seed),
        time,
        time,
      )
      .consume(pcs = defaultContracts, pkgs = allPackages, grantUpgradeVerification = None)
    res match {
      case Right((tx, _)) => Right(tx)
      case Left(e) => Left(e)
    }
  }

  "Reinterpretation" should {

    "be correct for a successful exercise command" in {
      val choiceName = "MyHello"
      val theCommand = {
        val templateId = Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")
        val r = Identifier(miniTestsPkgId, s"ReinterpretTests:$choiceName")
        val cid = toContractId("ReinterpretTests:MySimple:1")
        ReplayCommand.Exercise(
          templateId,
          None,
          cid,
          choiceName,
          ValueRecord(Some(r), ImmArray.Empty),
        )
      }
      val Right(tx) = reinterpretCommand(theCommand)
      Shape.ofTransaction(tx.transaction) shouldBe Top(Exercise())
    }

    "be a rollback for an exercise command which throws" in {
      val choiceName = "MyThrow"
      val theCommand = {
        val templateId = Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")
        val r = Identifier(miniTestsPkgId, s"ReinterpretTests:$choiceName")
        val cid = toContractId("ReinterpretTests:MySimple:1")
        ReplayCommand.Exercise(
          templateId,
          None,
          cid,
          choiceName,
          ValueRecord(Some(r), ImmArray.Empty),
        )
      }
      val Right(tx) = reinterpretCommand(theCommand)
      Shape.ofTransaction(tx.transaction) shouldBe Top(Rollback(Exercise()))
    }

    "still fail for an uncatchable exception" in {
      val choiceName = "ProvokeBadOrd"
      val theCommand = {
        val templateId = Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")
        val r = Identifier(miniTestsPkgId, s"ReinterpretTests:$choiceName")
        val cid = toContractId("ReinterpretTests:MySimple:1")
        ReplayCommand.Exercise(
          templateId,
          None,
          cid,
          choiceName,
          ValueRecord(Some(r), ImmArray.Empty),
        )
      }
      val Left(err) = reinterpretCommand(theCommand)
      assert(err.message.contains("Error: functions are not comparable"))
    }

    "rollback version 14 contract creation" in {
      // LF v2 does not need to ensure compatibility with v1.14
      assume(majorLanguageVersion == LanguageMajorVersion.V1)

      val choiceName = "Contract14ThenThrow"
      val theCommand = {
        val templateId = Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")
        val r = Identifier(miniTestsPkgId, s"ReinterpretTests:$choiceName")
        val cid = toContractId("ReinterpretTests:MySimple:1")
        ReplayCommand.Exercise(
          templateId,
          None,
          cid,
          choiceName,
          ValueRecord(Some(r), ImmArray.Empty),
        )
      }
      val Right(tx) = reinterpretCommand(theCommand)
      Shape.ofTransaction(tx.transaction) shouldBe Top(Rollback(Exercise(Create())))
    }
  }
}

object ReinterpretTest {

  private implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)

  sealed trait Shape
  object Shape {

    final case class Top(xs: List[Shape])
    final case class Exercise(x: List[Shape]) extends Shape
    final case class Rollback(x: List[Shape]) extends Shape
    final case class Create() extends Shape

    def ofTransaction(tx: Transaction): Top = {
      def ofNid(nid: NodeId): Shape = {
        tx.nodes(nid) match {
          case node: Node.Exercise => Exercise(node.children.toList.map(ofNid))
          case node: Node.Rollback => Rollback(node.children.toList.map(ofNid))
          case _: Node.Create => Create()
          case _ => sys.error(s"Shape.ofTransaction, unexpected tx node")
        }
      }
      Top(tx.roots.toList.map(nid => ofNid(nid)))
    }
  }

}
