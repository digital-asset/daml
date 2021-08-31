// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import java.io.File

import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.transaction.{
  GlobalKeyWithMaintainers,
  NodeId,
  GenTransaction,
  SubmittedTransaction,
}
import com.daml.lf.transaction.Node.{NodeRollback, NodeExercises, NodeCreate}
import com.daml.lf.value.Value._
import com.daml.lf.command._
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedValue
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.EitherValues
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class ReinterpretTest
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import ReinterpretTest._

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)

  private val party = Party.assertFromString("Party")

  private def loadPackage(resource: String): (PackageId, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    (packages.main._1, packages.all.toMap)
  }

  private val (miniTestsPkgId, allPackages) = loadPackage(
    "daml-lf/tests/ReinterpretTests.dar"
  )

  private val defaultContracts: Map[ContractId, ContractInst[VersionedValue[ContractId]]] =
    Map(
      toContractId("ReinterpretTests:MySimple:1") ->
        ContractInst(
          TypeConName(miniTestsPkgId, "ReinterpretTests:MySimple"),
          assertAsVersionedValue(
            ValueRecord(
              Some(Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")),
              ImmArray((Some[Name]("p"), ValueParty(party))),
            )
          ),
          "",
        )
    )

  val lookupContract = defaultContracts.get(_)
  val lookupPackage = allPackages.get(_)
  val lookupKey = { _: GlobalKeyWithMaintainers => None }

  private val engine = new Engine(
    EngineConfig(
      allowedLanguageVersions = language.LanguageVersion.DevVersions,
      requireV1ContractId = true,
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

  private def reinterpretCommand(theCommand: Command): Either[Error, SubmittedTransaction] = {
    val res = engine
      .reinterpret(
        submitters,
        theCommand,
        Some(seed),
        time,
        time,
      )
      .consume(
        lookupContract,
        lookupPackage,
        lookupKey,
      )
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
        ExerciseCommand(templateId, cid, choiceName, ValueRecord(Some(r), ImmArray.empty))
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
        ExerciseCommand(templateId, cid, choiceName, ValueRecord(Some(r), ImmArray.empty))
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
        ExerciseCommand(templateId, cid, choiceName, ValueRecord(Some(r), ImmArray.empty))
      }
      val Left(err) = reinterpretCommand(theCommand)
      assert(err.message.contains("Error: functions are not comparable"))
    }

    "rollback version 14 contract creation" in {
      val choiceName = "Contract14ThenThrow"
      val theCommand = {
        val templateId = Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")
        val r = Identifier(miniTestsPkgId, s"ReinterpretTests:$choiceName")
        val cid = toContractId("ReinterpretTests:MySimple:1")
        ExerciseCommand(templateId, cid, choiceName, ValueRecord(Some(r), ImmArray.empty))
      }
      val Right(tx) = reinterpretCommand(theCommand)
      Shape.ofTransaction(tx.transaction) shouldBe Top(Rollback(Exercise(Create())))
    }

    "not rollback version 13 contract creation" in {
      val choiceName = "Contract13ThenThrow"
      val theCommand = {
        val templateId = Identifier(miniTestsPkgId, "ReinterpretTests:MySimple")
        val r = Identifier(miniTestsPkgId, s"ReinterpretTests:$choiceName")
        val cid = toContractId("ReinterpretTests:MySimple:1")
        ExerciseCommand(templateId, cid, choiceName, ValueRecord(Some(r), ImmArray.empty))
      }

      val Left(err) = reinterpretCommand(theCommand)
      assert(err.toString().contains("ReinterpretTests:MyError"))
    }

  }
}

object ReinterpretTest {

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

    def ofTransaction(tx: GenTransaction[NodeId, ContractId]): Top = {
      def ofNid(nid: NodeId): Shape = {
        tx.nodes(nid) match {
          case node: NodeExercises[_, _] => Exercise(node.children.toList.map(ofNid))
          case node: NodeRollback[_] => Rollback(node.children.toList.map(ofNid))
          case _: NodeCreate[_] => Create()
          case _ => sys.error(s"Shape.ofTransaction, unexpected tx node")
        }
      }
      Top(tx.roots.toList.map(nid => ofNid(nid)))
    }
  }

}
