// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import java.util.zip.ZipInputStream
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  Node,
  NodeId,
  SerializationVersion,
  SubmittedTransaction,
  Transaction,
}
import com.digitalasset.daml.lf.value.Value._
import com.digitalasset.daml.lf.command.ReplayCommand
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.ContractIdVersion
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.EitherValues
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

import scala.language.implicitConversions

class ReinterpretTestV2 extends ReinterpretTest(LanguageVersion.Major.V2)

class ReinterpretTest(majorLanguageVersion: LanguageVersion.Major)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues {

  import ReinterpretTest._

  private[this] val version = SerializationVersion.assign(LanguageVersion.latestStableLfVersion)

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)

  private val party = Party.assertFromString("Party")

  private def loadPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    val packages = DarDecoder.readArchive(resource, new ZipInputStream(stream)).toOption.get
    (packages.main._1, packages.main._2, packages.all.toMap)
  }

  private val (miniTestsPkgId, miniTestsPkg, allPackages) = loadPackage(
    s"ReinterpretTests-v${majorLanguageVersion.pretty}.dar"
  )

  private val defaultContracts: Map[ContractId, FatContractInstance] =
    Map(
      toContractId("ReinterpretTests:MySimple:1") ->
        TransactionBuilder.fatContractInstanceWithDummyDefaults(
          version = version,
          packageName = miniTestsPkg.pkgName,
          template = TypeConId(miniTestsPkgId, "ReinterpretTests:MySimple"),
          arg = ValueRecord(
            None,
            ImmArray((None, ValueParty(party))),
          ),
          signatories = List(party),
        )
    )

  private def freshEngine = new Engine(
    EngineConfig(
      allowedLanguageVersions = language.LanguageVersion.allLfVersionsRange,
      forbidLocalContractIds = true,
    )
  )

  private val engine = freshEngine

  def Top(xs: Shape*) = Shape.Top(xs.toList)
  def Exercise(xs: Shape*) = Shape.Exercise(xs.toList)
  def Rollback(xs: Shape*) = Shape.Rollback(xs.toList)
  def Create() = Shape.Create()

  val submitters = Set(party)
  val time = Time.Timestamp.now()
  val seed = hash("ReinterpretTests")
  val contractIdVersion = ContractIdVersion.V1

  private def reinterpretCommand(theCommand: ReplayCommand): Either[Error, SubmittedTransaction] = {
    val res = engine
      .reinterpret(
        submitters,
        theCommand,
        Some(seed),
        time,
        time,
        contractIdVersion,
      )
      .consume(pcs = defaultContracts, pkgs = allPackages)
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
        val cid = toContractId("ReinterpretTests:MySimple:1")
        ReplayCommand.Exercise(
          templateId,
          None,
          cid,
          choiceName,
          ValueRecord(None, ImmArray.Empty),
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
  }

  "exercise by interface pull interfaceId package " in {
    val templatePkgId = Ref.PackageId.assertFromString("-template-package-")
    val templateId = Ref.Identifier(templatePkgId, "A:T")
    val interfacePkgId = Ref.PackageId.assertFromString("-interface-package-")
    val interfaceId = Ref.Identifier(interfacePkgId, "B:I")

    val testCases = Table(
      "cmd" -> "packgeIds",
      ReplayCommand.Create(templateId, ValueUnit) -> List(templatePkgId),
      ReplayCommand.Exercise(templateId, None, toContractId("cid"), "myChoice", ValueUnit) -> List(
        templatePkgId
      ),
      ReplayCommand.Exercise(
        templateId,
        Some(interfaceId),
        toContractId("cid"),
        "myChoice",
        ValueUnit,
      ) -> List(templatePkgId, interfacePkgId),
      ReplayCommand.ExerciseByKey(templateId, ValueUnit, "MyChoice", ValueUnit) -> List(
        templatePkgId
      ),
      ReplayCommand.Fetch(templateId, None, toContractId("cid")) -> List(templatePkgId),
      ReplayCommand.Fetch(templateId, Some(interfaceId), toContractId("cid")) ->
        List(templatePkgId, interfacePkgId),
      ReplayCommand.FetchByKey(templateId, ValueUnit) -> List(templatePkgId),
      ReplayCommand.LookupByKey(templateId, ValueUnit) -> List(templatePkgId),
    )

    forEvery(testCases) { (cmd, pkgIds) =>
      val emptyPackage =
        Package(
          Map.empty,
          Set.empty,
          LanguageVersion.latestStableLfVersion,
          PackageMetadata(
            PackageName.assertFromString("foo"),
            PackageVersion.assertFromString("0.0.0"),
            None,
          ),
          GeneratedImports(
            reason = "package made in com.digitalasset.daml.lf.engine.ReinterpretTest",
            pkgIds = Set.empty,
          ),
        )
      var queriedPackageIds = Set.empty[Ref.PackageId]
      val trackPackageQueries: PartialFunction[Ref.PackageId, Package] = { pkgId =>
        queriedPackageIds = queriedPackageIds + pkgId
        emptyPackage
      }
      freshEngine
        .reinterpret(
          submitters,
          cmd,
          Some(seed),
          time,
          time,
          contractIdVersion,
        )
        .consume(pkgs = trackPackageQueries)
      pkgIds.toSet shouldBe queriedPackageIds
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
