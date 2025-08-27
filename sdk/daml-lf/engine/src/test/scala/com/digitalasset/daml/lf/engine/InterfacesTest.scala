// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.bazeltools.BazelRunfiles
import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.archive.UniversalArchiveDecoder
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.LanguageMajorVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{SubmittedTransaction, Transaction}
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.EitherValues
import org.scalatest.Inside._
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import scala.language.implicitConversions

class InterfacesTestV2 extends InterfacesTest(LanguageMajorVersion.V2)

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class InterfacesTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import InterfacesTest._

  private[this] val engine = Engine.DevEngine(majorLanguageVersion)
  private[this] val compiledPackages = ConcurrentCompiledPackages(engine.config.getCompilerConfig)
  private[this] val preprocessor = preprocessing.Preprocessor.forTesting(compiledPackages)

  private def loadAndAddPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    assert(
      compiledPackages.addPackage(mainPkgId, mainPkg).consume(pkgs = packages.all.toMap).isRight
    )
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  "interfaces" should {

    val (interfacesPkgId, interfacesPkg, allInterfacesPkgs) =
      loadAndAddPackage(s"daml-lf/tests/Interfaces-v${majorLanguageVersion.pretty}.dar")

    val packageNameMap = Map(interfacesPkg.pkgName -> interfacesPkgId)

    val idI1 = Identifier(interfacesPkgId, "Interfaces:I1")
    val idI2 = Identifier(interfacesPkgId, "Interfaces:I2")
    val idT1 = Identifier(interfacesPkgId, "Interfaces:T1")
    val idT2 = Identifier(interfacesPkgId, "Interfaces:T2")
    val let = Time.Timestamp.now()
    val submissionSeed = hash("rollback")
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)
    val cid1 = toContractId("1")
    val cid2 = toContractId("2")
    val contracts = Map(
      cid1 ->
        TransactionBuilder.fatContractInstanceWithDummyDefaults(
          version = LanguageMajorVersion.V2.minStableVersion,
          packageName = interfacesPkg.pkgName,
          template = idT1,
          arg = ValueRecord(None, ImmArray((None, ValueParty(party)))),
        ),
      cid2 ->
        TransactionBuilder.fatContractInstanceWithDummyDefaults(
          version = LanguageMajorVersion.V2.minStableVersion,
          interfacesPkg.pkgName,
          idT2,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
        ),
    )
    def consume[X](x: Result[X]) = x.consume(contracts, allInterfacesPkgs)

    def preprocessApi(cmd: ApiCommand) = consume(preprocessor.preprocessApiCommand(Map.empty, cmd))
    def run[Cmd](cmd: Cmd)(preprocess: Cmd => Result[speedy.Command]) =
      for {
        speedyCmd <- preprocess(cmd)
        result <- engine
          .interpretCommands(
            validating = false,
            submitters = Set(party),
            readAs = Set.empty,
            commands = ImmArray(speedyCmd),
            disclosures = ImmArray.empty,
            ledgerTime = let,
            preparationTime = let,
            seeding = seeding,
            packageResolution = packageNameMap,
          )
      } yield result
    def runApi(cmd: ApiCommand): Either[Error, (SubmittedTransaction, Transaction.Metadata)] =
      consume(run(cmd)(preprocessor.preprocessApiCommand(Map.empty, _)))

    /* generic exercise tests */
    "be able to exercise interface I1 on a T1 contract" in {
      val command = ApiCommand.Exercise(
        idI1.toRef,
        cid1,
        "C1",
        ValueRecord(None, ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise interface I1 on a T2 contract" in {
      val command = ApiCommand.Exercise(
        idI1.toRef,
        cid2,
        "C1",
        ValueRecord(None, ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise interface I2 on a T2 contract" in {
      val command = ApiCommand.Exercise(
        idI2.toRef,
        cid2,
        "C2",
        ValueRecord(None, ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }
    "be unable to exercise interface I2 on a T1 contract" in {
      val command = ApiCommand.Exercise(
        idI2.toRef,
        cid1,
        "C2",
        ValueRecord(None, ImmArray.empty),
      )
      inside(runApi(command)) { case Left(Error.Interpretation(err, _)) =>
        err shouldBe Error.Interpretation.DamlException(
          IE.ContractDoesNotImplementInterface(idI2, cid1, idT1)
        )
      }
    }
    "be able to exercise T1 by interface I1" in {
      val command = ApiCommand.Exercise(
        idI1.toRef,
        cid1,
        "C1",
        ValueRecord(None, ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise T2 by interface I1" in {
      val command = ApiCommand.Exercise(
        idI1.toRef,
        cid2,
        "C1",
        ValueRecord(None, ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }
    "be able to exercise T2 by interface I2" in {
      val command = ApiCommand.Exercise(
        idI2.toRef,
        cid2,
        "C2",
        ValueRecord(None, ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }
    "be unable to exercise T1 by interface I2 (stopped in preprocessor)" in {
      val command = ApiCommand.Exercise(
        idT1.toRef,
        cid1,
        "C2",
        ValueRecord(None, ImmArray.empty),
      )
      preprocessApi(command) shouldBe a[Left[_, _]]
    }
  }
}

object InterfacesTest {

  private implicit def logContext: LoggingContext = LoggingContext.ForTesting

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
  private def participant = Ref.ParticipantId.assertFromString("participant")

  private val party = Party.assertFromString("Party")

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)
}
