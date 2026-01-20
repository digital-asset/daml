// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.logging.LoggingContext
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  SerializationVersion,
  SubmittedTransaction,
  Transaction,
}
import com.digitalasset.daml.lf.value.ContractIdVersion
import com.digitalasset.daml.lf.value.Value._
import org.scalatest.EitherValues
import org.scalatest.Inside._
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.util.zip.ZipInputStream
import scala.language.implicitConversions

class InterfacesTestV2 extends InterfacesTest(LanguageVersion.Major.V2)

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class InterfacesTest(majorLanguageVersion: LanguageVersion.Major)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues {

  import InterfacesTest._

  private[this] val engine = Engine.DevEngine
  private[this] val compiledPackages = ConcurrentCompiledPackages(engine.config.getCompilerConfig)
  private[this] val preprocessor = preprocessing.Preprocessor.forTesting(compiledPackages)

  private def loadAndAddPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    val packages = DarDecoder.readArchive(resource, new ZipInputStream(stream)).toOption.get
    val (mainPkgId, mainPkg) = packages.main
    assert(
      compiledPackages.addPackage(mainPkgId, mainPkg).consume(pkgs = packages.all.toMap).isRight
    )
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  "interfaces" should {

    val (interfacesPkgId, interfacesPkg, allInterfacesPkgs) =
      loadAndAddPackage(s"Interfaces-v${majorLanguageVersion.pretty}.dar")

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
          version = SerializationVersion.StableVersions.max,
          packageName = interfacesPkg.pkgName,
          template = idT1,
          arg = ValueRecord(None, ImmArray((None, ValueParty(party)))),
          signatories = List(party),
        ),
      cid2 ->
        TransactionBuilder.fatContractInstanceWithDummyDefaults(
          version = SerializationVersion.StableVersions.min,
          packageName = interfacesPkg.pkgName,
          template = idT2,
          arg = ValueRecord(None, ImmArray((None, ValueParty(party)))),
          signatories = List(party),
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
            ledgerTime = let,
            preparationTime = let,
            seeding = seeding,
            contractIdVersion = contractIdVersion,
            packageResolution = packageNameMap,
          )
        (tx, meta, _) = result
      } yield (tx, meta)
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
  private val contractIdVersion = ContractIdVersion.V1

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)
}
