// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.UniversalArchiveDecoder
import com.daml.lf.command.ApiCommand
import com.daml.lf.data.Ref._
import com.daml.lf.data._
import com.daml.lf.language.Ast._
import com.daml.lf.language.LanguageMajorVersion
import com.daml.lf.transaction.test.TransactionBuilder.assertAsVersionedContract
import com.daml.lf.transaction.{SubmittedTransaction, Transaction}
import com.daml.lf.value.Value._
import com.daml.logging.LoggingContext
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.io.File
import scala.language.implicitConversions

class UpgradeTestV2 extends UpgradeTest(LanguageMajorVersion.V2)

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class UpgradeTest(majorLanguageVersion: LanguageMajorVersion)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with BazelRunfiles {

  import UpgradeTest._

  private[this] val engine = Engine.DevEngine(majorLanguageVersion)
  private[this] val compiledPackages = ConcurrentCompiledPackages(engine.config.getCompilerConfig)
  private[this] val preprocessor = new preprocessing.Preprocessor(compiledPackages)

  private def loadAndAddPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val packages = UniversalArchiveDecoder.assertReadFile(new File(rlocation(resource)))
    val (mainPkgId, mainPkg) = packages.main
    assert(
      compiledPackages.addPackage(mainPkgId, mainPkg).consume(pkgs = packages.all.toMap).isRight
    )
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  "upgrades" should {

    val (upgradesV1PkgId, upgradesV1Pkg, allUpgradesV1Pkgs) =
      loadAndAddPackage(s"daml-lf/tests/Upgrades-pkgv1-v${majorLanguageVersion.pretty}dev.dar")
    val (upgradesV2PkgId, upgradesV2Pkg, allUpgradesV2Pkgs) =
      loadAndAddPackage(s"daml-lf/tests/Upgrades-pkgv2-v${majorLanguageVersion.pretty}dev.dar")

    val packageNameMap = Map(upgradesV2Pkg.name -> upgradesV2PkgId)

    val v1TemplateId = Identifier(upgradesV1PkgId, "Upgrades:Asset")
    val v2TemplateId = Identifier(upgradesV2PkgId, "Upgrades:Asset")
    val v1Choice = Identifier(upgradesV1PkgId, "Upgrades:AssetChoice")
    val v2Choice = Identifier(upgradesV2PkgId, "Upgrades:AssetChoice")

    val cid1 = toContractId("Upgrades:Asset:1")
    val cid2 = toContractId("Upgrades:Asset:2")
    val contracts = Map(
      cid1 ->
        assertAsVersionedContract(
          ContractInstance(
            upgradesV1Pkg.name,
            TypeConName(upgradesV1PkgId, "Upgrades:Asset"),
            ValueRecord(
              Some(Identifier(upgradesV1PkgId, "Upgrades:Asset")),
              ImmArray(Some[Name]("issuer") -> ValueParty(party)),
            ),
          )
        ),
      cid2 ->
        assertAsVersionedContract(
          ContractInstance(
            upgradesV2Pkg.name,
            TypeConName(upgradesV2PkgId, "Upgrades:Asset"),
            ValueRecord(
              Some(Identifier(upgradesV2PkgId, "Upgrades:Asset")),
              ImmArray(
                Some[Name]("issuer") -> ValueParty(party),
                Some[Name]("v2Field") -> ValueOptional.apply(None),
              ),
            ),
          )
        ),
    )
    val allPkgs = allUpgradesV1Pkgs ++ allUpgradesV2Pkgs

    def consume[X](x: Result[X]) = x.consume(contracts, allPkgs, grantUpgradeVerification = None)

    val let = Time.Timestamp.now()
    val submissionSeed = hash("upgrades")
    val seeding = Engine.initialSeeding(submissionSeed, participant, let)

    def run(cmds: IterableOnce[ApiCommand]) =
      for {
        speedyCmds <- preprocessor.preprocessApiCommands(Map.empty, ImmArray.from(cmds))
        result <- engine
          .interpretCommands(
            validating = false,
            submitters = Set(party),
            readAs = Set.empty,
            commands = speedyCmds,
            disclosures = ImmArray.empty,
            ledgerTime = let,
            submissionTime = let,
            seeding = seeding,
            packageResolution = packageNameMap,
          )
      } yield result

    def runApi(cmds: ApiCommand*): Either[Error, (SubmittedTransaction, Transaction.Metadata)] =
      consume(run(cmds))

    "be able to exercise a v2 choice on a v1 contract" in {
      val command = ApiCommand.Exercise(
        v2TemplateId.toRef,
        cid1,
        "AssetChoice",
        ValueRecord(Some(v2Choice), ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }

    "be able to exercise a v1 choice on a downgradable v2 contract" in {
      val command = ApiCommand.Exercise(
        v1TemplateId.toRef,
        cid2,
        "AssetChoice",
        ValueRecord(Some(v1Choice), ImmArray.empty),
      )
      runApi(command) shouldBe a[Right[_, _]]
    }

    // TODO(https://github.com/digital-asset/daml/issues/19161): enable once fixed
    "be able to exercise a v1 choice then a v2 choice on a v1 contract in the same request" ignore {
      val command1 = ApiCommand.Exercise(
        v1TemplateId.toRef,
        cid1,
        "AssetChoice",
        ValueRecord(Some(v1Choice), ImmArray.empty),
      )
      val command2 = ApiCommand.Exercise(
        v2TemplateId.toRef,
        cid1,
        "AssetChoice",
        ValueRecord(Some(v2Choice), ImmArray.empty),
      )
      runApi(command1, command2) shouldBe a[Right[_, _]]
    }
  }
}

object UpgradeTest {

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
