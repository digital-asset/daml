// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import better.files.*
import cats.data.EitherT
import com.daml.error.DamlError
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.{PackageMetadataViewConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.admin.PackageServiceTest.readCantonExamples
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{BaseTest, HasActorSystem, HasExecutionContext, LfPackageId}
import com.digitalasset.daml.lf.archive
import com.digitalasset.daml.lf.archive.DamlLf.Archive
import com.digitalasset.daml.lf.archive.testing.Encode
import com.digitalasset.daml.lf.archive.{DamlLf, Dar as LfDar, DarParser, DarWriter}
import com.digitalasset.daml.lf.language.{Ast, LanguageMajorVersion, LanguageVersion}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.file.{Files, Paths}
import scala.concurrent.Future
import scala.util.Using

object PackageServiceTest {

  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def loadExampleDar(): archive.Dar[Archive] =
    DarParser
      .readArchiveFromFile(new File(BaseTest.CantonExamplesPath))
      .getOrElse(throw new IllegalArgumentException("Failed to read dar"))

  def readCantonExamples(): List[DamlLf.Archive] =
    loadExampleDar().all

  def readCantonExamplesBytes(): Array[Byte] =
    Files.readAllBytes(Paths.get(BaseTest.CantonExamplesPath))

  def badDarPath: String =
    ("community" / "participant" / "src" / "test" / "resources" / "daml" / "illformed.dar").toString
}

class PackageServiceTest
    extends AsyncWordSpec
    with BaseTest
    with HasActorSystem
    with HasExecutionContext {
  private val examplePackages: List[Archive] = readCantonExamples()
  private val bytes = PackageServiceTest.readCantonExamplesBytes()
  private val darName = String255.tryCreate("CantonExamples")
  private val participantId = DefaultTestIdentities.participant1

  private class Env(now: CantonTimestamp) {
    val packageStore = new InMemoryDamlPackageStore(loggerFactory)
    private val processingTimeouts = ProcessingTimeout()
    val packageDependencyResolver =
      new PackageDependencyResolver(packageStore, processingTimeouts, loggerFactory)
    private val engine =
      DAMLe.newEngine(enableLfDev = false, enableLfBeta = false, enableStackTraces = false)

    val sut: PackageService = PackageService
      .createAndInitialize(
        clock = new SimClock(start = now, loggerFactory = loggerFactory),
        engine = engine,
        packageDependencyResolver = packageDependencyResolver,
        enableUpgradeValidation = true,
        futureSupervisor = FutureSupervisor.Noop,
        hashOps = new SymbolicPureCrypto(),
        loggerFactory = loggerFactory,
        metrics = ParticipantTestMetrics,
        exitOnFatalFailures = true,
        packageMetadataViewConfig = PackageMetadataViewConfig(),
        packageOps = new PackageOpsForTesting(participantId, loggerFactory),
        timeouts = processingTimeouts,
      )
      .futureValueUS
  }

  private val uploadTime = CantonTimestamp.now()

  private def withEnv[T](test: Env => Future[T]): Future[T] = {
    val env = new Env(uploadTime)
    test(env)
  }

  private lazy val cantonExamplesDescription = String255.tryCreate("CantonExamples")
  private lazy val expectedPackageIdsAndState: Seq[PackageDescription] =
    examplePackages
      .map { pkg =>
        PackageDescription(
          DamlPackageStore.readPackageId(pkg),
          cantonExamplesDescription,
          uploadTime,
          pkg.getPayload.size(),
        )
      }

  "PackageService" should {
    "append DAR and packages from file" in withEnv { env =>
      import env.*

      val payload = BinaryFileUtil
        .readByteStringFromFile(CantonExamplesPath)
        .valueOrFail("could not load examples")
      for {
        hash <- sut
          .upload(
            darBytes = payload,
            fileNameO = Some("CantonExamples"),
            submissionIdO = None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
          )
          .value
          .map(_.valueOrFail("append dar"))
          .failOnShutdown
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        packages should contain theSameElementsAs expectedPackageIdsAndState
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), bytes))
      }
    }

    "append DAR and packages from bytes" in withEnv { env =>
      import env.*

      for {
        hash <- sut
          .upload(
            darBytes = ByteString.copyFrom(bytes),
            fileNameO = Some("some/path/CantonExamples.dar"),
            submissionIdO = None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
          )
          .value
          .map(_.valueOrFail("should be right"))
          .failOnShutdown
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        packages should contain theSameElementsAs expectedPackageIdsAndState
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), bytes))
      }
    }

    "validate DAR and packages from bytes" in withEnv { env =>
      import env.*

      for {
        hash <- sut
          .validateDar(
            ByteString.copyFrom(bytes),
            "some/path/CantonExamples.dar",
          )
          .value
          .map(_.valueOrFail("couldn't validate a dar file"))
          .failOnShutdown
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        expectedPackageIdsAndState.intersect(packages) shouldBe empty
        dar shouldBe None
      }
    }

    "fetching dependencies" in withEnv { env =>
      import env.*

      val dar = PackageServiceTest.loadExampleDar()
      val mainPackageId = DamlPackageStore.readPackageId(dar.main)
      val dependencyIds =
        com.digitalasset.daml.lf.archive.Decode.assertDecodeArchive(dar.main)._2.directDeps
      (for {
        _ <- sut
          .upload(
            darBytes = ByteString.copyFrom(bytes),
            fileNameO = Some("some/path/CantonExamples.dar"),
            submissionIdO = None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
          )
          .valueOrFail("appending dar")
        deps <- packageDependencyResolver.packageDependencies(mainPackageId).value
      } yield {
        // test for explict dependencies
        deps match {
          case Left(value) => fail(value)
          case Right(loaded) =>
            // all direct dependencies should be part of this
            (dependencyIds -- loaded) shouldBe empty
        }
      }).unwrap.map(_.failOnShutdown)
    }

    "validateDar validates the package" in withEnv { env =>
      import env.*

      val badDarPath = PackageServiceTest.badDarPath
      val payload = BinaryFileUtil
        .readByteStringFromFile(badDarPath)
        .valueOrFail(s"could not load bad dar file at $badDarPath")
      for {
        error <- leftOrFail(
          sut.validateDar(
            payload,
            badDarPath,
          )
        )("append illformed.dar").failOnShutdown
      } yield {
        error match {
          case validation: PackageServiceErrors.Validation.ValidationError.Error =>
            validation.validationError shouldBe a[com.digitalasset.daml.lf.validation.ETypeMismatch]
          case _ => fail(s"$error is not a validation error")
        }
      }
    }

    "appendDar validates the package" in withEnv { env =>
      import env.*

      val badDarPath = PackageServiceTest.badDarPath
      val payload = BinaryFileUtil
        .readByteStringFromFile(badDarPath)
        .valueOrFail(s"could not load bad dar file at $badDarPath")
      for {
        error <- leftOrFail(
          sut.upload(
            payload,
            Some(badDarPath),
            None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
          )
        )("append illformed.dar").failOnShutdown
      } yield {
        error match {
          case validation: PackageServiceErrors.Validation.ValidationError.Error =>
            validation.validationError shouldBe a[com.digitalasset.daml.lf.validation.ETypeMismatch]
          case _ => fail(s"$error is not a validation error")
        }
      }
    }
  }

  "The DAR referenced by the requested hash does not exist" when {
    def rejectOnMissingDar(
        req: PackageService => EitherT[FutureUnlessShutdown, CantonError, Unit],
        darHash: Hash,
        op: String,
    ): Env => Future[Assertion] = { env =>
      req(env.sut).value.unwrap.map {
        case UnlessShutdown.Outcome(result) =>
          result shouldBe Left(
            CantonPackageServiceError.DarNotFound
              .Reject(
                operation = op,
                darHash = darHash.toHexString,
              )
          )
        case UnlessShutdown.AbortedDueToShutdown => fail("Unexpected shutdown")
      }
    }

    val unknownDarHash = Hash
      .build(HashPurpose.TopologyTransactionSignature, HashAlgorithm.Sha256)
      .add("darhash")
      .finish()

    "requested by PackageService.unvetDar" should {
      "reject the request with an error" in withEnv(
        rejectOnMissingDar(_.unvetDar(unknownDarHash), unknownDarHash, "DAR archive unvetting")
      )
    }

    "requested by PackageService.vetDar" should {
      "reject the request with an error" in withEnv(
        rejectOnMissingDar(
          _.vetDar(unknownDarHash, PackageVettingSynchronization.NoSync),
          unknownDarHash,
          "DAR archive vetting",
        )
      )
    }

    "requested by PackageService.removeDar" should {
      "reject the request with an error" in withEnv(
        rejectOnMissingDar(_.removeDar(unknownDarHash), unknownDarHash, "DAR archive removal")
      )
    }

    "validate upgrade-incompatible DARs that are uploaded concurrently" in withEnv { env =>
      import env.*

      // Upload DARs concurrently
      val concurrentDarUploadsF =
        upgradeIncompatibleDars.map { case (darName, archive) =>
          val payload = encodeDarArchive(archive)
          EitherT
            .rightT[FutureUnlessShutdown, DamlError](())
            // Delegate the future within
            .flatMap(_ =>
              sut.upload(
                darBytes = payload,
                fileNameO = Some(darName),
                submissionIdO = None,
                vetAllPackages = false,
                synchronizeVetting = PackageVettingSynchronization.NoSync,
              )
            )
        }
      for {
        results <- Future.sequence(concurrentDarUploadsF.map(_.value.failOnShutdown))
      } yield {
        // Only one upload should have succeeded, i.e. the first stored DAR
        results.collect { case Right(_) => () }.size shouldBe 1
        // Expect the other results to be failures due to incompatible upgrades
        results.collect {
          case Left(_: PackageServiceErrors.Validation.Upgradeability.Error) => succeed
          case Left(other) => fail(s"Unexpected $other")
        }.size shouldBe (upgradeIncompatibleDars.size - 1)
      }
    }
  }

  private val upgradeIncompatibleDars =
    Seq(
      testArchive(1)("someParty: Party"),
      testArchive(2)("someText: Text"),
      testArchive(3)("someBool: Bool"),
      testArchive(4)("someDate: Date"),
      testArchive(5)("someParty: Party, anotherParty: Party"),
      testArchive(6)("someParty: Party, someText: Text"),
      testArchive(7)("someParty: Party, someBool: Bool"),
      testArchive(8)("someParty: Party, someDate: Date"),
      testArchive(9)("someText: Text, anotherText: Text"),
      testArchive(10)("someText: Text, someBool: Bool"),
    )

  private def testArchive(idx: Int)(discriminatorField: String) =
    s"incompatible$idx.dar" -> createLfArchive { implicit parserParameters =>
      p"""
        metadata ( 'incompatibleUpgrade' : '$idx.0.0' )
        module Mod {
          record @serializable T = { actor: Party, $discriminatorField };

          template (this: T) = {
            precondition True;
            signatories Cons @Party [Mod:T {actor} this] (Nil @Party);
            observers Nil @Party;
          };
       }"""
    }

  private def createLfArchive(defn: ParserParameters[?] => Ast.Package): Archive = {
    val lfVersion = LanguageVersion.defaultOrLatestStable(LanguageMajorVersion.V2)
    val selfPkgId = LfPackageId.assertFromString("-self-")
    implicit val parseParameters: ParserParameters[Nothing] = ParserParameters(
      defaultPackageId = selfPkgId,
      languageVersion = lfVersion,
    )

    val pkg = defn(parseParameters)

    Encode.encodeArchive(selfPkgId -> pkg, lfVersion)
  }

  private def encodeDarArchive(archive: Archive) =
    Using(ByteString.newOutput()) { os =>
      DarWriter.encode(
        BuildInfo.damlLibrariesVersion,
        LfDar(("archive.dalf", archive.toByteArray), List()),
        os,
      )
      os.toByteString
    }.get
}
