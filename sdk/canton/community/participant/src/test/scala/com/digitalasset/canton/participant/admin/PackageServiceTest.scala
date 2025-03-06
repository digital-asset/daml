// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import better.files.*
import cats.data.EitherT
import com.daml.error.DamlError
import com.digitalasset.canton.BaseTest.getResourcePath
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.{PackageMetadataViewConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.participant.admin.PackageService.DarMainPackageId
import com.digitalasset.canton.participant.admin.PackageServiceTest.{
  AdminWorkflowsPath,
  readAdminWorkflows,
  readAdminWorkflowsBytes,
  readCantonExamples,
  readCantonExamplesBytes,
}
import com.digitalasset.canton.participant.admin.data.UploadDarData
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.{BinaryFileUtil, MonadUtil}
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
  private def loadDar(path: String): archive.Dar[Archive] =
    DarParser
      .readArchiveFromFile(new File(path))
      .getOrElse(throw new IllegalArgumentException("Failed to read dar"))

  def loadExampleDar(): archive.Dar[Archive] =
    loadDar(BaseTest.CantonExamplesPath)

  def readCantonExamples(): List[DamlLf.Archive] =
    loadExampleDar().all

  def readCantonExamplesBytes(): Array[Byte] =
    Files.readAllBytes(Paths.get(BaseTest.CantonExamplesPath))

  private val AdminWorkflowsPath = getResourcePath("AdminWorkflows.dar")
  def loadAdminWorkflowsDar(): archive.Dar[Archive] =
    loadDar(AdminWorkflowsPath)

  def readAdminWorkflows(): List[DamlLf.Archive] =
    loadAdminWorkflowsDar().all

  def readAdminWorkflowsBytes(): Array[Byte] =
    Files.readAllBytes(Paths.get(AdminWorkflowsPath))

  def badDarPath: String =
    ("community" / "participant" / "src" / "test" / "resources" / "daml" / "illformed.dar").toString

  def lf1xDarPath: String =
    ("community" / "participant" / "src" / "test" / "resources" / "daml" / "dummy-1.15.dar").toString

}

class PackageServiceTest
    extends AsyncWordSpec
    with BaseTest
    with HasActorSystem
    with HasExecutionContext {
  private val examplePackages: List[Archive] = readCantonExamples()
  private val adminWorkflowPackages: List[Archive] = readAdminWorkflows()
  private val bytes = PackageServiceTest.readCantonExamplesBytes()
  private val description = String255.tryCreate("CantonExamples")
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

  private def withEnvUS[T](test: Env => FutureUnlessShutdown[T]): Future[T] = {
    val env = new Env(uploadTime)
    test(env).failOnShutdown
  }

  private lazy val expectedPackageIds: Set[LfPackageId] =
    examplePackages.map(DamlPackageStore.readPackageId).toSet

  "PackageService" should {

    "append DAR and packages from file" in withEnvUS { env =>
      import env.*

      val payload = BinaryFileUtil
        .readByteStringFromFile(CantonExamplesPath)
        .valueOrFail("could not load examples")
      for {
        hash <- sut
          .upload(
            darBytes = payload,
            description = Some("CantonExamples"),
            submissionIdO = None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
            expectedMainPackageId = None,
          )
          .value
          .map(_.valueOrFail("append dar"))
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash).value
      } yield {
        packages.map(_.packageId).toSet should contain theSameElementsAs expectedPackageIds
        val darV = dar.valueOrFail("dar should be present")
        darV.bytes shouldBe bytes
        darV.descriptor.description shouldBe description
        darV.descriptor.name shouldBe "CantonExamples"
      }
    }

    "append DAR and packages from bytes" in withEnvUS { env =>
      import env.*

      for {
        hash <- sut
          .upload(
            darBytes = ByteString.copyFrom(bytes),
            description = Some("some/path/CantonExamples.dar"),
            submissionIdO = None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
            expectedMainPackageId = None,
          )
          .value
          .map(_.valueOrFail("should be right"))
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash).value
      } yield {
        packages.map(_.packageId).toSet should contain theSameElementsAs expectedPackageIds
        val darV = dar.valueOrFail("dar should be present")
        darV.bytes shouldBe bytes
        darV.descriptor.mainPackageId shouldBe hash
        darV.descriptor.name.str shouldBe "CantonExamples"
      }
    }

    "upload multiple DARs" in withEnvUS { env =>
      import env.*

      val examples = UploadDarData(
        bytes = BinaryFileUtil
          .readByteStringFromFile(CantonExamplesPath)
          .valueOrFail("could not load examples"),
        description = Some("CantonExamples"),
        expectedMainPackageId = None,
      )
      val test = UploadDarData(
        bytes = BinaryFileUtil
          .readByteStringFromFile(AdminWorkflowsPath)
          .valueOrFail("could not load admin workflows"),
        description = Some("AdminWorkflows"),
        expectedMainPackageId = None,
      )

      for {
        hashes <- sut
          .upload(
            Seq(examples, test),
            submissionIdO = None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
          )
          .value
          .map(_.valueOrFail("upload multiple dars"))
        packages <- packageStore.listPackages()
        dars <- MonadUtil.sequentialTraverse(hashes)(packageStore.getDar(_).value)
      } yield {
        val testAndExamplePackages =
          (examplePackages ++ adminWorkflowPackages).map(DamlPackageStore.readPackageId).toSet
        packages.map(_.packageId).toSet should contain theSameElementsAs testAndExamplePackages

        forAll(
          Seq(
            String255.tryCreate("CantonExamples") -> readCantonExamplesBytes(),
            String255.tryCreate("AdminWorkflows") -> readAdminWorkflowsBytes(),
          )
        ) { case (name, bytes) =>
          val dar = dars.flatten.find(_.descriptor.name == name).value
          dar.bytes shouldBe bytes
          dar.descriptor.description shouldBe name
          dar.descriptor.name shouldBe name
        }
      }
    }

    "expected main package id validation detects correct and wrong main package ids" in withEnvUS {
      env =>
        import env.*

        def attempt(expected: Option[String]) =
          sut
            .upload(
              darBytes = ByteString.copyFrom(bytes),
              description = Some("some/path/CantonExamples.dar"),
              submissionIdO = None,
              vetAllPackages = false,
              synchronizeVetting = PackageVettingSynchronization.NoSync,
              expectedMainPackageId = expected.map(LfPackageId.assertFromString),
            )
            .value

        for {
          // fail on invalid
          _ <- attempt(Some("123"))
            .map {
              case Left(value) =>
                value.code shouldBe PackageServiceErrors.Reading.MainPackageInDarDoesNotMatchExpected
              case Right(value) => fail("the expected main package id check should have failed")
            }
          mainPackageId <- attempt(None).map(_.valueOrFail("failed to upload dar"))
          _ <- attempt(Some(mainPackageId.unwrap)).map {
            case Left(value) => fail(s"should succeed but found $value")
            case Right(value) => succeed
          }
        } yield succeed
    }

    "validate DAR and packages from bytes" in withEnvUS { env =>
      import env.*

      for {
        hash <- sut
          .validateDar(
            ByteString.copyFrom(bytes),
            "some/path/CantonExamples.dar",
          )
          .value
          .map(_.valueOrFail("couldn't validate a dar file"))
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash).value
      } yield {
        expectedPackageIds.intersect(packages.map(_.packageId).toSet) shouldBe empty
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
            description = Some("some/path/CantonExamples.dar"),
            submissionIdO = None,
            vetAllPackages = false,
            synchronizeVetting = PackageVettingSynchronization.NoSync,
            expectedMainPackageId = None,
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
            expectedMainPackageId = None,
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
        mainPackageId: DarMainPackageId,
        op: String,
    ): Env => Future[Assertion] = { env =>
      req(env.sut).value.unwrap.map {
        case UnlessShutdown.Outcome(result) =>
          result shouldBe Left(
            CantonPackageServiceError.Fetching.DarNotFound
              .Reject(
                operation = op,
                mainPackageId = mainPackageId.unwrap,
              )
          )
        case UnlessShutdown.AbortedDueToShutdown => fail("Unexpected shutdown")
      }
    }

    val unknownDarId = DarMainPackageId.tryCreate("darid")

    "requested by PackageService.unvetDar" should {
      "reject the request with an error" in withEnv(
        rejectOnMissingDar(_.unvetDar(unknownDarId), unknownDarId, "DAR archive unvetting")
      )
    }

    "requested by PackageService.vetDar" should {
      "reject the request with an error" in withEnv(
        rejectOnMissingDar(
          _.vetDar(unknownDarId, PackageVettingSynchronization.NoSync),
          unknownDarId,
          "DAR archive vetting",
        )
      )
    }

    "requested by PackageService.removeDar" should {
      "reject the request with an error" in withEnv(
        rejectOnMissingDar(_.removeDar(unknownDarId), unknownDarId, "DAR archive removal")
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
                description = Some(darName),
                submissionIdO = None,
                vetAllPackages = false,
                synchronizeVetting = PackageVettingSynchronization.NoSync,
                expectedMainPackageId = None,
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

  private def encodeDarArchive(archive: Archive): ByteString =
    Using(ByteString.newOutput()) { os =>
      DarWriter.encode(
        BuildInfo.damlLibrariesVersion,
        LfDar(("archive.dalf", archive.toByteArray), List()),
        os,
      )
      os.toByteString
    }.success.value
}
