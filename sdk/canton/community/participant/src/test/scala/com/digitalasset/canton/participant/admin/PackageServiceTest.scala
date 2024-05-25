// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import better.files.*
import cats.Eval
import cats.data.EitherT
import com.daml.SdkVersion
import com.daml.daml_lf_dev.DamlLf
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.error.DamlError
import com.daml.lf.archive.testing.Encode
import com.daml.lf.archive.{Dar as LfDar, DarParser, DarWriter}
import com.daml.lf.data.Ref
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.daml.lf.testing.parser.ParserParameters
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String256M}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.participant.admin.PackageService.{Dar, DarDescriptor}
import com.digitalasset.canton.participant.admin.PackageServiceTest.readCantonExamples
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.{LfPackageName, PackageDescription}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfPackageVersion}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.file.{Files, Paths}
import scala.concurrent.Future
import scala.util.Using

object PackageServiceTest {

  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def loadExampleDar() =
    DarParser
      .readArchiveFromFile(new File(BaseTest.CantonExamplesPath))
      .getOrElse(throw new IllegalArgumentException("Failed to read dar"))

  def readCantonExamples(): List[DamlLf.Archive] = {
    loadExampleDar().all
  }

  def readCantonExamplesBytes(): Array[Byte] =
    Files.readAllBytes(Paths.get(BaseTest.CantonExamplesPath))

  def badDarPath: String =
    ("community" / "participant" / "src" / "test" / "resources" / "daml" / "illformed.dar").toString
}

class PackageServiceTest extends AsyncWordSpec with BaseTest with HasExecutionContext {
  private val examplePackages: List[Archive] = readCantonExamples()
  private val bytes = PackageServiceTest.readCantonExamplesBytes()
  val darName = String255.tryCreate("CantonExamples")
  private val eventPublisher = mock[ParticipantEventPublisher]
  when(eventPublisher.publish(any[LedgerSyncEvent])(anyTraceContext))
    .thenAnswer(FutureUnlessShutdown.unit)
  val participantId = DefaultTestIdentities.participant1

  private class Env {
    val packageStore = new InMemoryDamlPackageStore(loggerFactory)
    private val processingTimeouts = ProcessingTimeout()
    val packageDependencyResolver =
      new PackageDependencyResolver(packageStore, processingTimeouts, loggerFactory)
    val engine =
      DAMLe.newEngine(uniqueContractKeys = false, enableLfDev = true, enableStackTraces = false)

    private val packageUploader =
      Eval.now(
        PackageUploader
          .createAndInitialize(
            engine = engine,
            hashOps = new SymbolicPureCrypto(),
            enableUpgradeValidation = true,
            eventPublisher = eventPublisher,
            packageDependencyResolver = packageDependencyResolver,
            clock = new SimClock(loggerFactory = loggerFactory),
            futureSupervisor = FutureSupervisor.Noop,
            timeouts = processingTimeouts,
            loggerFactory = loggerFactory,
          )
          .futureValueUS
      )
    val sut =
      new PackageService(
        packageDependencyResolver,
        packageUploader,
        new SymbolicPureCrypto(),
        new PackageOpsForTesting(participantId, loggerFactory),
        ParticipantTestMetrics,
        processingTimeouts,
        loggerFactory,
      )
  }

  private def withEnv[T](test: Env => Future[T]): Future[T] = {
    val env = new Env()
    test(env)
  }

  lazy val cantonExamplesDescription = String256M.tryCreate("CantonExamples")

  "PackageService" should {
    "append DAR and packages from file" in withEnv { env =>
      import env.*

      val expectedPackageIdsAndState = examplePackages
        .map(DamlPackageStore.readPackageId)
        .map(PackageDescription(_, cantonExamplesDescription, None, None))
      val payload = BinaryFileUtil
        .readByteStringFromFile(CantonExamplesPath)
        .valueOrFail("could not load examples")
      for {
        hash <- sut
          .appendDarFromByteString(
            payload,
            "CantonExamples",
            vetAllPackages = false,
            synchronizeVetting = false,
          )
          .value
          .map(_.valueOrFail("append dar"))
          .failOnShutdown
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        packages should contain.only(expectedPackageIdsAndState: _*)
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), bytes))
      }
    }

    "append the package name and version for language version 1.16" in withEnv { env =>
      import env.*

      val fileName = "CantonExamples.dar"
      val pkgName = "somePkgName"
      val pkgVersion = "1.2.3"
      val archive = createLf1_16_Archive { implicit parserParameters =>
        p"""
        metadata ( '$pkgName' : '$pkgVersion' )
        module Mod {
          record @serializable T = { actor: Party };

          template (this: T) = {
            precondition True;
            signatories Cons @Party [Mod:T {actor} this] (Nil @Party);
            observers Nil @Party;
            agreement "Agreement";
          };
       }"""
      }
      val pkgId = DamlPackageStore.readPackageId(archive)
      val expectedPackageIdAndState = PackageDescription(
        packageId = pkgId,
        sourceDescription = cantonExamplesDescription,
        packageName = Some(LfPackageName.assertFromString(pkgName)),
        packageVersion = Some(LfPackageVersion.assertFromString(pkgVersion)),
      )
      val payload = encodeDarArchive(archive)
      for {
        hash <- sut
          .appendDarFromByteString(
            payload,
            fileName,
            vetAllPackages = false,
            synchronizeVetting = false,
          )
          .value
          .map(_.valueOrFail("append dar"))
          .failOnShutdown
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        packages should contain.only(expectedPackageIdAndState)
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), payload.toByteArray))
      }
    }

    "append DAR and packages from bytes" in withEnv { env =>
      import env.*

      val expectedPackageIdsAndState = examplePackages
        .map(DamlPackageStore.readPackageId)
        .map(PackageDescription(_, cantonExamplesDescription, None, None))

      for {
        hash <- sut
          .appendDarFromByteString(
            ByteString.copyFrom(bytes),
            "some/path/CantonExamples.dar",
            vetAllPackages = false,
            synchronizeVetting = false,
          )
          .value
          .map(_.valueOrFail("should be right"))
          .failOnShutdown
        packages <- packageStore.listPackages()
        dar <- packageStore.getDar(hash)
      } yield {
        packages should contain.only(expectedPackageIdsAndState: _*)
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), bytes))
      }
    }

    "fetching dependencies" in withEnv { env =>
      import env.*

      val dar = PackageServiceTest.loadExampleDar()
      val mainPackageId = DamlPackageStore.readPackageId(dar.main)
      val dependencyIds = com.daml.lf.archive.Decode.assertDecodeArchive(dar.main)._2.directDeps
      for {
        _ <- sut
          .appendDarFromByteString(
            ByteString.copyFrom(bytes),
            "some/path/CantonExamples.dar",
            false,
            false,
          )
          .valueOrFail("appending dar")
          .failOnShutdown
        deps <- packageDependencyResolver.packageDependencies(List(mainPackageId)).value
      } yield {
        // test for explict dependencies
        deps match {
          case Left(value) => fail(value)
          case Right(loaded) =>
            // all direct dependencies should be part of this
            (dependencyIds -- loaded) shouldBe empty
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
          sut.appendDarFromByteString(
            payload,
            badDarPath,
            vetAllPackages = false,
            synchronizeVetting = false,
          )
        )("append illformed.dar").failOnShutdown
      } yield {
        error match {
          case validation: PackageServiceErrors.Validation.ValidationError.Error =>
            validation.validationError shouldBe a[com.daml.lf.validation.ETypeMismatch]
          case _ => fail(s"$error is not a validation error")
        }
      }
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
              sut.appendDarFromByteString(
                payload = payload,
                filename = darName,
                vetAllPackages = false,
                synchronizeVetting = false,
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
          _.vetDar(unknownDarHash, synchronize = true),
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
    s"incompatible$idx.dar" -> createLf1_16_Archive { implicit parserParameters =>
      p"""
        metadata ( 'incompatibleUpgrade' : '$idx.0.0' )
        module Mod {
          record @serializable T = { actor: Party, $discriminatorField };

          template (this: T) = {
            precondition True;
            signatories Cons @Party [Mod:T {actor} this] (Nil @Party);
            observers Nil @Party;
            agreement "Agreement";
          };
       }"""
    }

  private def createLf1_16_Archive(defn: ParserParameters[?] => Ast.Package): Archive = {
    val lfVersion = LanguageVersion.v1_16
    val selfPkgId = Ref.PackageId.assertFromString("-self-")
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
        SdkVersion.sdkVersion,
        LfDar(("archive.dalf", archive.toByteArray), List()),
        os,
      )
      os.toByteString
    }.get
}
