// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import better.files.*
import cats.data.EitherT
import com.daml.daml_lf_dev.DamlLf
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.lf.archive
import com.daml.lf.archive.DarParser
import com.digitalasset.canton.BaseTest
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
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.util.BinaryFileUtil
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AsyncWordSpec

import java.io.File
import java.nio.file.{Files, Paths}
import scala.concurrent.Future

object PackageServiceTest {

  @SuppressWarnings(Array("org.wartremover.warts.TryPartial"))
  def loadExampleDar(): archive.Dar[Archive] =
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

class PackageServiceTest extends AsyncWordSpec with BaseTest {
  private val examplePackages: List[Archive] = readCantonExamples()
  private val bytes = PackageServiceTest.readCantonExamplesBytes()
  private val darName = String255.tryCreate("CantonExamples")
  private val eventPublisher = mock[ParticipantEventPublisher]
  when(eventPublisher.publish(any[LedgerSyncEvent])(anyTraceContext))
    .thenAnswer(FutureUnlessShutdown.unit)
  private val participantId = DefaultTestIdentities.participant1

  private class Env {
    val packageStore = new InMemoryDamlPackageStore(loggerFactory)
    private val processingTimeouts = ProcessingTimeout()
    val packageDependencyResolver =
      new PackageDependencyResolver(packageStore, processingTimeouts, loggerFactory)
    private val engine =
      DAMLe.newEngine(enableLfDev = false, enableStackTraces = false)
    val sut =
      new PackageService(
        engine,
        packageDependencyResolver,
        eventPublisher,
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

  private lazy val cantonExamplesDescription = String256M.tryCreate("CantonExamples")

  "PackageService" should {
    "append DAR and packages from file" in withEnv { env =>
      import env.*

      val expectedPackageIdsAndState = examplePackages
        .map(DamlPackageStore.readPackageId)
        .map(PackageDescription(_, cantonExamplesDescription))
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
        packages should contain.only(expectedPackageIdsAndState *)
        dar shouldBe Some(Dar(DarDescriptor(hash, darName), bytes))
      }
    }

    "append DAR and packages from bytes" in withEnv { env =>
      import env.*

      val expectedPackageIdsAndState = examplePackages
        .map(DamlPackageStore.readPackageId)
        .map(PackageDescription(_, cantonExamplesDescription))

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
            vetAllPackages = false,
            synchronizeVetting = false,
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
}
