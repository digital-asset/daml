// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.lf.data.Ref
import com.daml.lf.language.LanguageVersion
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.{String255, String256M}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.PackageService.DarDescriptor
import com.digitalasset.canton.participant.admin.PackageUploaderTest.*
import com.digitalasset.canton.participant.admin.PackagesTestUtils.{
  encodeDarArchive,
  lfArchiveTemplate,
}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.memory.InMemoryDamlPackageStore
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.PackageDescription
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{
  BaseTest,
  DiscardOps,
  HasExecutionContext,
  LedgerSubmissionId,
  LfTimestamp,
}
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

class PackageUploaderTest extends AnyWordSpec with BaseTest with HasExecutionContext {
  "validateAndStorePackages" should {
    s"persist LF ${LanguageVersion.v1_15} packages" in testPackagePersistence(
      testLanguageVersion = LanguageVersion.v1_15,
      shouldStorePkgNameAndVersion = false,
    )

    s"persist LF ${LanguageVersion.v1_16} packages" in testPackagePersistence(
      testLanguageVersion = LanguageVersion.v1_16,
      shouldStorePkgNameAndVersion = true,
    )

    s"persist LF ${LanguageVersion.v1_dev} packages" in testPackagePersistence(
      testLanguageVersion = LanguageVersion.v1_dev,
      shouldStorePkgNameAndVersion = true,
    )

    "handle persistence failures" in {
      val recoveryPkgId = Ref.PackageId.assertFromString(lf1_16_archive.getHash)
      val failingPackageStore = {
        val pkgStore = mock[InMemoryDamlPackageStore]
        when(
          pkgStore.append(anySeq, any[String256M], any[Option[PackageService.Dar]])(anyTraceContext)
        ).thenReturn(FutureUnlessShutdown.failed(new RuntimeException("append failed")))

        when(pkgStore.listPackages()(traceContext)).thenReturn(
          Future.successful(
            Seq(
              PackageDescription(
                recoveryPkgId,
                String256M("someDescription")(None),
                packageName = Some(pkgName),
                packageVersion = Some(pkgVersion2),
              )
            )
          )
        )
        when(
          pkgStore.getPackage(recoveryPkgId)(traceContext)
        ).thenReturn(Future.successful(Some(lf1_16_archive)))
        pkgStore
      }

      withTestEnv(damlPackageStore = failingPackageStore) { env =>
        import env.*

        val submissionId = LedgerSubmissionId.assertFromString("sub-1")
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          within = sut
            .validateAndStorePackages(
              encodeDarArchive(archive_max_stable_LF),
              None,
              submissionId,
            )
            .valueOrFailShutdown("validateAndStorePackages failed")
            .failed
            .futureValue
            .getMessage shouldBe "append failed",
          assertion = LogEntry.assertLogSeq(
            Seq(
              (
                _.warningMessage should include("Failed to upload one or more archives"),
                "expected warning on failed upload",
              )
            )
          ),
        )

        // Check rejected LedgerSyncEvent publication
        verify(participantEventPublisherMock)
          .publish(
            LedgerSyncEvent.PublicPackageUploadRejected(
              submissionId,
              LfTimestamp.Epoch, // LedgerSyncEvent times are overwritten later (see [[ParticipantEventPublisher]])
              "append failed",
            )
          )(traceContext)
          .discard

        // Check that the package map does not include the failed package
        // but only the recovery package (after re-initialization on failure)
        sut.upgradablePackageResolutionMapRef.get().value shouldBe Map(
          recoveryPkgId -> (pkgName -> pkgVersion2)
        )

        succeed
      }
    }

    "not persist DAR in store if filename not specified" in withTestEnv(lfDev = true) { env =>
      import env.*
      val persistedPackageId = sut
        .validateAndStorePackages(
          encodeDarArchive(archive_max_stable_LF),
          None,
          LedgerSubmissionId.assertFromString("sub-1"),
        )
        .valueOrFailShutdown("validateAndStorePackages failed")
        .futureValue
        .headOption
        .value

      damlPackageStore.listPackages().futureValue should contain only PackageDescription(
        persistedPackageId,
        // If fileName not provided, the "default" is populated for descriptions
        sourceDescription = String256M("default")(None),
        // Max stable version now is 1.15, which won't store the name + version, as upgrades isn't supported on 1.15
        packageName = None,
        packageVersion = None,
      )

      damlPackageStore.listDars().futureValue shouldBe empty
    }

    "allow persisting packages with the same package-name and package-version if one is non-upgradable" in withTestEnv(
      lfDev = true
    ) { env =>
      import env.*

      // Define three packages that would be upgrade-incompatible: two non-upgradable (LF 1.15) and one upgradable (LF 1.16)
      // Same package-name and package-version
      val pkg_lf_1_15 = lfArchiveTemplate(
        pkgName,
        pkgVersion1,
        "someTest: Text",
        LanguageVersion.v1_15,
        Ref.PackageId.assertFromString("somePkgId"),
      )
      val another_pkg_lf_1_15 = lfArchiveTemplate(
        pkgName,
        pkgVersion1,
        "someBool: Bool",
        LanguageVersion.v1_15,
        Ref.PackageId.assertFromString("somePkgId"),
      )
      val pkg_lf_1_16 = lfArchiveTemplate(
        pkgName,
        pkgVersion1,
        "someParty: Party",
        LanguageVersion.v1_16,
        Ref.PackageId.assertFromString("somePkgId"),
      )

      val pkgId1 = sut
        .validateAndStorePackages(
          encodeDarArchive(pkg_lf_1_15),
          Some("fileName1"),
          LedgerSubmissionId.assertFromString("sub-1"),
        )
        .valueOrFailShutdown("validateAndStorePackages failed")
        .futureValue
        .headOption
        .value
      val pkgId2 = sut
        .validateAndStorePackages(
          encodeDarArchive(another_pkg_lf_1_15),
          Some("fileName2"),
          LedgerSubmissionId.assertFromString("sub-1"),
        )
        .valueOrFailShutdown("validateAndStorePackages failed")
        .futureValue
        .headOption
        .value
      val pkgId3 = sut
        .validateAndStorePackages(
          encodeDarArchive(pkg_lf_1_16),
          Some("fileName3"),
          LedgerSubmissionId.assertFromString("sub-1"),
        )
        .valueOrFailShutdown("validateAndStorePackages failed")
        .futureValue
        .headOption
        .value

      // Check that the packages are properly persisted
      damlPackageStore.listPackages().futureValue should contain theSameElementsAs Seq(
        PackageDescription(
          pkgId1,
          String256M("fileName1")(None),
          packageName = None,
          packageVersion = None,
        ),
        PackageDescription(
          pkgId2,
          String256M("fileName2")(None),
          packageName = None,
          packageVersion = None,
        ),
        PackageDescription(
          pkgId3,
          String256M("fileName3")(None),
          packageName = Some(pkgName),
          packageVersion = Some(pkgVersion1),
        ),
      )

      // Check that only the upgradable package updated the package resolution map
      sut.upgradablePackageResolutionMapRef.get().value shouldBe Map(
        pkgId3 -> (pkgName -> pkgVersion1)
      )

      succeed
    }
  }

  "initialize" should {
    "initialize the package resolution map from the daml package store" when {
      "package store non-empty" in withTestEnv(initialize = false) { env =>
        import env.*

        sut.upgradablePackageResolutionMapRef.get() shouldBe empty

        damlPackageStore
          .append(
            Seq(
              // Entries with empty package names and versions should not be loaded on initialization
              (lf1_15_archive, None, None),
              (lf1_16_archive, Some(pkgName), Some(pkgVersion2)),
              (archive_max_stable_LF, Some(pkgName), Some(pkgVersion3)),
            ),
            String256M("DarFileName")(),
            None,
          )
          .futureValueUS
        sut.initialize.futureValue

        val expectedPkgId1 = Ref.PackageId.assertFromString(lf1_16_archive.getHash)
        val expectedPkgId2 = Ref.PackageId.assertFromString(archive_max_stable_LF.getHash)

        val actualLoadedEntries = sut.upgradablePackageResolutionMapRef.get().value

        actualLoadedEntries shouldBe Map(
          expectedPkgId1 -> (pkgName -> pkgVersion2),
          expectedPkgId2 -> (pkgName -> pkgVersion3),
        )
      }

      "package store is empty" in withTestEnv(initialize = false) { env =>
        import env.*

        sut.upgradablePackageResolutionMapRef.get() shouldBe empty
        sut.initialize.futureValue
        sut.upgradablePackageResolutionMapRef.get().value shouldBe empty
      }
    }
  }

  final def withTestEnv(
      initialize: Boolean = true,
      lfDev: Boolean = false,
      lfPreview: Boolean = false,
      damlPackageStore: DamlPackageStore = new InMemoryDamlPackageStore(loggerFactory),
  )(test: WithInitializedPackageUploader => Assertion): Assertion = {
    val uploader = new WithInitializedPackageUploader(
      initialize,
      lfDev = lfDev,
      lfPreview = lfPreview,
      damlPackageStore,
    ) {
      override def run(): Assertion = {
        super.run()
        test(this)
      }
    }
    try uploader.run()
    finally uploader.close()
  }

  private abstract class WithInitializedPackageUploader(
      initialize: Boolean = true,
      lfDev: Boolean = false,
      lfPreview: Boolean = false,
      val damlPackageStore: DamlPackageStore,
  ) extends AutoCloseable {
    final val participantEventPublisherMock: ParticipantEventPublisher =
      mock[ParticipantEventPublisher]
    final val hashOps = new SymbolicPureCrypto
    when(participantEventPublisherMock.publish(any[LedgerSyncEvent])(anyTraceContext))
      .thenReturn(FutureUnlessShutdown.unit)

    final val sut = new PackageUploader(
      engine = DAMLe.newEngine(
        uniqueContractKeys = false,
        enableLfDev = lfDev,
        enableLfPreview = lfPreview,
        enableStackTraces = false,
      ),
      hashOps = hashOps,
      eventPublisher = participantEventPublisherMock,
      packageDependencyResolver =
        new PackageDependencyResolver(damlPackageStore, timeouts, loggerFactory),
      enableUpgradeValidation = true,
      clock = new SimClock(loggerFactory = loggerFactory),
      futureSupervisor = FutureSupervisor.Noop,
      timeouts = ProcessingTimeout(),
      loggerFactory = loggerFactory,
    )

    def run(): Assertion = if (initialize) {
      sut.initialize.futureValue
      succeed
    } else succeed

    override def close(): Unit = sut.close()
  }

  def testPackagePersistence(
      testLanguageVersion: LanguageVersion,
      shouldStorePkgNameAndVersion: Boolean,
  ): Assertion = {
    val previewMode = LanguageVersion.EarlyAccessVersions.contains(testLanguageVersion)
    val devMode =
      testLanguageVersion == LanguageVersion.v1_dev || testLanguageVersion == LanguageVersion.v2_dev

    withTestEnv(lfDev = devMode, lfPreview = previewMode) { env =>
      import env.*
      val testArchive = lfArchiveTemplate(
        pkgName,
        pkgVersion1,
        "discriminator: Text",
        testLanguageVersion,
        Ref.PackageId.assertFromString("somePkgId"),
      )
      val darPayload = encodeDarArchive(testArchive)

      val submissionId = LedgerSubmissionId.assertFromString("sub-1")
      val persistedPackageIds = sut
        .validateAndStorePackages(darPayload, Some("DarFileName"), submissionId)
        .valueOrFailShutdown("validateAndStorePackages failed")
        .futureValue

      val persistedPackageId = inside(persistedPackageIds) { case Seq(pkgId) => pkgId }

      // Check Daml Package/Archive persisted
      damlPackageStore
        .getPackage(persistedPackageId)
        .futureValue
        .value shouldBe testArchive

      // Check PackageDescription persisted correctly
      damlPackageStore.listPackages().futureValue should contain only PackageDescription(
        persistedPackageId,
        String256M("DarFileName")(None),
        packageName = Option.when(shouldStorePkgNameAndVersion)(pkgName),
        packageVersion = Option.when(shouldStorePkgNameAndVersion)(pkgVersion1),
      )

      val expectedDarHash = hashOps.digest(HashPurpose.DarIdentifier, darPayload)
      val expectedDarDescriptor = DarDescriptor(
        expectedDarHash,
        String255("DarFileName")(),
      )
      // Check DAR persisted
      damlPackageStore.listDars().futureValue should contain only expectedDarDescriptor

      val persistedDar = damlPackageStore.getDar(expectedDarHash).futureValue.value
      persistedDar.descriptor shouldBe expectedDarDescriptor
      ByteString.copyFrom(persistedDar.bytes) shouldBe darPayload

      // Check upgradablePackageResolutionMapRef correctly updated
      sut.upgradablePackageResolutionMapRef
        .get()
        .valueOrFail("upgradablePackageResolutionMapRef")
        .get(persistedPackageId) shouldBe Option.when(shouldStorePkgNameAndVersion)(
        pkgName -> pkgVersion1
      )

      // Check successful LedgerSyncEvent publication
      verify(participantEventPublisherMock)
        .publish(
          LedgerSyncEvent.PublicPackageUpload(
            List(testArchive),
            Some("DarFileName"),
            // LedgerSyncEvent times are overwritten later (see [[ParticipantEventPublisher]])
            LfTimestamp.Epoch,
            Some(submissionId),
          )
        )(traceContext)
        .discard

      succeed
    }
  }
}

object PackageUploaderTest {
  private val pkgName = Ref.PackageName.assertFromString("SomePkg")

  private val pkgVersion1 = Ref.PackageVersion.assertFromString("0.0.1")
  private val pkgVersion2 = Ref.PackageVersion.assertFromString("0.0.2")
  private val pkgVersion3 = Ref.PackageVersion.assertFromString("0.0.3")

  private val lf1_15_archive = lfArchiveTemplate(
    pkgName,
    pkgVersion1,
    "discriminator: Text",
    LanguageVersion.v1_15,
    Ref.PackageId.assertFromString("somePkgId"),
  )
  private val lf1_16_archive = lfArchiveTemplate(
    pkgName,
    pkgVersion2,
    "discriminator: Text",
    LanguageVersion.v1_16,
    Ref.PackageId.assertFromString("somePkgId"),
  )
  private val archive_max_stable_LF = lfArchiveTemplate(
    pkgName,
    pkgVersion3,
    "discriminator: Text",
    LanguageVersion.StableVersions.max,
    Ref.PackageId.assertFromString("anotherPkgId"),
  )
}
