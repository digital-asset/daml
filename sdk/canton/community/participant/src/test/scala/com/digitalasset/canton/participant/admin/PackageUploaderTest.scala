// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.{CachingConfigs, PackageMetadataViewConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java.iou.Dummy
import com.digitalasset.canton.ledger.error.PackageServiceErrors
import com.digitalasset.canton.ledger.participant.state.PackageDescription
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.admin.PackageService.{DarDescription, DarMainPackageId}
import com.digitalasset.canton.participant.store.DamlPackageStore
import com.digitalasset.canton.participant.store.memory.{
  InMemoryDamlPackageStore,
  MutablePackageMetadataViewImpl,
}
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.platform.apiserver.services.admin.{
  PackageTestUtils,
  PackageUpgradeValidator,
}
import com.digitalasset.canton.time.SimClock
import com.digitalasset.canton.{
  BaseTest,
  HasActorSystem,
  HasExecutionContext,
  LedgerSubmissionId,
  LfPackageId,
  LfPackageName,
  LfPackageVersion,
}
import com.digitalasset.daml.lf.archive.{DamlLf, Dar, Decode}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import com.google.protobuf.ByteString
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

class PackageUploaderTest
    extends AnyWordSpec
    with BaseTest
    with HasActorSystem
    with HasExecutionContext {

  "validateDar" should {
    "succeed on valid DAR" in withTestEnv() { env =>
      import env.*
      val validationResult = packageUploader
        .validateDar(
          payload = cantonExampleBytesString,
          darName = "someDarName",
        )
        .futureValueUS

      validationResult.value._1 shouldBe cantonExamplesMainPkgId
      validationResult.value._2.size shouldBe 31

      // Assert not persisted
      packageStore.listPackages().futureValueUS shouldBe empty
      packageStore.listDars().futureValueUS shouldBe empty
    }

    "fail on invalid DAR" in withTestEnv() { env =>
      import env.*
      val invalidDar = ByteString.copyFromUtf8("invalid dar")
      val validationResult = packageUploader
        .validateDar(payload = invalidDar, darName = "someDarName")
        .futureValueUS
        .leftOrFail("expected error")

      validationResult shouldBe a[PackageServiceErrors.Reading.InvalidDar.Error]
    }
  }

  "upload" should {
    "store the correct artifacts in the store" in withTestEnv() { env =>
      import env.*
      def testPersistence(
          expectedPkgId: Option[LfPackageId],
          descriptionO: Option[String],
          expectedDescription: String,
      ): Assertion = {
        val (actualPkgId, depPkgIds) = packageUploader
          .upload(
            darPayload = cantonExampleBytesString,
            description = descriptionO,
            submissionId = LedgerSubmissionId.assertFromString("sub-test"),
            expectedMainPackageId = expectedPkgId,
          )
          .valueOrFail("upload failed")
          .futureValueUS

        depPkgIds should contain theSameElementsAs cantonExamplesDependencyPkgIds

        packageStore.getPackage(actualPkgId).futureValueUS.value shouldBe exampleDarExamples.main
        expectedPkgId.foreach(actualPkgId shouldBe _)

        val expectedPackagesList = decodedDar.map { case (archive, (pkgId, astPkg)) =>
          PackageDescription(
            packageId = pkgId,
            name = String255.tryCreate(astPkg.metadata.name),
            version = String255.tryCreate(astPkg.metadata.version.toString()),
            uploadedAt = clockNow,
            packageSize = archive.getPayload.size(),
          )
        }

        packageStore
          .listPackages()
          .futureValueUS should contain theSameElementsAs expectedPackagesList

        val actual = packageStore.listDars().futureValueUS
        actual should contain theSameElementsAs Seq(
          DarDescription(
            mainPackageId = DarMainPackageId.tryCreate(cantonExamplesMainPkgId),
            description = String255.tryCreate(expectedDescription),
            name = String255.tryCreate(Dummy.PACKAGE_NAME),
            version = String255.tryCreate(Dummy.PACKAGE_VERSION.toString),
          )
        )
      }

      testPersistence(
        expectedPkgId = None,
        descriptionO = None,
        expectedDescription = s"DAR_$cantonExamplesMainPkgId",
      )
      testPersistence(
        expectedPkgId = None,
        descriptionO = Some(PackageUploaderTest.super.getClass.getSimpleName),
        expectedDescription = PackageUploaderTest.super.getClass.getSimpleName,
      )
      testPersistence(
        expectedPkgId = Some(cantonExamplesMainPkgId),
        descriptionO = None,
        expectedDescription = s"DAR_$cantonExamplesMainPkgId",
      )
      testPersistence(
        expectedPkgId = Some(cantonExamplesMainPkgId),
        descriptionO = Some(PackageUploaderTest.super.getClass.getSimpleName),
        expectedDescription = PackageUploaderTest.super.getClass.getSimpleName,
      )
    }

    "report an error if the main package id does not match the expected" in withTestEnv() { env =>
      import env.*
      val otherPkgId = LfPackageId.assertFromString("otherPkgId")

      val uploadError = packageUploader
        .upload(
          darPayload = cantonExampleBytesString,
          description = Some(PackageUploaderTest.super.getClass.getSimpleName),
          submissionId = LedgerSubmissionId.assertFromString("sub-test"),
          expectedMainPackageId = Some(otherPkgId),
        )
        .leftOrFail("expected error")
        .futureValueUS

      val expectedError = PackageServiceErrors.Reading.MainPackageInDarDoesNotMatchExpected
        .Reject(cantonExamplesMainPkgId, otherPkgId)

      uploadError shouldBe expectedError

      packageStore.getPackage(cantonExamplesMainPkgId).futureValueUS shouldBe empty
      packageStore.getPackage(otherPkgId).futureValueUS shouldBe empty
    }

    "handle persistence failures" in {
      val recoveryPkgName = LfPackageName.assertFromString("recovery")
      val recoveryPkgVersion = LfPackageVersion.assertFromString("1.0")
      val recoveryArchive = PackageTestUtils.sampleLfArchive(
        packageName = recoveryPkgName,
        packageVersion = recoveryPkgVersion,
      )
      val recoveryPkgId = LfPackageId.assertFromString(recoveryArchive.getHash)

      val failingPackageStore = {
        val pkgStore = mock[InMemoryDamlPackageStore]
        when(
          pkgStore.append(anyList, any[CantonTimestamp], any[PackageService.Dar])(anyTraceContext)
        ).thenReturn(FutureUnlessShutdown.failed(new RuntimeException("append failed")))

        when(pkgStore.listPackages()(traceContext)).thenReturn(
          FutureUnlessShutdown.pure(
            Seq(
              PackageDescription(
                packageId = recoveryPkgId,
                name = String255.tryCreate(recoveryPkgName),
                version = String255.tryCreate(recoveryPkgVersion.toString()),
                uploadedAt = CantonTimestamp.Epoch,
                recoveryArchive.getPayload.size(),
              )
            )
          )
        )

        when(
          pkgStore.getPackage(recoveryPkgId)(traceContext)
        ).thenReturn(FutureUnlessShutdown.pure(Some(recoveryArchive)))
        pkgStore
      }

      withTestEnv(damlPackageStore = failingPackageStore) { env =>
        import env.*

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          within = packageUploader
            .upload(
              darPayload = cantonExampleBytesString,
              description = None,
              submissionId = LedgerSubmissionId.assertFromString("sub-1"),
              expectedMainPackageId = Some(cantonExamplesMainPkgId),
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

        // Check that the package map does not include the failed package
        // but only the recovery package (after re-initialization on failure)
        mutablePackageMetadataViewImpl.getSnapshot.packageIdVersionMap shouldBe Map(
          recoveryPkgId -> (recoveryPkgName -> recoveryPkgVersion)
        )

        succeed
      }
    }
  }

  final def withTestEnv(
      initialize: Boolean = true,
      damlPackageStore: DamlPackageStore = new InMemoryDamlPackageStore(loggerFactory),
      enableStrictDarValidation: Boolean = false,
  )(test: WithInitializedTestEnv => Assertion): Assertion = {
    val testEnv = new WithInitializedTestEnv(
      initialize,
      damlPackageStore,
      enableStrictDarValidation,
    ) {
      override def run(): Assertion = {
        super.run()
        test(this)
      }
    }

    try testEnv.run()
    finally testEnv.close()
  }

  private abstract class WithInitializedTestEnv(
      initialize: Boolean,
      val packageStore: DamlPackageStore,
      enableStrictDarValidation: Boolean,
  ) extends AutoCloseable {
    val clockNow: CantonTimestamp = CantonTimestamp.ofEpochMilli(1337L)
    private val clock = new SimClock(start = clockNow, loggerFactory = loggerFactory)
    val mutablePackageMetadataViewImpl = new MutablePackageMetadataViewImpl(
      clock = clock,
      packageStore = packageStore,
      new PackageUpgradeValidator(CachingConfigs.defaultPackageUpgradeCache, loggerFactory),
      loggerFactory = loggerFactory,
      packageMetadataViewConfig = PackageMetadataViewConfig(),
      timeouts = ProcessingTimeout(),
      futureSupervisor = futureSupervisor,
      exitOnFatalFailures = false,
    )
    val packageUploader = new PackageUploader(
      clock = clock,
      packageStore = packageStore,
      engine = DAMLe.newEngine(
        enableLfDev = false,
        enableLfBeta = false,
        enableStackTraces = false,
        paranoidMode = true,
      ),
      enableStrictDarValidation = enableStrictDarValidation,
      packageMetadataView = mutablePackageMetadataViewImpl,
      timeouts = ProcessingTimeout(),
      loggerFactory = loggerFactory,
    )

    val cantonExampleBytesString: ByteString =
      ByteString.copyFrom(PackageServiceTest.readCantonExamplesBytes())
    val exampleDarExamples: Dar[DamlLf.Archive] = PackageServiceTest.loadExampleDar()

    val decodedDar: List[(DamlLf.Archive, (PackageId, Ast.Package))] = exampleDarExamples.all.map {
      archive =>
        archive -> Decode.assertDecodeArchive(archive)
    }

    val cantonExamplesMainPkgId: PackageId = decodedDar.headOption.value._2._1
    val cantonExamplesDependencyPkgIds: List[PackageId] = decodedDar.tail.map(_._2._1)

    def run(): Assertion = if (initialize) {
      mutablePackageMetadataViewImpl.refreshState.futureValueUS
      succeed
    } else succeed

    override def close(): Unit = {
      packageUploader.close()
      mutablePackageMetadataViewImpl.close()
    }
  }

}
