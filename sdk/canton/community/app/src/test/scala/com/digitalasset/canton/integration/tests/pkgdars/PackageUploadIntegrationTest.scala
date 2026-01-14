// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.pkgdars

import better.files.File
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.console.{CommandFailure, ParticipantReference, SequencerReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.PackageServiceErrors.Reading.InvalidDar
import com.digitalasset.canton.ledger.error.PackageServiceErrors.Validation.ValidationError
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors.Package.AllowedLanguageVersions
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.participant.admin.CantonPackageServiceError.PackageRemovalErrorCode
import com.digitalasset.canton.participant.admin.PackageService.{DarDescription, DarMainPackageId}
import com.digitalasset.canton.participant.admin.{AdminWorkflowServices, PackageServiceTest}
import com.digitalasset.canton.platform.apiserver.services.admin.PackageTestUtils
import com.digitalasset.canton.platform.apiserver.services.admin.PackageTestUtils.ArchiveOps
import com.digitalasset.canton.topology.TopologyManagerError
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.BinaryFileUtil
import com.digitalasset.daml.lf.archive.{DarParser, DarReader}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.google.protobuf.ByteString
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Seconds, Span}

import java.util.zip.ZipInputStream
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

trait PackageUploadIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with PackageUsableMixin {
  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P5_S1M1

  private def inStore(store: TopologyStoreId, participant: ParticipantReference) =
    participant.topology.vetted_packages
      .list(store = Some(store), filterParticipant = participant.id.filterString)
      .flatMap(_.item.packages)
      .map(_.packageId)
      .toSet

  private def ensureParticipantIsConnectedUploaded(
      ref: ParticipantReference,
      sequencerConnection: SequencerReference,
      synchronizerAlias: SynchronizerAlias,
  ): Unit =
    if (!ref.synchronizers.list_connected().map(_.synchronizerAlias).contains(synchronizerAlias)) {
      ref.synchronizers.connect_local(sequencerConnection, alias = synchronizerAlias)
      ref.dars.upload(CantonTestsPath, synchronizerId = Some(sequencerConnection.synchronizer_id))
    }

  private var cantonTestsMainPackageId, cantonExamplesMainPkgId: String = _

  "uploading before connecting" must {

    "enable the package" in { implicit env =>
      import env.*

      inStore(daId, participant1) shouldBe empty

      clue("uploading tests without vetting " + CantonTestsPath) {
        participant1.dars.upload(
          CantonTestsPath,
          vetAllPackages = false,
          synchronizeVetting = false,
        )
      }

      inStore(daId, participant1) shouldBe empty

      clue("connecting to synchronizer") {
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
      }

      clue("uploading tests " + CantonTestsPath) {
        participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
      }

      assertPackageUsable(participant1, participant1, daId)
    }

    "properly deal with empty zips" in { implicit env =>
      import env.*

      clue("uploading empty dar") {
        better.files.File.usingTemporaryFile("empty-dar", "dar") { file =>
          BinaryFileUtil.writeByteStringToFile(file.toString(), ByteString.EMPTY)
          this.assertThrowsAndLogsCommandFailures(
            participant1.dars.upload(file.toString()),
            _.shouldBeCommandFailure(InvalidDar),
          )
        }
      }

    }

    "not struggle with multiple uploads of the same dar" in { implicit env =>
      import env.*

      def inSynchronizerStore() = inStore(TopologyStoreId.Synchronizer(daId), participant1)

      def packages() = participant1.packages.list()

      participant1.dars.upload(CantonTestsPath, synchronizerId = daId)

      val beforeReuploadTx = inSynchronizerStore()
      val beforeNumPx = packages()

      clue("uploading tests multiple times") {
        (1 to 5).foreach { _ =>
          participant1.dars.upload(CantonTestsPath, synchronizerId = daId)
        }
      }

      val afterVettingTx = inSynchronizerStore()
      val afterNumPx = packages()

      beforeNumPx.toSet shouldBe afterNumPx.toSet
      beforeReuploadTx shouldBe afterVettingTx

    }
  }

  "connecting before uploading" must {
    "enable the package" in { implicit env =>
      import env.*

      // ensure so we can also run sub-sets of the test
      ensureParticipantIsConnectedUploaded(participant1, sequencer1, daName)

      participant2.synchronizers.connect_local(sequencer1, alias = daName)
      participant2.dars.upload(CantonExamplesPath, synchronizerId = daId)
      participant2.dars.upload(CantonTestsPath, synchronizerId = daId)

      assertPackageUsable(participant1, participant2, daId)
      assertPackageUsable(participant2, participant1, daId)

    }
  }

  "connecting and then restarting" must {

    "not log any warnings when we stop" in { implicit env =>
      import env.*

      participant4.synchronizers.connect_local(sequencer1, alias = daName)
      participant4.dars.upload(CantonTestsPath, synchronizeVetting = false)
      participant4.stop()
    }

    "starting again" must {
      "enable the package on all synchronizers" in { implicit env =>
        import env.*

        participant4.start()
        participant4.synchronizers.reconnect_all()
        participant4.dars.upload(CantonTestsPath, synchronizerId = daId)
        participant4.packages.synchronize_vetting()

        assertPackageUsable(participant4, participant4, daId)

      }
    }
  }

  "bad dar packages" must {

    "be rejected" in { implicit env =>
      import env.*

      val badDarPath = PackageServiceTest.badDarPath

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.dars.upload(badDarPath, vetAllPackages = false, synchronizeVetting = false),
        _.shouldBeCommandFailure(ValidationError.code),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.packages.upload_dar(badDarPath),
        _.shouldBeCommandFailure(ValidationError.code),
      )

      val badDarPayload =
        BinaryFileUtil.readByteStringFromFile(badDarPath).valueOrFail("read bad dar file")
      val parsedBadDar = DarParser
        .readArchive("illformed.dar", new ZipInputStream(badDarPayload.newInput()))
        .valueOrFail("parse bad dar file")
      val badPackageHash = parsedBadDar.main.getHash
      val _ = badPackageHash

      participant1.packages.find_by_module("Mod") shouldBe Seq.empty
    }
  }

  "LF 1.x DAR uploads" should {
    "be rejected gracefully" in { implicit env =>
      import env.*

      val darPath = PackageServiceTest.lf1xDarPath

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.dars.upload(darPath),
        _.shouldBeCommandFailure(AllowedLanguageVersions.code),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.packages.upload_dar(darPath),
        _.shouldBeCommandFailure(AllowedLanguageVersions.code),
      )

      participant1.packages.find_by_module("Mod") shouldBe Seq.empty
    }
  }

  "dar inspection" should {
    "show the content of a DAR if it was uploaded via the Admin API" in { implicit env =>
      import env.*

      participant3.synchronizers.connect_local(sequencer1, alias = daName)
      participant3.dars.list(filterName = "CantonExamples") shouldBe empty

      participant3.dars.upload(CantonExamplesPath)

      val items = participant3.dars.list(filterName = "CantonExamples")
      items should have length (1)

      val dar = items.loneElement

      // list contents
      val content = participant3.dars.get_contents(dar.mainPackageId)
      content.description.name shouldBe dar.name

      // packages should exist ...
      val allPackages = participant3.packages.list().map(_.packageId).toSet
      forAll((content.packages)) { pkg =>
        allPackages contains pkg.packageId
      }
    }

    "show the content of a DAR if it was uploaded via the Ledger API" in { implicit env =>
      import env.*

      participant3.synchronizers.connect_local(sequencer1, alias = daName)
      participant3.dars.list(filterName = "CantonTests") shouldBe empty
      participant3.ledger_api.packages.upload_dar(CantonTestsPath)

      val items = participant3.dars.list(filterName = "CantonTests")
      items should have length (1)

      val dar = items.loneElement

      // list contents
      val content = participant3.dars.get_contents(dar.mainPackageId)
      content.description.name shouldBe dar.name

      // packages should exist ...
      val allPackages = participant3.packages.list().map(_.packageId).toSet
      forAll((content.packages)) { pkg =>
        allPackages contains pkg.packageId
      }
    }
  }

  "DAR validation" should {
    "successfully validate the DAR without uploading" in { implicit env =>
      import env.*

      participants.all.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))

      // Validate a DAR against all participants
      val darHashes = participants.all.dars.validate(CantonExamplesPath)

      // Validate a DAR against one of the participants (Admin API)
      val expectedDarHash = participant1.dars.validate(CantonExamplesPath)

      // Validate a DAR against one of the participants (Ledger API)
      participant1.ledger_api.packages.validate_dar(CantonExamplesPath)

      // Observe that validation had no effect on the participants
      participants.all.foreach(_.dars.list(filterName = "PerformanceTest") shouldBe empty)

      // Actually upload the DAR and check that the hash matches the expected one
      val uploadedDarHash = participant1.dars.upload(CantonExamplesPath)
      uploadedDarHash shouldBe expectedDarHash
      darHashes.view.foreach(_._2 shouldBe expectedDarHash)
    }

    "report validation errors on invalid DARs" in { implicit env =>
      import env.*

      val badDarPath = PackageServiceTest.badDarPath

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.dars.validate(badDarPath),
        _.shouldBeCommandFailure(ValidationError.code),
      )

      loggerFactory.assertThrowsAndLogs[CommandFailure](
        participant1.ledger_api.packages.validate_dar(badDarPath),
        _.shouldBeCommandFailure(ValidationError.code),
      )
    }
  }

  "Concurrently uploading DARs with vetting enabled" should {
    def testConcurrentUploadVetting(
        // We want different DARs in these test cases to ensure no collisions
        darsDiscriminatorList: Seq[Char],
        vettingSyncEnabled: Boolean,
    )(implicit env: FixtureParam): Unit = {
      import env.*
      def testArchive(discriminator: String) =
        discriminator -> PackageTestUtils.archiveFromLfDef { implicit parserParameters =>
          p"""
           metadata ( '$discriminator' : '1.0.0' )
           module SomeMod {
             record @serializable T = { actor: Party, $discriminator: Party };

             template (this: T) = {
               precondition True;
               signatories Cons @Party [SomeMod:T {actor} this] (Nil @Party);
               observers Nil @Party;
             };
          }"""
        }().lfArchiveToByteString

      // Using very simple discriminators to ensure different DAR names with different content
      val variousDars = darsDiscriminatorList.map(disc => testArchive(disc.toString))
      // Make sure we are testing enough concurrent DAR uploads
      variousDars.length should be >= 10

      def testParallelUploads() = Future
        .traverse(variousDars) { case (darName, dar) =>
          Future.delegate {
            val darPath = File.newTemporaryFile(darName, ".dar")
            val file = darPath.appendByteArray(dar.toByteArray)
            val mainPackageId = DarReader.assertReadArchiveFromFile(darPath.toJava).main.pkgId

            Future {
              participant1.dars
                // Be explicit about ensuring and waiting for vetting
                .upload(
                  file.pathAsString,
                  synchronizerId = daId,
                  synchronizeVetting = vettingSyncEnabled,
                )
            }
              .transform {
                case Success(hash) =>
                  val darMetadata = participant1.dars.get_contents(hash)
                  darMetadata.description.description shouldBe darPath.nameWithoutExtension
                  darMetadata.description.mainPackageId shouldBe mainPackageId
                  // If successful, the DAR's main package should also be vetted
                  inStore(daId, participant1).contains(mainPackageId) shouldBe vettingSyncEnabled
                  Success((darPath, Success(mainPackageId)))
                case Failure(t) =>
                  // When vetting is enabled, uploads can fail due to rejected vetting operations that were run concurrently
                  (inStore(
                    daId,
                    participant1,
                  ) should not contain mainPackageId).discard
                  // Wrap in Success to ensure all futures are waited for in the Future.traverse
                  Success((darPath, Failure(t)))
              }
          }
        }
        .map(
          _.map { case (darPath: File, result: Try[Ref.PackageId]) =>
            // Unlift the inner Try.
            // When vetting synchronization is enabled, explode on failure since
            // that shouldn't happen when synchronization is enabled.
            if (vettingSyncEnabled) Right(result.success.value)
            // If a failure occurs vetting synchronization is NOT enabled,
            // return the darPath on failure. The log assertion below will check
            // that a serial mismatch was emitted that references that DAR path.
            else
              inside(result) {
                case Failure(failed: CommandFailure) =>
                  Left(darPath.canonicalPath)
                case Success(value) => Right(value)
              }
          }
        )

      // If we reach this code, uploadedPackages should have resulted in a package uploaded or
      // a command failure due to serial mismatch due to concurrent vetting proposals
      val uploadedPackagesResults =
        loggerFactory.assertLogsUnorderedOptionalFromResult(
          testParallelUploads().futureValue(Timeout(Span(30, Seconds))),
          (results: Seq[Either[String, Ref.PackageId]]) =>
            results.collect { case Left(path) =>
              (
                LogEntryOptionality.Required,
                logEntry =>
                  logEntry.shouldBeOneOfCommandFailure(
                    Seq(TopologyManagerError.SerialMismatch),
                    path,
                  ),
              )
            },
        )

      uploadedPackagesResults should have size darsDiscriminatorList.size.toLong

      if (!vettingSyncEnabled) {
        // Wait for vetting transactions to finish if the command was not run with
        // vetting synchronization enabled
        participant1.packages.synchronize_vetting()
        participant1.topology.synchronisation.await_idle()
      }
    }

    "work correctly when vetting synchronization is enabled" in { implicit env =>
      testConcurrentUploadVetting(darsDiscriminatorList = 'a' to 'j', vettingSyncEnabled = true)
    }

    "work correctly when vetting synchronization is disabled" in { implicit env =>
      testConcurrentUploadVetting(darsDiscriminatorList = 'k' to 't', vettingSyncEnabled = false)
    }
  }

  "Batch uploading multiple packages" should {
    "work" in { implicit env =>
      import env.*
      def testArchive(discriminator: String) =
        discriminator -> PackageTestUtils.archiveFromLfDef { implicit parserParameters =>
          p"""
           metadata ( '$discriminator' : '1.0.0' )
           module SomeMod {
             record @serializable T = { actor: Party, $discriminator: Party };

             template (this: T) = {
               precondition True;
               signatories Cons @Party [SomeMod:T {actor} this] (Nil @Party);
               observers Nil @Party;
             };
          }"""
        }().lfArchiveToByteString

      val batchUploadPrefix = "batchUploadPackage"
      // Using very simple discriminators to ensure different DAR names with different content
      val variousDars = (0 to 10).map(i => testArchive(s"$batchUploadPrefix$i"))

      val (darFiles, mainPackages) = variousDars.map { case (darName, bytes) =>
        val darPath = File.newTemporaryFile(darName, ".dar")
        val file = darPath.appendByteArray(bytes.toByteArray)
        val mainPackageId = DarReader.assertReadArchiveFromFile(darPath.toJava).main.pkgId
        (file.pathAsString, mainPackageId)
      }.unzip

      def getVettingSerial = participant1.topology.vetted_packages
        .list(daId, filterParticipant = participant1.filterString)
        .loneElement
        .context
        .serial

      val vettedPackagesSerialBefore = getVettingSerial

      participant1.dars.upload_many(
        darFiles,
        synchronizerId = Some(daId),
        synchronizeVetting = true,
      )

      val darDescription = participant1.dars.list()
      darDescription
        .filter(_.name.startsWith(batchUploadPrefix))
        .map(_.name) should contain theSameElementsAs variousDars.map(_._1)

      // If successful, the DAR's main package should also be vetted
      mainPackages should contain
      inStore(
        daId,
        participant1,
      ) should contain allElementsOf mainPackages

      eventually() {
        getVettingSerial shouldBe vettedPackagesSerialBefore.increment
      }
    }
  }

  "package inspection" should {
    "show uploaded dar" in { implicit env =>
      import env.*
      val mainPackageId = participant1.dars.upload(CantonExamplesPath)
      val found = participant1.dars.list().find(_.mainPackageId == mainPackageId).value
      val content = participant1.dars.get_contents(mainPackageId)
      // check that the content that we get from getting the dar matches the one from listing dars
      (
        found.name,
        found.version,
        found.description,
      ) shouldBe (content.description.name, content.description.version, content.description.description)
      content.packages.map(_.packageId) should contain(mainPackageId)

      val prim = participant1.packages.list(filterName = "daml-prim-DA-Types").headOption.value
      // just test whether we correctly can find the references for a given package
      participant1.packages.get_references(prim.packageId).map(_.name).toSet shouldBe Set(
        AdminWorkflowServices.PingDarResourceName,
        "CantonTests",
        "CantonExamples",
      )

    }

  }

  "DAR removal" should {
    "fail if the main package-id is used" in { implicit env =>
      import env.*
      participant4.dars.upload(CantonExamplesPath)
      participant4.dars.upload(CantonTestsPath)

      cantonTestsMainPackageId = participant4.dars
        .list(filterName = "CantonTests")
        .loneElement
        .mainPackageId

      cantonExamplesMainPkgId = participant4.dars
        .list(filterName = "CantonExamples")
        .loneElement
        .mainPackageId

      assertThrowsAndLogsCommandFailures(
        participant4.dars.remove(cantonTestsMainPackageId),
        _.shouldBeCommandFailure(
          PackageRemovalErrorCode.code,
          s"The DAR ${DarDescription(
              DarMainPackageId.tryCreate(cantonTestsMainPackageId),
              String255.tryCreate("CantonTests-3.4.0"),
              String255.tryCreate("CantonTests"),
              String255.tryCreate("3.4.0"),
            )} cannot be removed because its main package $cantonTestsMainPackageId is in-use",
        ),
      )
    }

    "only remove a dependency package-id if it's not referenced in another DAR" in { implicit env =>
      import env.*

      // Archive the contract using CantonTests
      archiveContract(participant4)

      // Now, we can remove the CantonTests DAR
      participant4.dars.remove(cantonTestsMainPackageId)

      // Check that CantonTests main package-id was removed
      participant4.packages
        .list(filterName = "CantonTests")
        .map(_.packageId) should not contain cantonTestsMainPackageId

      // but not the CantonExamples main package-id (that is a dependency of CantonTests)
      participant4.packages.list(filterName = "CantonExamples").map(_.packageId) should contain(
        cantonExamplesMainPkgId
      )

      // Wait for the package vetting to be effectively removed from the store
      eventually() {
        inStore(daId, participant4) should not contain cantonTestsMainPackageId
      }
      // Now, remove the CantonExamples DAR
      participant4.dars.remove(cantonExamplesMainPkgId)
      // Check that CantonExamples main package-id was removed
      participant4.packages
        .list(filterName = "CantonExamples")
        .map(_.packageId) should not contain cantonExamplesMainPkgId

      eventually() {
        inStore(daId, participant4) should not contain cantonExamplesMainPkgId
      }
    }
  }

  "Uploading and vetting a dar" must {
    "make its package dependencies usable immediately" in { implicit env =>
      import env.*

      participant5.synchronizers.connect_local(sequencer1, alias = daName)
      participant5.dars.upload(CantonExamplesPath)

      participant5.topology.vetted_packages
        .propose_delta(
          participant5.id,
          store = daId,
          adds = Seq(
            VettedPackage(
              cantonExamplesMainPkgId,
              validFromInclusive = Some(CantonTimestamp.now().plusSeconds(60)),
              validUntilExclusive = None,
            )
          ),
        )

      eventually() {
        participant5.topology.vetted_packages
          .list(daId, filterParticipant = participant5.filterString)
          .loneElement
          .item
          .packages
          .find(vp => vp.packageId == cantonExamplesMainPkgId)
          .value
          .validFromInclusive should not be empty
      }

      // Upload CantonTests that depends on the CantonExamples main package-id
      participant5.dars.upload(CantonTestsPath)

      eventually() {
        participant5.topology.vetted_packages
          .list(daId, filterParticipant = participant5.filterString)
          .loneElement
          .item
          .packages
          .find(vp => vp.packageId == cantonExamplesMainPkgId)
          .value
          .validFromInclusive shouldBe empty

      }

      assertPackageUsable(
        participant5,
        participant1,
        daId,
        (submitter, observer) =>
          new Iou(
            submitter.toProtoPrimitive,
            observer.toProtoPrimitive,
            new Amount(BigDecimal(17).bigDecimal, "fake-coin"),
            List.empty.asJava,
          ).create().commands().asScala.toSeq,
      )

    }
  }

}

class PackageUploadIntegrationTestPostgres extends PackageUploadIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
