// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.committer

import java.util.UUID

import com.codahale.metrics.MetricRegistry
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.store.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.wire.DamlSubmission
import com.daml.lf.archive.Decode
import com.daml.lf.archive.testing.Encode
import com.daml.lf.data.Ref
import com.daml.lf.engine.{Engine, EngineConfig}
import com.daml.lf.language.{Ast, LanguageVersion}
import com.daml.lf.testing.parser.Implicits._
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.ParallelTestExecution
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

class PackageCommitterSpec extends AnyWordSpec with Matchers with ParallelTestExecution {
  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  private[this] def encodePackage[P](pkg: Ast.Package) =
    Encode.encodeArchive(
      defaultParserParameters.defaultPackageId -> pkg,
      defaultParserParameters.languageVersion,
    )

  // 3 different well-typed and self-consistent archives
  // They need to be different in order to have different pkgId.
  val List(
    (pkg1, pkgId1, archive1),
    (pkg2, pkgId2, archive2),
    (pkg3, pkgId3, archive3),
  ) = for {
    i <- List(1, 2, 3)
    archive = encodePackage(
      p"""
        metadata ( 'Package$i' : '0.0.1' )

        module Mod$i {
          record Record$i = {};
        }
      """
    )
    (pkgId, pkg) = Decode.assertDecodeArchive(archive)
  } yield (pkg, pkgId, archive)

  // [libraryArchive] contains another well-typed and self-consistent package
  private[this] val libraryArchive = encodePackage(
    p"""
        metadata ( 'Color' : '0.0.1' )

        module Color {
          enum Primary = Red | Green | Blue;
        }
      """
  )
  val (libraryPackageId, libraryPackage) = Decode.assertDecodeArchive(libraryArchive)

  // [dependentArchive] contains a well-typed package that depends on [libraryArchive]
  private[this] val dependentArchive = encodePackage(
    p"""
        metadata ( 'Quantum' : '0.0.1' )

        module Chromodynamics {
          record Charge = { value: '$libraryPackageId':Color:Primary } ;
        }
      """
  )
  val (dependentPackageId, _) = Decode.assertDecodeArchive(dependentArchive)

  private[this] val participantId = Ref.ParticipantId.assertFromString("participant")

  private[this] class CommitterWrapper(
      validationMode: PackageValidationMode,
      preloadingMode: PackagePreloadingMode,
  ) {
    val metrics = new Metrics(new MetricRegistry)
    var engine: Engine = _
    var packageCommitter: PackageCommitter = _
    var state = Map.empty[DamlStateKey, DamlStateValue]
    restart()

    // simulate restart of the participant node
    def restart(): Unit = this.synchronized {
      engine = new Engine(
        EngineConfig(
          allowedLanguageVersions = LanguageVersion.DevVersions,
          packageValidation = false,
        )
      )
      packageCommitter = new PackageCommitter(engine, metrics, validationMode, preloadingMode)
    }

    def submit(submission: DamlSubmission): (DamlLogEntry, Map[DamlStateKey, DamlStateValue]) = {
      val result @ (log2, output1) =
        packageCommitter.run(
          Some(com.daml.lf.data.Time.Timestamp.now()),
          submission,
          participantId,
          Compat.wrapMap(state),
        )
      if (log2.hasPackageUploadRejectionEntry)
        assert(output1.isEmpty)
      else
        state ++= output1
      result
    }
  }

  private[this] def buildSubmission(archives: DamlLf.Archive*) =
    DamlSubmission
      .newBuilder()
      .setPackageUploadEntry(
        DamlPackageUploadEntry
          .newBuilder()
          .setSubmissionId(UUID.randomUUID().toString)
          .setParticipantId(participantId)
          .addAllArchives(archives.asJava)
      )
      .build()

  private[this] val emptyState = Compat.wrapMap(Map.empty[DamlStateKey, DamlStateValue])

  private[this] def details(rejection: DamlPackageUploadRejectionEntry) = {
    import DamlPackageUploadRejectionEntry.ReasonCase._
    rejection.getReasonCase match {
      case PARTICIPANT_NOT_AUTHORIZED =>
        rejection.getParticipantNotAuthorized.getDetails
      case DUPLICATE_SUBMISSION =>
        rejection.getDuplicateSubmission.getDetails
      case INVALID_PACKAGE =>
        rejection.getInvalidPackage.getDetails
      case REASON_NOT_SET =>
        ""
    }
  }

  private[this] def shouldFailWith(
      output: (DamlLogEntry, _),
      reason: DamlPackageUploadRejectionEntry.ReasonCase,
      msg: String = "",
  ) = {
    output._1.hasPackageUploadRejectionEntry shouldBe true
    output._1.getPackageUploadRejectionEntry.getReasonCase shouldBe reason
    details(output._1.getPackageUploadRejectionEntry) should include(msg)
  }

  private[this] def shouldSucceed(output: (DamlLogEntry, Map[DamlStateKey, DamlStateValue])) = {
    output._1.hasPackageUploadRejectionEntry shouldBe false
    output._2 shouldBe Symbol("nonEmpty")
  }

  private[this] def shouldSucceedWith(
      output: (DamlLogEntry, Map[DamlStateKey, DamlStateValue]),
      committedPackages: Set[Ref.PackageId],
  ) = {
    shouldSucceed(output)
    val archives = output._1.getPackageUploadEntry.getArchivesList
    archives.size() shouldBe committedPackages.size
    archives.iterator().asScala.map(_.getHash).toSet shouldBe committedPackages.toSet[String]
  }

  "PackageCommitter" should {
    def newCommitter = new CommitterWrapper(PackageValidationMode.No, PackagePreloadingMode.No)

    // Don't need to run the below test cases for all instances of PackageCommitter.
    "set record time in log entry if record time is available" in {
      val submission1 = buildSubmission(archive1)
      val output = newCommitter.packageCommitter.run(
        Some(theRecordTime),
        submission1,
        participantId,
        emptyState,
      )
      shouldSucceed(output)
      output._1.hasRecordTime shouldBe true
      output._1.getRecordTime shouldBe buildTimestamp(theRecordTime)
    }

    "skip setting record time in log entry when it is not available" in {
      val submission1 = buildSubmission(archive1)
      val output = newCommitter.packageCommitter.run(None, submission1, participantId, emptyState)
      shouldSucceed(output)
      output._1.hasRecordTime shouldBe false
    }

    "filter out already known packages" in {
      val committer = newCommitter

      shouldSucceedWith(committer.submit(buildSubmission(archive1)), Set(pkgId1))
      shouldSucceedWith(committer.submit(buildSubmission(archive1)), Set.empty)
      shouldSucceedWith(committer.submit(buildSubmission(archive1, archive2)), Set(pkgId2))
      shouldSucceedWith(committer.submit(buildSubmission(archive1, archive2)), Set.empty)
    }
  }

  private[this] def lenientValidationTests(newCommitter: => CommitterWrapper): Unit = {

    import DamlPackageUploadRejectionEntry.ReasonCase._

    "accept proper submissions" in {
      shouldSucceedWith(newCommitter.submit(buildSubmission(archive1)), Set(pkgId1))
      shouldSucceedWith(
        newCommitter.submit(buildSubmission(archive1, archive2)),
        Set(pkgId1, pkgId2),
      )
    }

    "accept submissions when dependencies are known by the engine" in {
      val committer = newCommitter
      committer.engine.preloadPackage(libraryPackageId, libraryPackage)
      shouldSucceed(committer.submit(buildSubmission(dependentArchive, libraryArchive)))
    }

    "reject not authorized submissions" in {
      val committer = newCommitter

      val submission1 = buildSubmission(archive1)
      val output = committer.packageCommitter.run(
        None,
        submission1,
        Ref.ParticipantId.assertFromString("authorizedParticipant"),
        emptyState,
      )
      shouldFailWith(output, PARTICIPANT_NOT_AUTHORIZED)
    }

    "reject duplicate submissions" in {
      val committer = newCommitter

      val submission = buildSubmission(archive1)
      shouldSucceed(committer.submit(submission))

      shouldFailWith(committer.submit(submission), DUPLICATE_SUBMISSION)
    }

    "reject empty submissions" in
      shouldFailWith(
        newCommitter.submit(buildSubmission()),
        INVALID_PACKAGE,
        "No archives in submission",
      )

    "reject submissions containing repeated packages" in {
      val committer = newCommitter
      val submission1 = buildSubmission(archive1, archive2, archive1, archive3)

      // when archive1 is unknown
      shouldFailWith(
        committer.submit(submission1),
        INVALID_PACKAGE,
        s"$pkgId1 appears more than once",
      )

      // when archive1 is known
      shouldSucceed(committer.submit(buildSubmission(archive1, archive2, archive3)))
      shouldFailWith(
        committer.submit(submission1),
        INVALID_PACKAGE,
        s"$pkgId1 appears more than once",
      )
    }

    "reject submissions containing different packages with same hash" in {
      val committer = newCommitter
      val archiveWithDuplicateHash = archive2.toBuilder.setHash(pkgId1).build()
      val submission1 = buildSubmission(archive1, archiveWithDuplicateHash)

      // when archive1 and archive2 are unknown
      shouldFailWith(
        committer.submit(submission1),
        INVALID_PACKAGE,
        s"$pkgId1 appears more than once",
      )

      // when archive1 and archive2 are known
      shouldSucceed(committer.submit(buildSubmission(archive1, archive2, archive3)))
      shouldFailWith(
        committer.submit(submission1),
        INVALID_PACKAGE,
        s"$pkgId1 appears more than once",
      )
    }

    "reject submissions containing a non valid package IDs" in {
      val committer = newCommitter
      val archiveWithInvalidPackageId =
        archive2.toBuilder.setHash("This is not a valid Package ID !").build()
      val submission = buildSubmission(archive1, archiveWithInvalidPackageId, archive3)

      //when archive2 is unknown
      shouldFailWith(
        newCommitter.submit(submission),
        INVALID_PACKAGE,
        "Invalid hash: non expected character",
      )

      // when archive2 is known
      shouldSucceed(committer.submit(buildSubmission(archive2)))
      committer.engine.preloadPackage(pkgId2, pkg2)
      shouldFailWith(newCommitter.submit(submission), INVALID_PACKAGE)
    }

    "reject submissions containing empty archives" in {
      val committer = newCommitter
      val archive = archive2.toBuilder.setPayload(ByteString.EMPTY).build()
      val submission = buildSubmission(archive1, archive, archive3)

      //when archive2 is unknown
      shouldFailWith(committer.submit(submission), INVALID_PACKAGE, pkgId2)

      // when archive2 is known
      shouldSucceed(committer.submit(buildSubmission(archive2)))
      committer.engine.preloadPackage(pkgId2, pkg2)
      shouldFailWith(committer.submit(submission), INVALID_PACKAGE)
    }

  }

  private[this] def strictValidationTests(newCommitter: => CommitterWrapper): Unit = {

    import DamlPackageUploadRejectionEntry.ReasonCase._

    "reject submissions containing packages with improper hashes" in {
      val committer = newCommitter
      val archiveWithImproperHash = archive3.toBuilder.setHash(pkgId2).build()
      val submission = buildSubmission(archive1, archiveWithImproperHash, archive3)

      //when archive2 is unknown
      shouldFailWith(
        committer.submit(submission),
        INVALID_PACKAGE,
        s"Mismatching hashes! Expected $pkgId3 but got $pkgId2",
      )

      // when archive2 is known
      shouldSucceed(committer.submit(buildSubmission(archive2)))
      committer.engine.preloadPackage(pkgId2, pkg2)
      shouldFailWith(
        committer.submit(submission),
        INVALID_PACKAGE,
        s"Mismatching hashes! Expected $pkgId3 but got $pkgId2",
      )
    }

    "reject submissions containing missing dependencies" in {

      val committer = newCommitter
      val submission = buildSubmission(dependentArchive)

      // when the dependencies are unknown
      shouldFailWith(
        committer.submit(submission),
        INVALID_PACKAGE,
        s"the missing dependencies are {'$libraryPackageId'}",
      )

      // when the dependencies are known
      shouldSucceed(committer.submit(buildSubmission(libraryArchive)))
      committer.engine.preloadPackage(libraryPackageId, libraryPackage)
      shouldFailWith(
        committer.submit(submission),
        INVALID_PACKAGE,
        s"the missing dependencies are {'$libraryPackageId'}",
      )
    }

    "reject submissions containing ill typed packages" in {

      val anIllTypeArchive =
        encodePackage(
          p"""
        metadata ( 'IllTyped' : '0.0.1' )

        module Wrong {
          val i: Numeric 10 = 1;  // 1 is an Int64 is not a Numeric
        }
      """
        )

      val committer = newCommitter
      val submission = buildSubmission(archive1, anIllTypeArchive, archive3)

      shouldFailWith(committer.submit(submission), INVALID_PACKAGE, "type mismatch")
    }

  }

  s"PackageCommitter with ${PackageValidationMode.Lenient} validation" should {
    def newCommitter = new CommitterWrapper(PackageValidationMode.Lenient, PackagePreloadingMode.No)
    lenientValidationTests(newCommitter)
  }

  s"PackageCommitter with ${PackageValidationMode.Strict} validation" should {
    def newCommitter = new CommitterWrapper(PackageValidationMode.Strict, PackagePreloadingMode.No)
    lenientValidationTests(newCommitter)
    strictValidationTests(newCommitter)
  }

  s"PackageCommitter with ${PackagePreloadingMode.No} preloading" should {
    "not preload the engine with packages" in {
      val committer = new CommitterWrapper(PackageValidationMode.No, PackagePreloadingMode.No)
      shouldSucceedWith(committer.submit(buildSubmission(archive1)), Set(pkgId1))
      Thread.sleep(1000)
      committer.engine.compiledPackages().packageIds shouldBe Symbol("empty")
    }
  }

  s"PackageCommitter with ${PackagePreloadingMode.Asynchronous} preloading" should {
    def newCommitter =
      new CommitterWrapper(PackageValidationMode.No, PackagePreloadingMode.Asynchronous)

    def waitWhile(cond: => Boolean): Unit =
      // wait up to 16s
      Iterator.iterate(16L)(_ * 2).takeWhile(_ <= 8192 && cond).foreach(Thread.sleep)

    "preload the engine with packages" in {
      val committer = newCommitter
      shouldSucceedWith(committer.submit(buildSubmission(archive1)), Set(pkgId1))
      waitWhile(committer.engine.compiledPackages().packageIds.isEmpty)
      committer.engine.compiledPackages().packageIds shouldBe Set(pkgId1)
    }

    "preload the engine with packages already in the ledger" in {
      val committer = newCommitter
      shouldSucceedWith(committer.submit(buildSubmission(libraryArchive)), Set(libraryPackageId))
      committer.restart()
      shouldSucceedWith(
        committer.submit(buildSubmission(libraryArchive, dependentArchive)),
        Set(dependentPackageId),
      )
      waitWhile(committer.engine.compiledPackages().packageIds.size < 2)
      committer.engine.compiledPackages().packageIds shouldBe Set(
        libraryPackageId,
        dependentPackageId,
      )
    }

    "preload the engine with packages when some are already in the engine" in {
      val committer = newCommitter
      shouldSucceedWith(committer.submit(buildSubmission(libraryArchive)), Set(libraryPackageId))
      waitWhile(committer.engine.compiledPackages().packageIds.size < 1)
      shouldSucceedWith(
        committer.submit(buildSubmission(libraryArchive, dependentArchive)),
        Set(dependentPackageId),
      )
      waitWhile(committer.engine.compiledPackages().packageIds.size < 2)
      committer.engine.compiledPackages().packageIds shouldBe Set(
        libraryPackageId,
        dependentPackageId,
      )
    }
  }

  s"PackageCommitter with ${PackagePreloadingMode.Synchronous} preloading" should {
    val newCommitter =
      new CommitterWrapper(PackageValidationMode.No, PackagePreloadingMode.Synchronous)

    "preload the engine with packages" in {
      val committer = newCommitter
      shouldSucceedWith(committer.submit(buildSubmission(archive1)), Set(pkgId1))
      committer.engine.compiledPackages().packageIds shouldBe Set(pkgId1)
    }

    "preload the engine with packages already in the ledger" in {
      val committer = newCommitter
      shouldSucceedWith(committer.submit(buildSubmission(libraryArchive)), Set(libraryPackageId))
      committer.restart()
      shouldSucceedWith(
        committer.submit(buildSubmission(libraryArchive, dependentArchive)),
        Set(dependentPackageId),
      )
      committer.engine.compiledPackages().packageIds shouldBe Set(
        libraryPackageId,
        dependentPackageId,
      )
    }

    "preload the engine with packages when some are already in the engine" in {
      val committer = newCommitter
      shouldSucceedWith(committer.submit(buildSubmission(libraryArchive)), Set(libraryPackageId))
      shouldSucceedWith(
        committer.submit(buildSubmission(libraryArchive, dependentArchive)),
        Set(dependentPackageId),
      )
      committer.engine.compiledPackages().packageIds shouldBe Set(
        libraryPackageId,
        dependentPackageId,
      )
    }
  }

  "buildLogEntry" should {

    val anEmptyResult = PackageCommitter.Result(
      DamlPackageUploadEntry.newBuilder.setSubmissionId("an ID").setParticipantId("a participant"),
      Map.empty,
    )

    def newCommitter = new CommitterWrapper(PackageValidationMode.No, PackagePreloadingMode.No)

    "produce an out-of-time-bounds rejection log entry in case pre-execution is enabled" in {
      val context = createCommitContext(recordTime = None)

      newCommitter.packageCommitter.buildLogEntry(context, anEmptyResult)

      context.preExecute shouldBe true
      context.outOfTimeBoundsLogEntry should not be empty
      context.outOfTimeBoundsLogEntry.foreach { actual =>
        actual.hasRecordTime shouldBe false
        actual.hasPackageUploadRejectionEntry shouldBe true
        actual.getPackageUploadRejectionEntry.getSubmissionId shouldBe anEmptyResult.uploadEntry.getSubmissionId
        actual.getPackageUploadRejectionEntry.getParticipantId shouldBe anEmptyResult.uploadEntry.getParticipantId
      }
    }

    "not set an out-of-time-bounds rejection log entry in case pre-execution is disabled" in {
      val context = createCommitContext(recordTime = Some(theRecordTime))

      newCommitter.packageCommitter.buildLogEntry(context, anEmptyResult)

      context.preExecute shouldBe false
      context.outOfTimeBoundsLogEntry shouldBe empty
    }
  }
}
