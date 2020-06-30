package com.daml.ledger.participant.state.kvutils.committer

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils.DamlPackageUploadEntry
import com.daml.ledger.participant.state.kvutils.TestHelpers._
import com.daml.ledger.participant.state.kvutils.Conversions.buildTimestamp
import com.daml.lf.engine.Engine
import com.daml.metrics.Metrics
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

class PackageCommitterSpec extends WordSpec with Matchers with MockitoSugar {
  private val metrics = new Metrics(new MetricRegistry)
  private val anEmptyResult = DamlPackageUploadEntry.newBuilder

  "buildLogEntry" should {
    "set record time in log entry if record time is available" in {
      val instance = new PackageCommitter(mock[Engine], metrics)
      val context = new FakeCommitContext(recordTime = Some(theRecordTime))

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe true
          actualLogEntry.getRecordTime shouldBe buildTimestamp(theRecordTime)
      }
    }

    "skip setting record time in log entry when it is not available" in {
      val instance = new PackageCommitter(mock[Engine], metrics)
      val context = new FakeCommitContext(recordTime = None)

      val actual = instance.buildLogEntry(context, anEmptyResult)

      actual match {
        case StepContinue(_) => fail
        case StepStop(actualLogEntry) =>
          actualLogEntry.hasRecordTime shouldBe false
      }
    }
  }

  "buildRejectionLogEntry" should {
    "set record time in log entry if record time is available" in {
      val instance = new PackageCommitter(mock[Engine], metrics)

      val actualLogEntry = instance.buildRejectionLogEntry(
        recordTime = Some(theRecordTime),
        submissionId = "id",
        participantId = "id",
        addErrorDetails = identity)

      actualLogEntry.hasRecordTime shouldBe true
      actualLogEntry.getRecordTime shouldBe buildTimestamp(theRecordTime)
      actualLogEntry.hasPackageUploadRejectionEntry shouldBe true
    }

    "skip setting record time in log entry when it is not available" in {
      val instance = new PackageCommitter(mock[Engine], metrics)

      val actualLogEntry = instance.buildRejectionLogEntry(
        recordTime = None,
        submissionId = "id",
        participantId = "id",
        addErrorDetails = identity)

      actualLogEntry.hasRecordTime shouldBe false
      actualLogEntry.hasPackageUploadRejectionEntry shouldBe true
    }
  }
}
