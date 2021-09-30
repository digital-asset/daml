// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration

import com.codahale.metrics.MetricRegistry
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.participant.state.kvutils.DamlConfigurationRejection.DamlConfigurationRejectionEntry
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class KVUtilsConfigSpec extends AnyWordSpec with Matchers {

  import KVTest._
  import TestHelpers._

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting

  "configuration" should {

    "be able to build, pack, unpack and parse" in {
      val keyValueSubmission = new KeyValueSubmission(new Metrics(new MetricRegistry))
      val subm = keyValueSubmission.unpackDamlSubmission(
        keyValueSubmission.packDamlSubmission(
          keyValueSubmission.configurationToSubmission(
            maxRecordTime = theRecordTime,
            submissionId = Ref.LedgerString.assertFromString("foobar"),
            participantId = Ref.ParticipantId.assertFromString("participant"),
            config = theDefaultConfig,
          )
        )
      )

      val configSubm = subm.getConfigurationSubmission
      Conversions.parseTimestamp(configSubm.getMaximumRecordTime) shouldEqual theRecordTime
      configSubm.getSubmissionId shouldEqual "foobar"
      Configuration.decode(configSubm.getConfiguration) shouldEqual Right(theDefaultConfig)
    }

    "pre-execute config submissions" in KVTest.runTest {
      for {
        preExecutionResult <- preExecuteConfig(
          configModify = c => c.copy(generation = c.generation + 1),
          submissionId = Ref.LedgerString.assertFromString("config"),
        )
      } yield preExecutionResult.successfulLogEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
    }

    "check generation" in KVTest.runTest {
      for {
        logEntry <- submitConfig(
          configModify = c => c.copy(generation = c.generation + 1),
          submissionId = Ref.LedgerString.assertFromString("submission0"),
        )
        newConfig <- getConfiguration

        // Change again, but without bumping generation.
        logEntry2 <- submitConfig(
          configModify = c => c.copy(generation = c.generation),
          submissionId = Ref.LedgerString.assertFromString("submission1"),
        )
        newConfig2 <- getConfiguration

      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        logEntry.getConfigurationEntry.getSubmissionId shouldEqual "submission0"
        newConfig.generation shouldEqual 1

        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY
        logEntry2.getConfigurationRejectionEntry.getSubmissionId shouldEqual "submission1"
        newConfig2 shouldEqual newConfig
      }
    }

    "reject expired submissions" in KVTest.runTest {
      for {
        logEntry <- submitConfig(
          minMaxRecordTimeDelta = Duration.ofMinutes(-1),
          configModify = { c =>
            c.copy(generation = c.generation + 1)
          },
          submissionId = Ref.LedgerString.assertFromString("some-submission-id"),
        )
      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY
        logEntry.getConfigurationRejectionEntry.getReasonCase shouldEqual DamlConfigurationRejectionEntry.ReasonCase.TIMED_OUT
      }
    }

    "authorize submission" in KVTest.runTest {
      val p0 = mkParticipantId(0)
      val p1 = mkParticipantId(1)

      for {
        // Set a configuration with an authorized participant id
        logEntry0 <- submitConfig(
          { c =>
            c.copy(
              generation = c.generation + 1
            )
          },
          submissionId = Ref.LedgerString.assertFromString("submission-id-1"),
        )

        //
        // A well authorized submission
        //

        logEntry1 <- withParticipantId(p0) {
          submitConfig(
            { c =>
              c.copy(
                generation = c.generation + 1
              )
            },
            submissionId = Ref.LedgerString.assertFromString("submission-id-2"),
          )
        }

        //
        // A badly authorized submission
        //

        logEntry2 <- withParticipantId(p1) {
          submitConfig(
            { c =>
              c.copy(
                generation = c.generation + 1
              )
            },
            submissionId = Ref.LedgerString.assertFromString("submission-id-3"),
          )
        }

      } yield {
        logEntry0.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        logEntry1.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY

        logEntry2.getPayloadCase shouldEqual
          DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY
        logEntry2.getConfigurationRejectionEntry.getReasonCase shouldEqual
          DamlConfigurationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED

      }
    }

    "reject duplicate" in KVTest.runTest {
      for {
        logEntry0 <- submitConfig(
          { c =>
            c.copy(
              generation = c.generation + 1
            )
          },
          submissionId = Ref.LedgerString.assertFromString("submission-id-1"),
        )

        logEntry1 <- submitConfig(
          { c =>
            c.copy(
              generation = c.generation + 1
            )
          },
          submissionId = Ref.LedgerString.assertFromString("submission-id-1"),
        )

      } yield {
        logEntry0.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        logEntry1.getPayloadCase shouldEqual
          DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY
        logEntry1.getConfigurationRejectionEntry.getReasonCase shouldEqual
          DamlConfigurationRejectionEntry.ReasonCase.DUPLICATE_SUBMISSION

      }
    }

    "update metrics" in KVTest.runTest {
      for {
        //Submit config twice to force one acceptance and one rejection on duplicate
        _ <- submitConfig(
          { c =>
            c.copy(
              generation = c.generation + 1
            )
          },
          submissionId = Ref.LedgerString.assertFromString("submission-id-1"),
        )

        _ <- submitConfig(
          { c =>
            c.copy(
              generation = c.generation + 1
            )
          },
          submissionId = Ref.LedgerString.assertFromString("submission-id-1"),
        )
      } yield {
        // Check that we're updating the metrics (assuming this test at least has been run)
        metrics.daml.kvutils.committer.config.accepts.getCount should be >= 1L
        metrics.daml.kvutils.committer.config.rejections.getCount should be >= 1L
        metrics.daml.kvutils.committer.runTimer("config").getCount should be >= 1L
      }
    }
  }

}
