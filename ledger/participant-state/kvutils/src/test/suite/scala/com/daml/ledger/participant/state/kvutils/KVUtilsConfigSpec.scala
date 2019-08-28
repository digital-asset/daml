// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils

import java.time.Duration

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class KVUtilsConfigSpec extends WordSpec with Matchers {
  import KVTest._
  import TestHelpers._

  "configuration" should {

    "be able to build, pack, unpack and parse" in {
      val subm = KeyValueSubmission.unpackDamlSubmission(
        KeyValueSubmission.packDamlSubmission(
          KeyValueSubmission.configurationToSubmission(
            maxRecordTime = theRecordTime,
            submissionId = "foobar",
            config = theDefaultConfig
          )))

      val configSubm = subm.getConfigurationSubmission
      Conversions.parseTimestamp(configSubm.getMaximumRecordTime) shouldEqual theRecordTime
      configSubm.getSubmissionId shouldEqual "foobar"
      Conversions.parseDamlConfiguration(configSubm.getConfiguration) shouldEqual Success(
        theDefaultConfig)
    }

    "check generation" in KVTest.runTest {
      for {
        logEntry <- submitConfig(
          configModify = c => c.copy(generation = c.generation + 1, openWorld = false),
          submissionId = "submission0"
        )
        newConfig <- getConfiguration

        // Change again, but without bumping generation.
        logEntry2 <- submitConfig(
          configModify = c => c.copy(generation = c.generation, openWorld = true),
          submissionId = "submission1"
        )
        newConfig2 <- getConfiguration

      } yield {
        logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
        logEntry.getConfigurationEntry.getSubmissionId shouldEqual "submission0"
        newConfig.generation shouldEqual 1
        newConfig.openWorld shouldEqual false

        logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY
        logEntry2.getConfigurationRejectionEntry.getSubmissionId shouldEqual "submission1"
        newConfig2 shouldEqual newConfig

      }
    }

    "reject expired submissions" in KVTest.runTest {
      for {
        logEntry <- submitConfig(mrtDelta = Duration.ofMinutes(-1), configModify = { c =>
          c.copy(generation = c.generation + 1)
        })
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
        logEntry0 <- submitConfig { c =>
          c.copy(
            generation = c.generation + 1,
            authorizedParticipantId = Some(p0)
          )
        }

        //
        // A well authorized submission
        //

        logEntry1 <- withParticipantId(p0) {
          submitConfig(
            c =>
              c.copy(
                generation = c.generation + 1,
                openWorld = false
            )
          )
        }

        //
        // A badly authorized submission
        //

        logEntry2 <- withParticipantId(p1) {
          submitConfig(
            c =>
              c.copy(
                generation = c.generation + 1,
                openWorld = false
            ))
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
  }

}
