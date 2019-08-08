package com.daml.ledger.participant.state.kvutils

import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.committing.Common.DamlStateMap
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId}
import com.digitalasset.daml.lf.data.Time.Timestamp
import org.scalatest.{Matchers, WordSpec}

import scala.util.Success

class KVUtilsConfigSpec extends WordSpec with Matchers {
  import TestHelpers._

  private def submitConfig(
      configModify: Configuration => Configuration,
      entryId: DamlLogEntryId = mkEntryId(0),
      defaultConfig: Configuration = theDefaultConfig,
      recordTime: Timestamp = theRecordTime,
      participantId: ParticipantId = mkParticipantId(0),
      inputState: DamlStateMap = Map.empty): (DamlLogEntry, DamlStateMap) = {
    val subm =
      KeyValueSubmission.configurationToSubmission(
        maxRecordTime = recordTime,
        config = configModify(defaultConfig),
      )
    KeyValueCommitting.processSubmission(
      engine = null,
      entryId = entryId,
      recordTime = recordTime,
      defaultConfig = defaultConfig,
      submission = subm,
      participantId = participantId,
      inputState = inputState.mapValues(Some(_))
    )
  }

  "configuration" should {

    "can build, pack, unpack and parse" in {
      val subm = KeyValueSubmission.unpackDamlSubmission(
        KeyValueSubmission.packDamlSubmission(
          KeyValueSubmission.configurationToSubmission(
            maxRecordTime = theRecordTime,
            config = theDefaultConfig,
          )))

      val configSubm = subm.getConfigurationSubmission
      Conversions.parseTimestamp(configSubm.getMaximumRecordTime) shouldEqual theRecordTime
      Conversions.parseDamlConfiguration(configSubm.getConfiguration) shouldEqual Success(
        theDefaultConfig)
    }

    "accept first submission" in {
      val (logEntry, newState) = submitConfig(
        c =>
          c.copy(
            generation = c.generation + 1,
            openWorld = false
        ))

      logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
      newState should
        (contain key Conversions.configurationStateKey and have size (1))
      val acceptedConfig = newState.head._2.getConfiguration
      acceptedConfig.getGeneration shouldEqual 1
      acceptedConfig.getOpenWorld shouldEqual false
    }

    "authorize submission from default config" in {
      val newDefaultConfig = theDefaultConfig.copy(
        authorizedParticipantId = Some(mkParticipantId(0))
      )

      //
      // A well authorized submission
      //

      val (logEntry, newState) = submitConfig(
        c =>
          c.copy(
            generation = c.generation + 1,
            openWorld = false
        ),
        defaultConfig = newDefaultConfig,
        participantId = mkParticipantId(0))

      logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
      newState should
        (contain key Conversions.configurationStateKey and have size 1)

      //
      // A badly authorized submission
      //

      val (logEntry2, newState2) = submitConfig(
        c =>
          c.copy(
            generation = c.generation + 1,
            openWorld = false
        ),
        defaultConfig = newDefaultConfig,
        participantId = mkParticipantId(1))

      logEntry2.getPayloadCase shouldEqual
        DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY
      logEntry2.getConfigurationRejectionEntry.getReasonCase shouldEqual
        DamlConfigurationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED
      newState2 shouldBe 'empty
    }

    "authorize submission from previous config" in {
      // Submit a config which sets the authorized participant
      val (logEntry, newState) = submitConfig(
        c =>
          c.copy(
            generation = c.generation + 1,
            openWorld = false,
            authorizedParticipantId = Some(mkParticipantId(1))
        ))
      logEntry.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
      newState should
        (contain key Conversions.configurationStateKey and have size 1)

      // Submit configuration from an unauthorized participant
      val (logEntry2, newState2) = submitConfig(
        c =>
          c.copy(
            generation = c.generation + 2,
            openWorld = false,
            authorizedParticipantId = None
        ),
        inputState = newState,
        participantId = mkParticipantId(0)
      )
      logEntry2.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_REJECTION_ENTRY
      logEntry2.getConfigurationRejectionEntry.getReasonCase shouldEqual
        DamlConfigurationRejectionEntry.ReasonCase.PARTICIPANT_NOT_AUTHORIZED
      newState2 shouldBe 'empty

      // Submit configuration from the proper participant
      val (logEntry3, newState3) = submitConfig(
        c =>
          c.copy(
            generation = c.generation + 2,
            openWorld = false,
            authorizedParticipantId = None
        ),
        inputState = newState,
        participantId = mkParticipantId(1)
      )
      logEntry3.getPayloadCase shouldEqual DamlLogEntry.PayloadCase.CONFIGURATION_ENTRY
      newState3 should
        (contain key Conversions.configurationStateKey and have size 1)

    }

  }

}
