// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlConfigurationEntry,
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocation,
  DamlPartyAllocationEntry,
  DamlStateKey,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.WriteSetComparison.rawHexString
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.WriteSetComparisonSpec._
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Envelope, Raw, Version}
import com.daml.ledger.participant.state.protobuf.LedgerConfiguration
import com.daml.ledger.validator.StateKeySerializationStrategy
import com.google.protobuf.{ByteString, Empty}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

final class WriteSetComparisonSpec extends AsyncWordSpec with Matchers with Inside {

  private val writeSetComparison =
    new WriteSetComparison(StateKeySerializationStrategy.createDefault())

  "checking the entries are readable" should {
    "parse a log entry" in {
      val key = aValidLogEntryId
      val value = aValidLogEntry
      inside(writeSetComparison.checkEntryIsReadable(key, value)) { case Right(()) =>
        succeed
      }
    }

    "parse a state entry" in {
      val key = aValidStateKey
      val value = aValidStateValue
      inside(writeSetComparison.checkEntryIsReadable(key, value)) { case Right(()) =>
        succeed
      }
    }

    "fail on an invalid envelope" in {
      val value = Raw.Value(ByteString.copyFromUtf8("invalid envelope"))
      inside(writeSetComparison.checkEntryIsReadable(noKey, value)) { case Left(message) =>
        message should startWith("Invalid value envelope:")
      }
    }

    "fail on an unknown entry" in {
      val value = Raw.Value(
        DamlKvutils.Envelope.newBuilder
          .setVersion(Version.version)
          .build()
          .toByteString
      )
      inside(writeSetComparison.checkEntryIsReadable(noKey, value)) { case Left(_) =>
        succeed
      }
    }

    "fail on a submission entry" in {
      val value = Raw.Value(
        DamlKvutils.Envelope.newBuilder
          .setVersion(Version.version)
          .setKind(DamlKvutils.Envelope.MessageKind.SUBMISSION)
          .build()
          .toByteString
      )
      inside(writeSetComparison.checkEntryIsReadable(noKey, value)) { case Left(message) =>
        message should startWith("Unexpected submission message:")
      }
    }

    "fail on a submission batch entry" in {
      val value = Raw.Value(
        DamlKvutils.Envelope.newBuilder
          .setVersion(Version.version)
          .setKind(DamlKvutils.Envelope.MessageKind.SUBMISSION_BATCH)
          .build()
          .toByteString
      )
      inside(writeSetComparison.checkEntryIsReadable(noKey, value)) { case Left(message) =>
        message should startWith("Unexpected submission batch message:")
      }
    }

    "fail on a log entry with an invalid payload" in {
      val key = aValidLogEntryId
      val value = Envelope.enclose(DamlLogEntry.newBuilder.build())
      inside(writeSetComparison.checkEntryIsReadable(key, value)) { case Left(message) =>
        message should be("Log entry payload not set.")
      }
    }

    "fail on a state entry with an invalid key" in {
      val key = Raw.Key(DamlStateKey.newBuilder.build().toByteString)
      val value = aValidStateValue
      inside(writeSetComparison.checkEntryIsReadable(key, value)) { case Left(message) =>
        message should be("State key not set.")
      }
    }

    "fail on a state entry with an invalid value" in {
      val key = aValidStateKey
      val value = Envelope.enclose(DamlStateValue.newBuilder.build())
      inside(writeSetComparison.checkEntryIsReadable(key, value)) { case Left(message) =>
        message should be("State value not set.")
      }
    }

    // We cannot differentiate between log entry IDs and state keys based on structure, as they are
    // similar and we don't include any kind of differentiator. We therefore have to assume that we
    // never get them confused. Functional tests should ensure this.

    "unfortunately, allow a log entry ID with a state value" in {
      val key = aValidLogEntryId
      val value = aValidStateValue
      inside(writeSetComparison.checkEntryIsReadable(key, value)) { case Right(()) =>
        succeed
      }
    }

    "unfortunately, allow a state key with a log entry" in {
      val key = aValidStateKey
      val value = aValidLogEntry
      inside(writeSetComparison.checkEntryIsReadable(key, value)) { case Right(()) =>
        succeed
      }
    }
  }

  "compareWriteSets" should {
    "return None in case of no difference" in {
      val aWriteSet = Seq(aValidStateKey -> aValidStateValue)
      val result = writeSetComparison.compareWriteSets(aWriteSet, aWriteSet)

      result shouldBe None
    }

    "explain the difference in values" in {
      val value1 = aConfigurationStateValue(generation = 1)
      val value2 = aConfigurationStateValue(generation = 2)
      val result = writeSetComparison.compareWriteSets(
        Seq(aValidStateKey -> Envelope.enclose(value1)),
        Seq(aValidStateKey -> Envelope.enclose(value2)),
      )

      result match {
        case None =>
          fail("Expected a failure message.")
        case Some(explanation) =>
          explanation should include(s"Expected: $value1")
          explanation should include(s"Actual: Right($value2)")
      }
    }

    "return all explanations in case of multiple differences" in {
      val valueA1 = Envelope.enclose(aPartyAllocationStateValue("Alice"))
      val valueA2 = Envelope.enclose(aPartyAllocationStateValue("Alan"))
      val valueB1 = Envelope.enclose(aPartyAllocationStateValue("Bob"))
      val valueB2 = Envelope.enclose(aPartyAllocationStateValue("Barbara"))
      val result = writeSetComparison.compareWriteSets(
        Seq(
          aPartyAllocationStateKey("a") -> valueA1,
          aPartyAllocationStateKey("b") -> valueB1,
        ),
        Seq(
          aPartyAllocationStateKey("a") -> valueA2,
          aPartyAllocationStateKey("b") -> valueB2,
        ),
      )

      result match {
        case None =>
          fail("Expected a failure message.")
        case Some(explanation) =>
          explanation should include(
            s"expected value:    ${rawHexString(valueA1)}\n vs. actual value: ${rawHexString(valueA2)}\n"
          )
          explanation should include(
            s"expected value:    ${rawHexString(valueB1)}\n vs. actual value: ${rawHexString(valueB2)}\n"
          )
      }
    }

    "return differing keys" in {
      val result = writeSetComparison.compareWriteSets(
        Seq(aPartyAllocationStateKey("a") -> Envelope.enclose(aPartyAllocationStateValue("Alice"))),
        Seq(aPartyAllocationStateKey("b") -> Envelope.enclose(aPartyAllocationStateValue("Bob"))),
      )

      result match {
        case None =>
          fail("Expected a failure message.")
        case Some(explanation) =>
          explanation should include("expected key")
          explanation should include("actual key")
      }
    }
  }

}

object WriteSetComparisonSpec {

  private val noKey: Raw.Key =
    Raw.Key(ByteString.EMPTY)

  private val aValidLogEntryId: Raw.Key =
    Raw.Key(
      DamlLogEntryId.newBuilder
        .setEntryId(ByteString.copyFromUtf8("entry-id"))
        .build()
        .toByteString
    )

  private val aValidLogEntry: Raw.Value =
    Envelope.enclose(
      DamlLogEntry.newBuilder
        .setPartyAllocationEntry(DamlPartyAllocationEntry.newBuilder.setParty("Alice"))
        .build()
    )

  private val aValidStateKey: Raw.Key =
    aConfigurationStateKey

  private def aConfigurationStateKey =
    Raw.Key(DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance).build().toByteString)

  private def aPartyAllocationStateKey(partyId: String): Raw.Key =
    Raw.Key(DamlStateKey.newBuilder.setParty(partyId).build().toByteString)

  private val aValidStateValue: Raw.Value =
    Envelope.enclose(aConfigurationStateValue(generation = 1))

  private def aPartyAllocationStateValue(displayName: String): DamlStateValue =
    DamlStateValue.newBuilder
      .setParty(
        DamlPartyAllocation.newBuilder
          .setParticipantId("participant-id")
          .setDisplayName(displayName)
      )
      .build()

  private def aConfigurationStateValue(generation: Long): DamlStateValue =
    DamlStateValue.newBuilder
      .setConfigurationEntry(
        DamlConfigurationEntry.newBuilder
          .setParticipantId("participant")
          .setConfiguration(LedgerConfiguration.newBuilder.setGeneration(generation))
      )
      .build()

}
