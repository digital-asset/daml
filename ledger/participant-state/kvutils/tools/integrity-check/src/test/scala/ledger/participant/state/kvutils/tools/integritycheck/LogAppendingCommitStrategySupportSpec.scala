// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlConfigurationEntry,
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocationEntry,
  DamlStateKey,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.LogAppendingCommitStrategySupportSpec._
import com.daml.ledger.participant.state.kvutils.{DamlKvutils, Envelope, Version}
import com.daml.ledger.participant.state.protobuf.LedgerConfiguration
import com.google.protobuf.{ByteString, Empty}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.ExecutionContext

final class LogAppendingCommitStrategySupportSpec extends AnyWordSpec with Matchers with Inside {
  private val support = new LogAppendingCommitStrategySupport()(ExecutionContext.global)

  "checking the entries are readable" should {
    "parse a log entry" in {
      val key = aValidLogEntryId
      val value = aValidLogEntry
      inside(support.checkEntryIsReadable(key, value)) {
        case Right(()) => succeed
      }
    }

    "parse a state entry" in {
      val key = aValidStateKey
      val value = aValidStateValue
      inside(support.checkEntryIsReadable(key, value)) {
        case Right(()) => succeed
      }
    }

    "fail on an invalid envelope" in {
      val value = ByteString.copyFromUtf8("invalid envelope")
      inside(support.checkEntryIsReadable(noKey, value)) {
        case Left(message) => message should startWith("Invalid value envelope:")
      }
    }

    "fail on an unknown entry" in {
      val value = DamlKvutils.Envelope.newBuilder
        .setVersion(Version.version)
        .build()
        .toByteString
      inside(support.checkEntryIsReadable(noKey, value)) {
        case Left(_) => succeed
      }
    }

    "fail on a submission entry" in {
      val value = DamlKvutils.Envelope.newBuilder
        .setVersion(Version.version)
        .setKind(DamlKvutils.Envelope.MessageKind.SUBMISSION)
        .build()
        .toByteString
      inside(support.checkEntryIsReadable(noKey, value)) {
        case Left(message) => message should startWith("Unexpected submission message:")
      }
    }

    "fail on a submission batch entry" in {
      val value = DamlKvutils.Envelope.newBuilder
        .setVersion(Version.version)
        .setKind(DamlKvutils.Envelope.MessageKind.SUBMISSION_BATCH)
        .build()
        .toByteString
      inside(support.checkEntryIsReadable(noKey, value)) {
        case Left(message) => message should startWith("Unexpected submission batch message:")
      }
    }

    "fail on a log entry with an invalid payload" in {
      val key = aValidLogEntryId
      val value = Envelope.enclose(DamlLogEntry.newBuilder.build())
      inside(support.checkEntryIsReadable(key, value)) {
        case Left(message) => message should be("Log entry payload not set.")
      }
    }

    "fail on a state entry with an invalid key" in {
      val key = DamlStateKey.newBuilder.build().toByteString
      val value = aValidStateValue
      inside(support.checkEntryIsReadable(key, value)) {
        case Left(message) => message should be("State key not set.")
      }
    }

    "fail on a state entry with an invalid value" in {
      val key = aValidStateKey
      val value = Envelope.enclose(DamlStateValue.newBuilder.build())
      inside(support.checkEntryIsReadable(key, value)) {
        case Left(message) => message should be("State value not set.")
      }
    }

    // We cannot differentiate between log entry IDs and state keys based on structure, as they are
    // similar and we don't include any kind of differentiator. We therefore have to assume that we
    // never get them confused. Functional tests should ensure this.

    "unfortunately, allow a log entry ID with a state value" in {
      val key = aValidLogEntryId
      val value = aValidStateValue
      inside(support.checkEntryIsReadable(key, value)) {
        case Right(()) => succeed
      }
    }

    "unfortunately, allow a state key with a log entry" in {
      val key = aValidStateKey
      val value = aValidLogEntry
      inside(support.checkEntryIsReadable(key, value)) {
        case Right(()) => succeed
      }
    }
  }
}

object LogAppendingCommitStrategySupportSpec {
  private val noKey: ByteString = ByteString.EMPTY

  private val aValidLogEntryId: ByteString =
    DamlLogEntryId.newBuilder.setEntryId(ByteString.copyFromUtf8("entry-id")).build().toByteString

  private val aValidLogEntry: ByteString =
    Envelope.enclose(
      DamlLogEntry.newBuilder
        .setPartyAllocationEntry(DamlPartyAllocationEntry.newBuilder.setParty("Alice"))
        .build())

  private val aValidStateKey: ByteString =
    DamlStateKey.newBuilder.setConfiguration(Empty.getDefaultInstance).build().toByteString

  private val aValidStateValue: ByteString =
    Envelope.enclose(
      DamlStateValue.newBuilder
        .setConfigurationEntry(
          DamlConfigurationEntry.newBuilder
            .setParticipantId("participant")
            .setConfiguration(LedgerConfiguration.newBuilder.setGeneration(1)))
        .build())
}
