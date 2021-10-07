// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.time.Instant
import java.util.concurrent.TimeUnit

import akka.stream.scaladsl.Sink
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocationEntry,
}
import com.daml.ledger.participant.state.kvutils.export.SubmissionInfo
import com.daml.ledger.participant.state.kvutils.tools.integritycheck.LogAppendingReadServiceFactorySpec._
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.participant.state.v2.Update
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.Duration

final class LogAppendingReadServiceFactorySpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll {

  "LogAppendingReadServiceFactory" should {
    "handle empty blocks" in {
      val factory = createFactory()
      factory.appendBlock(submissionInfo, Seq.empty)

      factory.createReadService
        .stateUpdates(None)
        .runWith(Sink.fold(0)((n, _) => n + 1))
        .map(count => count shouldBe 0)
    }

    "handle non-empty blocks" in {
      val factory = createFactory()
      factory.appendBlock(submissionInfo, List(aSerializedLogEntryId -> aWrappedLogEntry))

      factory.createReadService
        .stateUpdates(None)
        .runWith(Sink.seq)
        .map { updates =>
          updates.size shouldBe 1
          updates.head._2 should be(aPartyAddedToParticipantUpdate)
        }
    }

    "replay appended blocks, even if they're appended later" in {
      val factory = createFactory()
      val readService = factory.createReadService

      factory.appendBlock(submissionInfo, List(aSerializedLogEntryId -> aWrappedLogEntry))

      readService
        .stateUpdates(None)
        .runWith(Sink.seq)
        .map { updates =>
          updates.size shouldBe 1
          updates.head._2 should be(aPartyAddedToParticipantUpdate)
        }
    }
  }
}

object LogAppendingReadServiceFactorySpec {
  private def createFactory() = {
    val metrics = new Metrics(new MetricRegistry)
    new LogAppendingReadServiceFactory(metrics)
  }

  private val AnEntryId = "AnEntryId"
  private val aLogEntryId =
    DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(AnEntryId)).build()

  private val APartyName = "aParty"
  private val AParticipantId = "aParticipant"
  private val ATimestampInSeconds = 1234L
  private val aLogEntry = DamlLogEntry
    .newBuilder()
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder().setParty(APartyName).setParticipantId(AParticipantId)
    )
    .setRecordTime(com.google.protobuf.Timestamp.newBuilder.setSeconds(ATimestampInSeconds))
    .build()

  private val aPartyAddedToParticipantUpdate = Update.PartyAddedToParticipant(
    Ref.Party.assertFromString(APartyName),
    "",
    Ref.ParticipantId.assertFromString(AParticipantId),
    Timestamp.assertFromLong(Duration(ATimestampInSeconds, TimeUnit.SECONDS).toMicros),
    None,
  )

  private val aSerializedLogEntryId = Raw.LogEntryId(aLogEntryId)
  private val aWrappedLogEntry = Envelope.enclose(aLogEntry)

  private val submissionInfo = SubmissionInfo(
    participantId = Ref.ParticipantId.assertFromString(AParticipantId),
    "correlation ID",
    Raw.Envelope.empty,
    Instant.EPOCH,
  )
}
