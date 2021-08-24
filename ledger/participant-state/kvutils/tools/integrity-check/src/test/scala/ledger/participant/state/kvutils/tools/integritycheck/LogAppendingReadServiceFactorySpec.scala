// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.tools.integritycheck

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocationEntry,
}
import com.daml.ledger.participant.state.kvutils.{Envelope, Raw}
import com.daml.ledger.participant.state.v2.Update
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration.Duration

final class LogAppendingReadServiceFactorySpec extends AsyncWordSpec with Matchers {

  "LogAppendingReadServiceFactory" should {
    "handle empty blocks" in {
      val factory = createFactory()
      factory.appendBlock(Seq.empty)

      factory.createReadService
        .stateUpdates(None)
        .runWith(Sink.fold(0)((n, _) => n + 1))
        .map(count => count shouldBe 0)
    }

    "handle non-empty blocks" in {
      val factory = createFactory()
      factory.appendBlock(List(aSerializedLogEntryId -> aWrappedLogEntry))

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

      factory.appendBlock(List(aSerializedLogEntryId -> aWrappedLogEntry))

      readService
        .stateUpdates(None)
        .runWith(Sink.seq)
        .map { updates =>
          updates.size shouldBe 1
          updates.head._2 should be(aPartyAddedToParticipantUpdate)
        }
    }
  }

  private def createFactory() = new LogAppendingReadServiceFactory(metrics)

  private lazy val actorSystem: ActorSystem = ActorSystem("LogAppendingReadServiceFactorySpec")
  private lazy implicit val materializer: Materializer = Materializer(actorSystem)
  private lazy val metrics = new Metrics(new MetricRegistry)

  private val AnEntryId = "AnEntryId"
  private lazy val aLogEntryId =
    DamlLogEntryId.newBuilder().setEntryId(ByteString.copyFromUtf8(AnEntryId)).build()

  private lazy val APartyName = "aParty"
  private lazy val AParticipantId = "aParticipant"
  private lazy val ATimestampInSeconds = 1234L
  private lazy val aLogEntry = DamlLogEntry
    .newBuilder()
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder().setParty(APartyName).setParticipantId(AParticipantId)
    )
    .setRecordTime(com.google.protobuf.Timestamp.newBuilder.setSeconds(ATimestampInSeconds))
    .build()

  private lazy val aPartyAddedToParticipantUpdate = Update.PartyAddedToParticipant(
    Ref.Party.assertFromString(APartyName),
    "",
    Ref.ParticipantId.assertFromString(AParticipantId),
    Timestamp.assertFromLong(Duration(ATimestampInSeconds, TimeUnit.SECONDS).toMicros),
    None,
  )

  private lazy val aSerializedLogEntryId = Raw.LogEntryId(aLogEntryId)
  private lazy val aWrappedLogEntry = Envelope.enclose(aLogEntry)
}
