// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import java.time.ZoneOffset.UTC
import java.time.{Instant, ZonedDateTime}

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocation,
  DamlPartyAllocationEntry,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.{Bytes, Envelope}
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateReaderSpec._
import com.daml.ledger.participant.state.kvutils.api.LedgerEntry.{Heartbeat, LedgerRecord}
import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar._
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future

class KeyValueParticipantStateReaderSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll {

  private val start: Instant = Instant.from(ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 0, UTC))

  "participant state reader" should {
    "stream offsets and heartbeats from the start" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(1, 1)), aLogEntryId(1), aWrappedLogEntry),
        Heartbeat(Offset(Array(1, 2)), start.plusSeconds(1)),
        LedgerRecord(Offset(Array(1, 3)), aLogEntryId(3), aWrappedLogEntry),
        Heartbeat(Offset(Array(1, 4)), start.plusSeconds(2)),
        Heartbeat(Offset(Array(1, 5)), start.plusSeconds(3)),
        LedgerRecord(Offset(Array(1, 6)), aLogEntryId(6), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 6
        actual shouldBe Seq(
          Offset(Array(1, 1, 0)),
          Offset(Array(1, 2, 0)),
          Offset(Array(1, 3, 0)),
          Offset(Array(1, 4, 0)),
          Offset(Array(1, 5, 0)),
          Offset(Array(1, 6, 0)),
        )
      }
    }

    "stream offsets and heartbeats from a given offset" in {
      val reader = readerStreamingFrom(
        offset = Some(Offset(Array(1, 4))),
        LedgerRecord(Offset(Array(1, 5)), aLogEntryId(5), aWrappedLogEntry),
        LedgerRecord(Offset(Array(1, 6)), aLogEntryId(6), aWrappedLogEntry),
        Heartbeat(Offset(Array(1, 7)), start.plusSeconds(2)),
        Heartbeat(Offset(Array(1, 8)), start.plusSeconds(3)),
        LedgerRecord(Offset(Array(1, 9)), aLogEntryId(9), aWrappedLogEntry),
        Heartbeat(Offset(Array(1, 10)), start.plusSeconds(4)),
        LedgerRecord(Offset(Array(1, 11)), aLogEntryId(11), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(Some(Offset(Array(1, 4, 0))))

      offsetsFrom(stream).map { actual =>
        actual should have size 7
        actual shouldBe Seq(
          Offset(Array(1, 5, 0)),
          Offset(Array(1, 6, 0)),
          Offset(Array(1, 7, 0)),
          Offset(Array(1, 8, 0)),
          Offset(Array(1, 9, 0)),
          Offset(Array(1, 10, 0)),
          Offset(Array(1, 11, 0)),
        )
      }
    }

    "remove index suffix when streaming from underlying reader" in {
      val reader = readerStreamingFrom(
        offset = Some(Offset(Array(1, 1))),
        LedgerRecord(Offset(Array(1, 2)), aLogEntryId(2), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(Some(Offset(Array(1, 1, 0))))

      offsetsFrom(stream).map { actual =>
        actual should have size 1
        actual shouldBe Seq(Offset(Array(1, 2, 0)))
      }
    }

    "append index to internal offset" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(1)), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(Offset(Array(2)), aLogEntryId(2), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 2
        actual shouldBe Seq(Offset(Array(1, 0)), Offset(Array(2, 0)))
      }
    }

    "skip events before specified offset" in {
      val reader = readerStreamingFromAnyOffset(
        LedgerRecord(Offset(Array(1)), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(Offset(Array(2)), aLogEntryId(2), aWrappedLogEntry),
        Heartbeat(Offset(Array(3)), start),
        LedgerRecord(Offset(Array(4)), aLogEntryId(4), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader)

      Future
        .sequence(
          Seq(
            offsetsFrom(instance.stateUpdates(None)),
            offsetsFrom(instance.stateUpdates(Some(Offset(Array(1, 0))))),
            offsetsFrom(instance.stateUpdates(Some(Offset(Array(3, 0))))),
            offsetsFrom(instance.stateUpdates(Some(Offset(Array(4, 0))))),
          )
        )
        .map {
          case Seq(all, afterFirst, beforeLast, afterLast) =>
            all should have size 4
            afterFirst should have size 3
            beforeLast should have size 1
            afterLast should have size 0
        }
    }

    "throw in case of an invalid log entry received" in {
      val anInvalidEnvelope = ByteString.copyFrom(Array[Byte](0, 1, 2))
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(0, 0)), aLogEntryId(0), anInvalidEnvelope),
      )
      val instance = new KeyValueParticipantStateReader(reader)

      offsetsFrom(instance.stateUpdates(None)).failed.map { _ =>
        succeed
      }
    }

    "throw in case of an envelope without a log entry received" in {
      val aStateValue = DamlStateValue.newBuilder
        .setParty(
          DamlPartyAllocation.newBuilder
            .setParticipantId("aParticipantId")
            .setDisplayName("participant"))
        .build
      val anInvalidEnvelopeMessage = Envelope.enclose(aStateValue)
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(0, 0)), aLogEntryId(0), anInvalidEnvelopeMessage),
      )
      val instance = new KeyValueParticipantStateReader(reader)

      offsetsFrom(instance.stateUpdates(None)).failed.map { _ =>
        succeed
      }
    }
  }

  private def offsetsFrom(stream: Source[(Offset, Update), NotUsed]): Future[Seq[Offset]] =
    stream.runWith(Sink.seq).map(_.map(_._1))
}

object KeyValueParticipantStateReaderSpec {

  private val aLogEntry = DamlLogEntry
    .newBuilder()
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder().setParty("aParty").setParticipantId("aParticipant"))
    .build()

  private val aWrappedLogEntry = Envelope.enclose(aLogEntry)

  private def aLogEntryId(index: Int): Bytes =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFrom(s"id-$index".getBytes))
      .build
      .toByteString

  private def readerStreamingFrom(offset: Option[Offset], items: LedgerEntry*): LedgerReader = {
    val reader = mock[LedgerReader]
    val stream = Source.fromIterator(() => items.iterator)
    when(reader.events(offset)).thenReturn(stream)
    reader
  }

  private def readerStreamingFromAnyOffset(items: LedgerEntry*): LedgerReader = {
    val reader = mock[LedgerReader]
    val stream = Source.fromIterator(() => items.iterator)
    when(reader.events(any[Option[Offset]]())).thenReturn(stream)
    reader
  }

}
