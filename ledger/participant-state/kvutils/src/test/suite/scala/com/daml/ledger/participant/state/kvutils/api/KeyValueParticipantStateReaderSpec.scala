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
import com.daml.ledger.participant.state.kvutils.{Bytes, Envelope, KVOffset}
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

  import KVOffset.{fromLong => toOffset}

  private val start: Instant = Instant.from(ZonedDateTime.of(2020, 1, 1, 12, 0, 0, 0, UTC))
  "participant state reader" should {
    "stream offsets and heartbeats from the start" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(toOffset(1), aLogEntryId(1), aWrappedLogEntry),
        Heartbeat(toOffset(2), start.plusSeconds(1)),
        LedgerRecord(toOffset(3), aLogEntryId(3), aWrappedLogEntry),
        Heartbeat(toOffset(4), start.plusSeconds(2)),
        Heartbeat(toOffset(5), start.plusSeconds(3)),
        LedgerRecord(toOffset(6), aLogEntryId(6), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 6
        actual shouldBe Seq(
          toOffset(1, 0),
          toOffset(2, 0),
          toOffset(3, 0),
          toOffset(4, 0),
          toOffset(5, 0),
          toOffset(6, 0),
        )
      }
    }

    "stream offsets and heartbeats from a given offset" in {
      val reader = readerStreamingFrom(
        offset = Some(toOffset(4)),
        LedgerRecord(toOffset(5), aLogEntryId(5), aWrappedLogEntry),
        LedgerRecord(toOffset(6), aLogEntryId(6), aWrappedLogEntry),
        Heartbeat(toOffset(7), start.plusSeconds(2)),
        Heartbeat(toOffset(8), start.plusSeconds(3)),
        LedgerRecord(toOffset(9), aLogEntryId(9), aWrappedLogEntry),
        Heartbeat(toOffset(10), start.plusSeconds(4)),
        LedgerRecord(toOffset(11), aLogEntryId(11), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(Some(toOffset(4, 0)))

      offsetsFrom(stream).map { actual =>
        actual should have size 7
        actual shouldBe Seq(
          toOffset(5, 0),
          toOffset(6, 0),
          toOffset(7, 0),
          toOffset(8, 0),
          toOffset(9, 0),
          toOffset(10, 0),
          toOffset(11, 0),
        )
      }
    }

    "remove index suffix when streaming from underlying reader" in {
      val reader = readerStreamingFrom(
        offset = Some(toOffset(1)),
        LedgerRecord(toOffset(2), aLogEntryId(2), aWrappedLogEntry))
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(Some(toOffset(1, 0)))

      offsetsFrom(stream).map { actual =>
        actual should have size 1
        actual shouldBe Seq(toOffset(2, 0))
      }
    }

    "append index to internal offset" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(toOffset(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(toOffset(2), aLogEntryId(2), aWrappedLogEntry)
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 2
        actual shouldBe Seq(toOffset(1, 0), toOffset(2, 0))
      }
    }

    "skip events before specified offset" in {
      val records = List(
        LedgerRecord(toOffset(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(toOffset(2), aLogEntryId(2), aWrappedLogEntry),
        Heartbeat(toOffset(3), start),
        LedgerRecord(toOffset(4), aLogEntryId(4), aWrappedLogEntry)
      )

      def getInstance(offset: Option[Offset], items: LedgerEntry*) =
        new KeyValueParticipantStateReader(readerStreamingFrom(offset = offset, items: _*))

      val instances = records.tails.flatMap {
        case first :: rest =>
          List(Option(first.offset) -> getInstance(Some(first.offset), rest: _*))
        case _ => Nil
      }.toMap + (None -> getInstance(None, records: _*))

      Future
        .sequence(
          Seq(None, Some(toOffset(1, 0)), Some(toOffset(3, 0)), Some(toOffset(4, 0)))
            .map(offset => offsetsFrom(instances(offset).stateUpdates(offset)))
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
        LedgerRecord(toOffset(0, 0), aLogEntryId(0), anInvalidEnvelope))
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
        LedgerRecord(toOffset(0, 0), aLogEntryId(0), anInvalidEnvelopeMessage))
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
