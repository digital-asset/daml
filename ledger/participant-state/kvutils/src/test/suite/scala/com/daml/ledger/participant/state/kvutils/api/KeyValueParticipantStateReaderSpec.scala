// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.participant.state.kvutils.DamlKvutils._
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateReaderSpec._
import com.daml.ledger.participant.state.kvutils.{Bytes, Envelope, KVOffset}
import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar._
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future

class KeyValueParticipantStateReaderSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll {

  import KVOffset.{fromLong => toOffset}

  private def newMetrics = new Metrics(new MetricRegistry)

  "participant state reader" should {
    "stream offsets from the start" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(toOffset(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(toOffset(2), aLogEntryId(2), aWrappedLogEntry),
        LedgerRecord(toOffset(3), aLogEntryId(3), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader, newMetrics)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 3
        actual shouldBe Seq(
          toOffset(1),
          toOffset(2),
          toOffset(3),
        )
      }
    }

    "stream offsets from a given offset" in {
      val reader = readerStreamingFrom(
        offset = Some(toOffset(4)),
        LedgerRecord(toOffset(5), aLogEntryId(5), aWrappedLogEntry),
        LedgerRecord(toOffset(6), aLogEntryId(6), aWrappedLogEntry),
        LedgerRecord(toOffset(7), aLogEntryId(7), aWrappedLogEntry),
        LedgerRecord(toOffset(8), aLogEntryId(8), aWrappedLogEntry),
      )
      val instance = new KeyValueParticipantStateReader(reader, newMetrics)
      val stream = instance.stateUpdates(Some(toOffset(4)))

      offsetsFrom(stream).map { actual =>
        actual should have size 4
        actual shouldBe Seq(
          toOffset(5),
          toOffset(6),
          toOffset(7),
          toOffset(8),
        )
      }
    }

    "remove index suffix when streaming from underlying reader" in {
      val reader = readerStreamingFrom(
        offset = Some(toOffset(1)),
        LedgerRecord(toOffset(2), aLogEntryId(2), aWrappedLogEntry))
      val instance = new KeyValueParticipantStateReader(reader, newMetrics)
      val stream = instance.stateUpdates(Some(toOffset(1)))

      offsetsFrom(stream).map { actual =>
        actual should have size 1
        actual shouldBe Seq(toOffset(2))
      }
    }

    "append index to internal offset" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(toOffset(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(toOffset(2), aLogEntryId(2), aWrappedLogEntry)
      )
      val instance = new KeyValueParticipantStateReader(reader, newMetrics)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 2
        actual shouldBe Seq(toOffset(1), toOffset(2))
      }
    }

    "skip events before specified offset" in {
      val records = List(
        LedgerRecord(toOffset(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(toOffset(2), aLogEntryId(2), aWrappedLogEntry),
        LedgerRecord(toOffset(3), aLogEntryId(3), aWrappedLogEntry)
      )

      def getInstance(offset: Option[Offset], items: LedgerRecord*) =
        new KeyValueParticipantStateReader(
          readerStreamingFrom(offset = offset, items: _*),
          newMetrics)

      val instances = records.tails.flatMap {
        case first :: rest =>
          List(Option(first.offset) -> getInstance(Some(first.offset), rest: _*))
        case _ => Nil
      }.toMap + (None -> getInstance(None, records: _*))

      Future
        .sequence(
          Seq(None, Some(toOffset(1)), Some(toOffset(2)), Some(toOffset(3)))
            .map(offset => offsetsFrom(instances(offset).stateUpdates(offset)))
        )
        .map {
          case Seq(all, afterFirst, beforeLast, afterLast) =>
            all should have size 3
            afterFirst should have size 2
            beforeLast should have size 1
            afterLast should have size 0
        }
    }

    "throw in case of an invalid log entry received" in {
      val anInvalidEnvelope = ByteString.copyFrom(Array[Byte](0, 1, 2))
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(toOffset(0), aLogEntryId(0), anInvalidEnvelope))
      val instance = new KeyValueParticipantStateReader(reader, newMetrics)

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
        LedgerRecord(toOffset(0), aLogEntryId(0), anInvalidEnvelopeMessage))
      val instance = new KeyValueParticipantStateReader(reader, newMetrics)

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

  private def readerStreamingFrom(offset: Option[Offset], items: LedgerRecord*): LedgerReader = {
    val reader = mock[LedgerReader]
    val stream = Source.fromIterator(() => items.iterator)
    when(reader.events(offset)).thenReturn(stream)
    reader
  }

}
