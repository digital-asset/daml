// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocation,
  DamlPartyAllocationEntry,
  DamlStateValue
}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Future

class KeyValueParticipantStateReaderSpec
    extends AsyncWordSpec
    with Matchers
    with MockitoSugar
    with AkkaBeforeAndAfterAll {
  "participant state reader" should {
    "remove index suffix when streaming from underlying reader" in {
      val reader = readerStreamingFrom(
        offset = Some(Offset(Array(1, 1))),
        LedgerRecord(Offset(Array(1, 2)), aLogEntryId, aWrappedLogEntry))
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
        LedgerRecord(Offset(Array(1)), aLogEntryId, aWrappedLogEntry),
        LedgerRecord(Offset(Array(2)), aLogEntryId, aWrappedLogEntry)
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
        LedgerRecord(Offset(Array(1)), aLogEntryId, aWrappedLogEntry),
        LedgerRecord(Offset(Array(2)), aLogEntryId, aWrappedLogEntry),
        LedgerRecord(Offset(Array(3)), aLogEntryId, aWrappedLogEntry)
      )
      val instance = new KeyValueParticipantStateReader(reader)

      Future
        .sequence(
          Seq(
            offsetsFrom(instance.stateUpdates(None)),
            offsetsFrom(instance.stateUpdates(Some(Offset(Array(1, 0))))),
            offsetsFrom(instance.stateUpdates(Some(Offset(Array(3, 0)))))
          )
        )
        .map {
          case Seq(all, afterFirst, afterLast) =>
            all should have size 3
            afterFirst should have size 2
            afterLast should have size 0
        }
    }

    "throw in case of an invalid log entry received" in {
      val anInvalidEnvelope = Array[Byte](0, 1, 2)
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(0, 0)), aLogEntryId, anInvalidEnvelope))
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
      val anInvalidEnvelopeMessage = Envelope.enclose(aStateValue).toByteArray
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(0, 0)), aLogEntryId, anInvalidEnvelopeMessage))
      val instance = new KeyValueParticipantStateReader(reader)

      offsetsFrom(instance.stateUpdates(None)).failed.map { _ =>
        succeed
      }
    }
  }

  private val aLogEntry = DamlLogEntry
    .newBuilder()
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder().setParty("aParty").setParticipantId("aParticipant"))
    .build()

  private val aWrappedLogEntry = Envelope.enclose(aLogEntry).toByteArray

  private def aLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFrom("anId".getBytes))
      .build

  private def readerStreamingFrom(offset: Option[Offset], items: LedgerRecord*): LedgerReader = {
    val reader = mock[LedgerReader]
    val stream = Source.fromIterator(() => items.iterator)
    when(reader.events(offset)).thenReturn(stream)
    reader
  }

  private def readerStreamingFromAnyOffset(items: LedgerRecord*): LedgerReader = {
    val reader = mock[LedgerReader]
    val stream = Source.fromIterator(() => items.iterator)
    when(reader.events(any[Option[Offset]]())).thenReturn(stream)
    reader
  }

  private def offsetsFrom(stream: Source[(Offset, Update), NotUsed]): Future[Seq[Offset]] =
    stream.runWith(Sink.seq).map(_.map(_._1))
}
