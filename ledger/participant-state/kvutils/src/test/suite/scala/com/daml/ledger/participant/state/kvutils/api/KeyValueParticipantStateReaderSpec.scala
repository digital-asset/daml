// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import com.google.protobuf.ByteString
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.DamlKvutils.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocationEntry
}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.participant.state.v1.{Offset, Update}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
      val result = offsetsFrom(stream)

      result.size shouldBe 1
      result.head shouldBe Offset(Array(1, 2, 0))
    }

    "append index to internal offset" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(1)), aLogEntryId, aWrappedLogEntry),
        LedgerRecord(Offset(Array(2)), aLogEntryId, aWrappedLogEntry)
      )
      val instance = new KeyValueParticipantStateReader(reader)
      val stream = instance.stateUpdates(None)
      val result = offsetsFrom(stream)

      result.size shouldBe 2
      result shouldBe Seq(Offset(Array(1, 0)), Offset(Array(2, 0)))
    }

    "skip events before specified offset" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(Offset(Array(1)), aLogEntryId, aWrappedLogEntry),
        LedgerRecord(Offset(Array(2)), aLogEntryId, aWrappedLogEntry),
        LedgerRecord(Offset(Array(3)), aLogEntryId, aWrappedLogEntry)
      )
      val instance = new KeyValueParticipantStateReader(reader)

      offsetsFrom(instance.stateUpdates(None)).size shouldBe 3
      offsetsFrom(instance.stateUpdates(Some(Offset(Array(1, 0))))).size shouldBe 2
      offsetsFrom(instance.stateUpdates(Some(Offset(Array(3, 0))))).isEmpty shouldBe true
    }
  }

  private val aLogEntry = DamlLogEntry
    .newBuilder()
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder().setParty("aParty").setParticipantId("aParticipant"))
    .build()

  private val aWrappedLogEntry = Envelope.enclose(aLogEntry).toByteArray()

  private def aLogEntryId =
    DamlLogEntryId.newBuilder
      .setEntryId(ByteString.copyFrom("anId".getBytes))
      .build

  private val DefaultTimeout = Duration(1, "minutes")

  private def readerStreamingFrom(offset: Option[Offset], items: LedgerRecord*): LedgerReader = {
    val reader = mock[LedgerReader]
    val stream = Source.fromIterator(() => items.toIterator)
    if (offset.isDefined) {
      when(reader.events(offset)).thenReturn(stream)
    } else {
      when(reader.events(any[Option[Offset]]())).thenReturn(stream)
    }
    reader
  }

  private def offsetsFrom(stream: Source[(Offset, Update), NotUsed]): Seq[Offset] =
    Await.result(stream.runWith(Sink.seq), DefaultTimeout).map(_._1)
}
