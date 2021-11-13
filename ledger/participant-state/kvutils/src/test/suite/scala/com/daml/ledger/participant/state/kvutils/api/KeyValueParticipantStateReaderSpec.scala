// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.api

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Source}
import com.codahale.metrics.MetricRegistry
import com.daml.error.ValueSwitch
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateReader.offsetForUpdate
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantStateReaderSpec._
import com.daml.ledger.participant.state.kvutils.store.events.DamlPartyAllocationEntry
import com.daml.ledger.participant.state.kvutils.store.{
  DamlLogEntry,
  DamlLogEntryId,
  DamlPartyAllocation,
  DamlStateValue,
}
import com.daml.ledger.participant.state.kvutils.{Envelope, KVOffsetBuilder, Raw}
import com.daml.ledger.participant.state.v2.Update
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.google.protobuf.ByteString
import org.mockito.Mockito.when
import org.mockito.MockitoSugar._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class KeyValueParticipantStateReaderSpec
    extends AsyncWordSpec
    with Matchers
    with AkkaBeforeAndAfterAll {

  private implicit val loggingContext: LoggingContext = LoggingContext.ForTesting
  private val offsetBuilder = new KVOffsetBuilder(0)

  "participant state reader" should {
    "stream offsets from the start" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(offsetBuilder.of(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(2), aLogEntryId(2), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(3), aLogEntryId(3), aWrappedLogEntry),
      )
      val instance = createInstance(reader)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 3
        actual shouldBe Seq(
          offsetBuilder.of(1),
          offsetBuilder.of(2),
          offsetBuilder.of(3),
        )
      }
    }

    "stream offsets from a given 1 component offset" in {
      val reader = readerStreamingFrom(
        offset = Some(offsetBuilder.of(4)),
        LedgerRecord(offsetBuilder.of(5), aLogEntryId(5), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(6), aLogEntryId(6), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(7), aLogEntryId(7), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(8), aLogEntryId(8), aWrappedLogEntry),
      )
      val instance = createInstance(reader)
      val stream = instance.stateUpdates(Some(offsetBuilder.of(4)))

      offsetsFrom(stream).map { actual =>
        actual should have size 4
        actual shouldBe Seq(
          offsetBuilder.of(5),
          offsetBuilder.of(6),
          offsetBuilder.of(7),
          offsetBuilder.of(8),
        )
      }
    }

    "remove third component of input offset when streaming from underlying reader" in {
      val reader = readerStreamingFrom(
        offset = Some(offsetBuilder.of(1, 2)),
        LedgerRecord(offsetBuilder.of(2), aLogEntryId(2), aWrappedLogEntry),
      )
      val instance = createInstance(reader)
      val stream = instance.stateUpdates(Some(offsetBuilder.of(1, 2, 3)))

      offsetsFrom(stream).map { actual =>
        actual should have size 1
        actual shouldBe Seq(offsetBuilder.of(2))
      }
    }

    "do not append index to underlying reader's offset in case of no more than 1 update" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(offsetBuilder.of(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(2), aLogEntryId(2), aWrappedLogEntry),
      )
      for (updateGenerator <- Seq(zeroUpdateGenerator, singleUpdateGenerator)) {
        val instance = createInstance(reader, updateGenerator)
        val stream = instance.stateUpdates(None)

        offsetsFrom(stream).map { actual =>
          actual should have size 2
          actual shouldBe Seq(offsetBuilder.of(1), offsetBuilder.of(2))
        }
      }
      succeed
    }

    "append index to underlying reader's offset in case of more than 1 update" in {
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(offsetBuilder.of(1, 11), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(2, 22), aLogEntryId(2), aWrappedLogEntry),
      )
      val instance = createInstance(reader, twoUpdatesGenerator)
      val stream = instance.stateUpdates(None)

      offsetsFrom(stream).map { actual =>
        actual should have size 4
        actual shouldBe Seq(
          offsetBuilder.of(1, 11, 0),
          offsetBuilder.of(1, 11, 1),
          offsetBuilder.of(2, 22, 0),
          offsetBuilder.of(2, 22, 1),
        )
      }
    }

    "skip events before specified offset" in {
      val records = List(
        LedgerRecord(offsetBuilder.of(1), aLogEntryId(1), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(2), aLogEntryId(2), aWrappedLogEntry),
        LedgerRecord(offsetBuilder.of(3), aLogEntryId(3), aWrappedLogEntry),
      )

      def getInstance(
          offset: Option[Offset],
          items: LedgerRecord*
      ): KeyValueParticipantStateReader =
        createInstance(readerStreamingFrom(offset = offset, items: _*))

      val instances = records.tails.flatMap {
        case first :: rest =>
          List(Option(first.offset) -> getInstance(Some(first.offset), rest: _*))
        case _ => Nil
      }.toMap + (None -> getInstance(None, records: _*))

      Future
        .sequence(
          Seq(None, Some(offsetBuilder.of(1)), Some(offsetBuilder.of(2)), Some(offsetBuilder.of(3)))
            .map(offset => offsetsFrom(instances(offset).stateUpdates(offset)))
        )
        .map { case Seq(all, afterFirst, beforeLast, afterLast) =>
          all should have size 3
          afterFirst should have size 2
          beforeLast should have size 1
          afterLast should have size 0
        }
    }

    "throw in case of an invalid log entry received" in {
      val anInvalidEnvelope = Raw.Envelope(ByteString.copyFrom(Array[Byte](0, 1, 2)))
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(offsetBuilder.of(0), aLogEntryId(0), anInvalidEnvelope),
      )
      val instance = createInstance(reader)

      offsetsFrom(instance.stateUpdates(None)).failed.map { _ =>
        succeed
      }
    }

    "throw in case of an envelope without a log entry received" in {
      val anInvalidEnvelopeMessage = Envelope.enclose(aStateValue)
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(offsetBuilder.of(0), aLogEntryId(0), anInvalidEnvelopeMessage),
      )
      val instance = createInstance(reader)

      offsetsFrom(instance.stateUpdates(None)).failed.map { _ =>
        succeed
      }
    }

    "skip in case of an envelope without a log entry received and `failOnUnexpectedEvent` is `false`" in {
      val anInvalidEnvelopeMessage = Envelope.enclose(aStateValue)
      val reader = readerStreamingFrom(
        offset = None,
        LedgerRecord(offsetBuilder.of(0), aLogEntryId(0), anInvalidEnvelopeMessage),
      )
      val instance = createInstance(reader, failOnUnexpectedEvent = false)

      offsetsFrom(instance.stateUpdates(None)).map { offsets =>
        offsets shouldBe Seq.empty
      }
    }
  }

  "offsetForUpdate" should {
    "not overwrite middle offset from record in case of 2 updates" in {
      val offsetFromRecord = offsetBuilder.of(1, 2)
      for (subOffset <- Seq(0, 1)) {
        (offsetForUpdate(offsetFromRecord, subOffset, 2)
          shouldBe offsetBuilder.of(1, 2, subOffset))
      }
      succeed
    }

    "use original offset in case less than 2 updates" in {
      val expectedOffset = offsetBuilder.of(1, 2, 3)
      for (totalUpdates <- Seq(0, 1)) {
        for (i <- 0 until totalUpdates) {
          offsetForUpdate(expectedOffset, i, totalUpdates) shouldBe expectedOffset
        }
      }
      succeed
    }
  }

  private def offsetsFrom(stream: Source[(Offset, Update), NotUsed]): Future[Seq[Offset]] =
    stream.runWith(Sink.seq).map(_.map(_._1))
}

object KeyValueParticipantStateReaderSpec {

  private val aLogEntry = DamlLogEntry
    .newBuilder()
    .setPartyAllocationEntry(
      DamlPartyAllocationEntry.newBuilder().setParty("aParty").setParticipantId("aParticipant")
    )
    .setRecordTime(com.google.protobuf.Timestamp.newBuilder.setSeconds(1234))
    .build()

  private val aWrappedLogEntry = Envelope.enclose(aLogEntry)

  private val zeroUpdateGenerator: (
      DamlLogEntryId,
      DamlLogEntry,
      ValueSwitch,
      Option[Timestamp],
  ) => LoggingContext => List[Update] =
    (_, _, _, _) => _ => List.empty

  private val singleUpdateGenerator: (
      DamlLogEntryId,
      DamlLogEntry,
      ValueSwitch,
      Option[Timestamp],
  ) => LoggingContext => List[Update] =
    (_, _, _, _) =>
      _ =>
        List(
          Update.PartyAddedToParticipant(
            Ref.Party.assertFromString("aParty"),
            "a party",
            Ref.ParticipantId.assertFromString("aParticipant"),
            Timestamp.now(),
            submissionId = None,
          )
        )

  private val twoUpdatesGenerator: (
      DamlLogEntryId,
      DamlLogEntry,
      ValueSwitch,
      Option[Timestamp],
  ) => LoggingContext => List[Update] =
    (entryId, entry, errorVersionSwitch, recordTime) =>
      loggingContext =>
        singleUpdateGenerator(
          entryId,
          entry,
          errorVersionSwitch,
          recordTime,
        )(loggingContext) ::: singleUpdateGenerator(
          entryId,
          entry,
          errorVersionSwitch,
          recordTime,
        )(loggingContext)

  private def aLogEntryId(index: Int): Raw.LogEntryId =
    Raw.LogEntryId(
      DamlLogEntryId.newBuilder
        .setEntryId(ByteString.copyFrom(s"id-$index".getBytes))
        .build
    )

  private def readerStreamingFrom(offset: Option[Offset], items: LedgerRecord*): LedgerReader = {
    val reader = mock[LedgerReader]
    val stream = Source.fromIterator(() => items.iterator)
    when(reader.events(offset)).thenReturn(stream)
    reader
  }

  private def createInstance(
      reader: LedgerReader,
      logEntryToUpdate: (
          DamlLogEntryId,
          DamlLogEntry,
          ValueSwitch,
          Option[Timestamp],
      ) => LoggingContext => List[Update] = singleUpdateGenerator,
      failOnUnexpectedEvent: Boolean = true,
  ): KeyValueParticipantStateReader =
    new KeyValueParticipantStateReader(
      reader,
      new Metrics(new MetricRegistry),
      enableSelfServiceErrorCodes = true,
      logEntryToUpdate,
      () => None,
      failOnUnexpectedEvent,
    )

  private def aStateValue =
    DamlStateValue.newBuilder
      .setParty(
        DamlPartyAllocation.newBuilder
          .setParticipantId("aParticipantId")
          .setDisplayName("participant")
      )
      .build
}
