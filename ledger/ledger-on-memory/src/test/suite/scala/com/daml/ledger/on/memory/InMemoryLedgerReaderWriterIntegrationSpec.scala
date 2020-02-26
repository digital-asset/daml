// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.memory

import java.time.{Duration, Instant}

import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase
import com.daml.ledger.participant.state.kvutils.ParticipantStateIntegrationSpecBase._
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.{LedgerId, ParticipantId, Update}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.logging.LoggingContext
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.resources.ResourceOwner
import org.scalatest.Matchers._

class InMemoryLedgerReaderWriterIntegrationSpec
    extends ParticipantStateIntegrationSpecBase("In-memory ledger/participant") {

  override val isPersistent: Boolean = false

  override def participantStateFactory(
      ledgerId: Option[LedgerId],
      participantId: ParticipantId,
      testId: String,
  )(implicit logCtx: LoggingContext): ResourceOwner[ParticipantState] =
    InMemoryLedgerReaderWriter
      .singleParticipantOwner(ledgerId, participantId)
      .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))

  "In-memory ledger/participant" should {
    "emit heartbeats if a source is provided" in newLoggingContext { implicit logCtx =>
      val start = Instant.EPOCH
      val heartbeats =
        Source
          .fromIterator(() => Iterator.iterate(start)(_.plusSeconds(1)))
          .take(10) // ensure this doesn't keep running forever, past the length of the test
      InMemoryLedgerReaderWriter
        .singleParticipantOwner(
          Some(newLedgerId()),
          participantId,
          heartbeatMechanism = ResourceOwner.successful(heartbeats),
        )
        .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))
        .use { ps =>
          for {
            updates <- ps
              .stateUpdates(beginAfter = None)
              .idleTimeout(IdleTimeout)
              .take(3)
              .runWith(Sink.seq)
          } yield {
            updates.map(_._2) should be(Seq(
              Update.Heartbeat(Timestamp.Epoch),
              Update.Heartbeat(Timestamp.Epoch.add(Duration.ofSeconds(1))),
              Update.Heartbeat(Timestamp.Epoch.add(Duration.ofSeconds(2))),
            ))
          }
        }
    }
  }
}
