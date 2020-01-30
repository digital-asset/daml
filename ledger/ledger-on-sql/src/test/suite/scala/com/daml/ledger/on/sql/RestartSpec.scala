// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.on.sql

import java.nio.file.Files
import java.util.UUID

import akka.stream.scaladsl.Sink
import com.daml.ledger.participant.state.kvutils.api.KeyValueParticipantState
import com.daml.ledger.participant.state.v1.ParticipantId
import com.daml.ledger.participant.state.v1.Update.PartyAddedToParticipant
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{LedgerString, Party}
import com.digitalasset.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.digitalasset.logging.LoggingContext.newLoggingContext
import com.digitalasset.resources.Resource
import org.scalatest.{AsyncWordSpec, BeforeAndAfterEach, Matchers}

import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.DurationInt
import scala.util.Random

class RestartSpec
    extends AsyncWordSpec
    with Matchers
    with BeforeAndAfterEach
    with AkkaBeforeAndAfterAll {

  private val root = Files.createTempDirectory(getClass.getSimpleName)

  private def start(id: String): Resource[KeyValueParticipantState] = {
    newLoggingContext { implicit logCtx =>
      val ledgerId: LedgerString = LedgerString.assertFromString(s"ledger-${UUID.randomUUID()}")
      val participantId: ParticipantId = LedgerString.assertFromString("participant")
      val jdbcUrl =
        s"jdbc:sqlite:file:$root/$id.sqlite"
      SqlLedgerReaderWriter
        .owner(ledgerId, participantId, jdbcUrl)
        .map(readerWriter => new KeyValueParticipantState(readerWriter, readerWriter))
        .acquire()
    }
  }

  "an SQL ledger reader-writer" should {
    "resume where it left off on restart" in {
      val id = Random.nextInt().toString
      for {
        _ <- start(id).use { participant =>
          for {
            _ <- participant
              .allocateParty(None, Some(Party.assertFromString("party-1")), randomLedgerString())
              .toScala
          } yield ()
        }
        updates <- start(id).use { participant =>
          for {
            _ <- participant
              .allocateParty(None, Some(Party.assertFromString("party-2")), randomLedgerString())
              .toScala
            updates <- participant
              .stateUpdates(beginAfter = None)
              .take(2)
              .completionTimeout(10.seconds)
              .runWith(Sink.seq)
          } yield updates.map(_._2)
        }
      } yield {
        all(updates) should be(a[PartyAddedToParticipant])
        val displayNames = updates.map(_.asInstanceOf[PartyAddedToParticipant].displayName)
        displayNames should be(Seq("party-1", "party-2"))
      }
    }
  }

  private def randomLedgerString(): LedgerString =
    Ref.LedgerString.assertFromString(UUID.randomUUID().toString)
}
