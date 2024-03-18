// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer as CantonSequencer
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencing.protocol.RecipientsTest.{p11, p12, p13, p14, p15}
import com.digitalasset.canton.sequencing.protocol.{Recipients, SubmissionRequest}
import com.digitalasset.canton.topology.{MediatorId, TestingIdentityFactoryX, TestingTopologyX}
import org.apache.pekko.stream.Materializer

import java.time.Duration

class DatabaseSequencerSnapshottingTest extends SequencerApiTest {

  def createSequencer(
      crypto: DomainSyncCryptoClient
  )(implicit materializer: Materializer): CantonSequencer =
    createSequencerWithSnapshot(crypto, None)

  def createSequencerWithSnapshot(
      crypto: DomainSyncCryptoClient,
      initialSnapshot: Option[SequencerSnapshot],
  )(implicit materializer: Materializer): CantonSequencer = {
    if (clock == null)
      clock = createClock()
    val crypto = TestingIdentityFactoryX(
      TestingTopologyX(),
      loggerFactory,
      DynamicDomainParameters.initialValues(clock, testedProtocolVersion),
    ).forOwnerAndDomain(owner = mediatorId, domainId)
    val metrics = SequencerMetrics.noop("database-sequencer-test")

    DatabaseSequencer.single(
      TestDatabaseSequencerConfig(),
      initialSnapshot,
      DefaultProcessingTimeouts.testing,
      new MemoryStorage(loggerFactory, timeouts),
      clock,
      domainId,
      sequencerId,
      testedProtocolVersion,
      crypto,
      metrics,
      loggerFactory,
    )(executorService, tracer, materializer)
  }

  final class SingleDbEnv extends Env {

    override protected val loggerFactory: NamedLoggerFactory =
      DatabaseSequencerSnapshottingTest.this.loggerFactory

    override lazy val topologyFactory =
      new TestingIdentityFactoryX(
        topology = TestingTopologyX().withSimpleParticipants(p11, p12, p13, p14, p15),
        loggerFactory,
        List.empty,
      )
  }

  override protected final type FixtureParam = SingleDbEnv
  override protected final def createEnv(): FixtureParam = new SingleDbEnv
  override protected def supportAggregation: Boolean = false

  "Database snapshotting" should {

    "allow a new separate database to be created" in { env =>
      import env.*

      val messageContent = "hello"
      val messageContent2 = "hello2"
      val sender: MediatorId = mediatorId
      val recipients = Recipients.cc(sender)

      val request: SubmissionRequest = createSendRequest(sender, messageContent, recipients)
      val request2: SubmissionRequest = createSendRequest(sender, messageContent2, recipients)

      for {
        _ <- valueOrFail(sequencer.registerMember(sender))("Register mediator")
        _ <- valueOrFail(sequencer.registerMember(sequencerId))("Register sequencer")

        _ <- valueOrFail(sequencer.sendAsync(request))("Sent async")
        messages <- readForMembers(List(sender), sequencer)
        _ = {
          val details = EventDetails(
            SequencerCounter(0),
            sender,
            Some(request.messageId),
            EnvelopeDetails(messageContent, recipients),
          )
          checkMessages(List(details), messages)
        }
        snapshot <- valueOrFail(sequencer.snapshot(CantonTimestamp.MaxValue))("get snapshot")

        // create a second separate sequencer from the snapshot
        secondSequencer = createSequencerWithSnapshot(
          topologyFactory.forOwnerAndDomain(owner = mediatorId, domainId),
          Some(snapshot),
        )

        // the snapshot from the second sequencer should look the same except that the lastTs will become the lower bound
        snapshot2 <- valueOrFail(secondSequencer.snapshot(CantonTimestamp.MaxValue))("get snapshot")
        _ = {
          snapshot2 shouldBe (snapshot.copy(status =
            snapshot.status.copy(lowerBound = snapshot.lastTs)
          )(snapshot.representativeProtocolVersion))
        }

        _ <- {
          // need to advance clock so that the new event doesn't get the same timestamp as the previous one,
          // which would then cause it to be ignored on the read path
          simClockOrFail(clock).advance(Duration.ofSeconds(1))
          valueOrFail(secondSequencer.sendAsync(request2))("Sent async")
        }

        messages2 <- readForMembers(
          List(sender),
          secondSequencer,
          firstSequencerCounter = SequencerCounter(1),
        )

      } yield {
        // the second sequencer (started from snapshot) is able to continue operating and create new messages
        val details2 = EventDetails(
          SequencerCounter(1),
          sender,
          Some(request2.messageId),
          EnvelopeDetails(messageContent2, recipients),
        )
        checkMessages(List(details2), messages2)

        secondSequencer.close()

        succeed
      }
    }
  }
}
