// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  MessageId,
  SequencersOfSynchronizer,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.block.update.{
  BlockChunkProcessor,
  BlockUpdateGeneratorImpl,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.SubmissionOutcome
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.topology.DefaultTestIdentities.{
  mediatorId,
  physicalSynchronizerId,
  sequencerId,
}
import com.digitalasset.canton.topology.TestingIdentityFactory
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class BlockChunkProcessorTest extends AsyncWordSpec with BaseTest {

  implicit val closeContext: CloseContext = CloseContext(
    FlagCloseable.withCloseContext(logger, ProcessingTimeout())
  )

  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  private val aHeight = 42L
  private val aMessageId = MessageId.tryCreate(s"topology-tick-$aHeight")

  "BlockChunkProcessor.processBlockChunk" should {

    "create the correct chunked update for the tick" when {
      "processing a tick chunk" in {
        val tickSequencingTimestamp = aTimestamp.immediateSuccessor
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
            sequencerId,
            physicalSynchronizerId,
            tickSequencingTimestamp,
          )
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]

        val blockChunkProcessor =
          new BlockChunkProcessor(
            testedProtocolVersion,
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            OrderingTimeFixMode.ValidateOnly,
            loggerFactory,
            SequencerTestMetrics,
            memberValidatorMock,
          )

        def emitTick(
            recipient: Either[AllMembersOfSynchronizer.type, SequencersOfSynchronizer.type]
        ) =
          blockChunkProcessor
            .emitTick(
              state = BlockUpdateGeneratorImpl.State(
                lastBlockTs = aTimestamp,
                lastChunkTs = aTimestamp,
                latestSequencerEventTimestamp = None,
                inFlightAggregations = Map.empty,
              ),
              height = aHeight,
              tickAtLeastAt = tickSequencingTimestamp,
              groupRecipient = recipient,
            )

        for {
          (state1, update1) <- emitTick(Right(SequencersOfSynchronizer))
          (state2, update2) <- emitTick(Left(AllMembersOfSynchronizer))
        } yield {
          Table(
            ("state", "update", "expected tick recipients"),
            (state1, update1, Set(sequencerId)),
            (state2, update2, Set(sequencerId, mediatorId)),
          ).forEvery { case (state, update, expectedTickRecipients) =>
            state.lastChunkTs shouldBe tickSequencingTimestamp
            state.latestSequencerEventTimestamp shouldBe Some(tickSequencingTimestamp)
            update.submissionsOutcomes should matchPattern {
              case Seq(
                    SubmissionOutcome.Deliver(
                      SubmissionRequest(
                        `sequencerId`,
                        `aMessageId`,
                        _,
                        `tickSequencingTimestamp`,
                        None,
                        None,
                        None,
                      ),
                      `tickSequencingTimestamp`,
                      deliverToMembers,
                      batch,
                      _,
                      _,
                      None,
                    )
                  )
                  if deliverToMembers == expectedTickRecipients &&
                    batch.envelopes.isEmpty =>
            }
          }
        }
      }.failOnShutdown
    }
  }
}
