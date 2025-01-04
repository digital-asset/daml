// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.sequencing.protocol.{MessageId, SubmissionRequest}
import com.digitalasset.canton.synchronizer.block.update.{
  BlockChunkProcessor,
  BlockUpdateGeneratorImpl,
}
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencing.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.synchronizer.sequencing.sequencer.{
  SubmissionOutcome,
  SubmissionRequestOutcome,
}
import com.digitalasset.canton.topology.DefaultTestIdentities.{sequencerId, synchronizerId}
import com.digitalasset.canton.topology.TestingIdentityFactory
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class BlockChunkProcessorTest extends AsyncWordSpec with BaseTest {

  implicit val closeContext: CloseContext = CloseContext(
    FlagCloseable.withCloseContext(logger, ProcessingTimeout())
  )

  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))
  private val aMessageId = MessageId.randomMessageId()

  "BlockChunkProcessor.processBlockChunk" should {

    "create the correct chunked update for the tick" when {
      "processing a tick chunk" in {
        val tickSequencingTimestamp = aTimestamp.immediateSuccessor
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndDomain(
            sequencerId,
            synchronizerId,
            tickSequencingTimestamp,
          )
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]

        val blockChunkProcessor =
          new BlockChunkProcessor(
            synchronizerId,
            testedProtocolVersion,
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            OrderingTimeFixMode.ValidateOnly,
            loggerFactory,
            SequencerTestMetrics,
            memberValidatorMock,
            () => aMessageId,
          )

        blockChunkProcessor
          .emitTick(
            state = BlockUpdateGeneratorImpl.State(
              lastBlockTs = aTimestamp,
              lastChunkTs = aTimestamp,
              latestSequencerEventTimestamp = None,
              inFlightAggregations = Map.empty,
            ),
            height = 0,
            tickAtLeastAt = tickSequencingTimestamp,
          )
          .map { case (state, update) =>
            state.lastChunkTs shouldBe tickSequencingTimestamp
            state.latestSequencerEventTimestamp shouldBe Some(tickSequencingTimestamp)
            update.submissionsOutcomes should matchPattern {
              case Seq(
                    SubmissionRequestOutcome(
                      _,
                      None,
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
                      ),
                    )
                  )
                  if deliverToMembers == Set(sequencerId) &&
                    batch.envelopes.isEmpty =>
            }
          }
          .failOnShutdown
      }
    }
  }
}
