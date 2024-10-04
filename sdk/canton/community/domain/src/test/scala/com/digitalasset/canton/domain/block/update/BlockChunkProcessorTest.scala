// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.metrics.SequencerTestMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.domain.sequencing.sequencer.{
  SubmissionOutcome,
  SubmissionRequestOutcome,
}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable}
import com.digitalasset.canton.sequencing.protocol.{MessageId, SubmissionRequest}
import com.digitalasset.canton.topology.DefaultTestIdentities.{domainId, sequencerId}
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
      "processing a tick chunk and the last validated event was addressed to the sequencer" in {
        val tickSequencingTimestamp = aTimestamp.immediateSuccessor
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndDomain(
            sequencerId,
            domainId,
            tickSequencingTimestamp,
          )
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]

        val blockChunkProcessor =
          new BlockChunkProcessor(
            domainId,
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
            )
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

  "create an empty update" when {
    "processing a tick chunk but the last validated event was addressed to the sequencer" in {
      val tickSequencingTimestamp = aTimestamp.immediateSuccessor
      val syncCryptoApiFake =
        TestingIdentityFactory(loggerFactory).forOwnerAndDomain(
          sequencerId,
          domainId,
          tickSequencingTimestamp,
        )
      val rateLimitManagerMock = mock[SequencerRateLimitManager]
      val memberValidatorMock = mock[SequencerMemberValidator]

      val blockChunkProcessor =
        new BlockChunkProcessor(
          domainId,
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
            latestSequencerEventTimestamp = Some(aTimestamp),
            inFlightAggregations = Map.empty,
          )
        )
        .map { case (state, update) =>
          state.lastChunkTs shouldBe aTimestamp
          state.latestSequencerEventTimestamp shouldBe Some(aTimestamp)
          update.submissionsOutcomes shouldBe empty
        }
        .failOnShutdown
    }
  }
}
