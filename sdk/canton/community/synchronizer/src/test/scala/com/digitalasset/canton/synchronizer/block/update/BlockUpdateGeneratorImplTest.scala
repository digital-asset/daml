// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfSynchronizer, Recipients}
import com.digitalasset.canton.synchronizer.HasTopologyTransactionTestFactory
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGenerator.{
  EndOfBlock,
  NextChunk,
  TopologyTickChunk,
}
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGeneratorImpl
import com.digitalasset.canton.synchronizer.block.{BlockEvents, LedgerBlockEvent, RawLedgerBlock}
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.topology.DefaultTestIdentities.{sequencerId, synchronizerId}
import com.digitalasset.canton.topology.TestingIdentityFactory
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, HasExecutorService}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Instant

class BlockUpdateGeneratorImplTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasExecutorService
    with HasTopologyTransactionTestFactory {

  implicit val closeContext: CloseContext = CloseContext(
    FlagCloseable.withCloseContext(logger, ProcessingTimeout())
  )

  private val aTimestamp =
    CantonTimestamp.assertFromInstant(Instant.parse("2024-03-08T12:00:00.000Z"))

  "BlockUpdateGeneratorImpl.extractBlockEvents" should {
    "append a topology tick event only" when {
      "the block requires one" in {
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
            sequencerId,
            synchronizerId,
            aTimestamp,
          )

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            synchronizerId,
            testedProtocolVersion,
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            OrderingTimeFixMode.ValidateOnly,
            SequencerTestMetrics,
            loggerFactory,
            memberValidatorMock,
          )

        blockUpdateGenerator.extractBlockEvents(
          RawLedgerBlock(
            1L,
            Seq.empty,
            tickTopologyAtMicrosFromEpoch = Some(aTimestamp.toMicros),
          )
        ) shouldBe BlockEvents(1L, Seq.empty, Some(aTimestamp))

        blockUpdateGenerator.extractBlockEvents(
          RawLedgerBlock(1L, Seq.empty, None)
        ) shouldBe BlockEvents(1L, Seq.empty, None)
      }
    }
  }

  "BlockUpdateGeneratorImpl.chunkBlock" should {
    "append a tick chunk" when {
      "the block requires one" in {
        val sequencerAddressedEventTimestamp = aTimestamp.immediateSuccessor
        val topologyTickEventTimestamp = sequencerAddressedEventTimestamp.immediateSuccessor
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
            sequencerId,
            synchronizerId,
            topologyTickEventTimestamp,
          )

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            synchronizerId,
            testedProtocolVersion,
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            OrderingTimeFixMode.ValidateOnly,
            SequencerTestMetrics,
            loggerFactory,
            memberValidatorMock,
          )

        for {
          signedSubmissionRequest <- FutureUnlessShutdown.outcomeF(
            sequencerSignedAndSenderSignedSubmissionRequest(
              topologyTransactionFactory.participant1,
              Recipients.cc(AllMembersOfSynchronizer),
            )
          )
          chunks = blockUpdateGenerator.chunkBlock(
            BlockEvents(
              height = 1L,
              Seq(
                Traced(
                  LedgerBlockEvent.Send(sequencerAddressedEventTimestamp, signedSubmissionRequest)
                )(TraceContext.empty)
              ),
              tickTopologyAtLeastAt = Some(topologyTickEventTimestamp),
            )
          )
        } yield {
          chunks match {
            case Seq(
                  NextChunk(1L, 0, chunkEvents),
                  TopologyTickChunk(1L, `topologyTickEventTimestamp`),
                  EndOfBlock(1L),
                ) =>
              chunkEvents.forgetNE should matchPattern {
                case Seq(
                      Traced(
                        LedgerBlockEvent.Send(`sequencerAddressedEventTimestamp`, _, _)
                      )
                    ) =>
              }
            case _ => fail(s"Unexpected chunks $chunks")
          }
        }
      }.failOnShutdown
    }
  }
}
