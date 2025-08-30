// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.sequencing.protocol.{AllMembersOfSynchronizer, Recipients}
import com.digitalasset.canton.synchronizer.HasTopologyTransactionTestFactory
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.{Acknowledgment, Send}
import com.digitalasset.canton.synchronizer.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGenerator.{
  EndOfBlock,
  NextChunk,
  TopologyTickChunk,
}
import com.digitalasset.canton.synchronizer.block.{BlockEvents, LedgerBlockEvent, RawLedgerBlock}
import com.digitalasset.canton.synchronizer.metrics.SequencerTestMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.BlockSequencerFactory.OrderingTimeFixMode
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerNodeParameterConfig
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.synchronizer.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.topology.DefaultTestIdentities.{physicalSynchronizerId, sequencerId}
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
    "filter out events" when {
      "the sequencing time is before or at the minimum sequencing time" in {
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
            sequencerId,
            physicalSynchronizerId,
            aTimestamp,
          )
        val sequencingTimeLowerBoundExclusive = CantonTimestamp.Epoch.plusSeconds(10)

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            testedProtocolVersion,
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            OrderingTimeFixMode.ValidateOnly,
            sequencingTimeLowerBoundExclusive = Some(sequencingTimeLowerBoundExclusive),
            SequencerTestMetrics,
            loggerFactory,
            memberValidatorMock,
          )

        val signedSubmissionRequest = senderSignedSubmissionRequest(
          topologyTransactionFactory.participant1,
          Recipients.cc(AllMembersOfSynchronizer),
        ).futureValue
        val acknowledgeRequest =
          senderSignedAcknowledgeRequest(topologyTransactionFactory.participant1).futureValue

        blockUpdateGenerator.extractBlockEvents(
          RawLedgerBlock(
            1L,
            Seq(
              RawBlockEvent.Send(
                signedSubmissionRequest.toByteString,
                sequencingTimeLowerBoundExclusive.minusSeconds(5).toMicros,
                sequencerId.toProtoPrimitive,
              ),
              RawBlockEvent
                .Acknowledgment(
                  acknowledgeRequest.toByteString,
                  sequencingTimeLowerBoundExclusive.minusSeconds(4).toMicros,
                ),
            ).map(Traced(_)(TraceContext.empty)),
            tickTopologyAtMicrosFromEpoch = None,
          )
        ) shouldBe BlockEvents(1L, Seq.empty, None)

        blockUpdateGenerator.extractBlockEvents(
          RawLedgerBlock(
            1L,
            Seq(
              RawBlockEvent
                .Acknowledgment(
                  acknowledgeRequest.toByteString,
                  sequencingTimeLowerBoundExclusive.immediatePredecessor.toMicros,
                ),
              RawBlockEvent
                .Send(
                  signedSubmissionRequest.toByteString,
                  sequencingTimeLowerBoundExclusive.immediateSuccessor.toMicros,
                  sequencerId.toProtoPrimitive,
                ),
              RawBlockEvent
                .Acknowledgment(
                  acknowledgeRequest.toByteString,
                  sequencingTimeLowerBoundExclusive.immediateSuccessor.immediateSuccessor.toMicros,
                ),
            ).map(Traced(_)(TraceContext.empty)),
            None,
          )
        ) shouldBe BlockEvents(
          1L,
          Seq(
            Send(
              sequencingTimeLowerBoundExclusive.immediateSuccessor,
              signedSubmissionRequest,
              sequencerId,
              signedSubmissionRequest.toByteString.size(),
            ),
            Acknowledgment(
              sequencingTimeLowerBoundExclusive.immediateSuccessor.immediateSuccessor,
              acknowledgeRequest,
            ),
          ).map(
            Traced(_)(TraceContext.empty)
          ),
          None,
        )

      }
    }

    "append a topology tick event only" when {
      "the block requires one" in {
        val rateLimitManagerMock = mock[SequencerRateLimitManager]
        val memberValidatorMock = mock[SequencerMemberValidator]
        val syncCryptoApiFake =
          TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
            sequencerId,
            physicalSynchronizerId,
            aTimestamp,
          )

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            testedProtocolVersion,
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            OrderingTimeFixMode.ValidateOnly,
            sequencingTimeLowerBoundExclusive =
              SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive,
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
            physicalSynchronizerId,
            topologyTickEventTimestamp,
          )

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            testedProtocolVersion,
            syncCryptoApiFake,
            sequencerId,
            rateLimitManagerMock,
            OrderingTimeFixMode.ValidateOnly,
            sequencingTimeLowerBoundExclusive =
              SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive,
            SequencerTestMetrics,
            loggerFactory,
            memberValidatorMock,
          )

        for {
          signedSubmissionRequest <- FutureUnlessShutdown.outcomeF(
            senderSignedSubmissionRequest(
              topologyTransactionFactory.participant1,
              Recipients.cc(AllMembersOfSynchronizer),
            )
          )
          chunks = blockUpdateGenerator.chunkBlock(
            BlockEvents(
              height = 1L,
              Seq(
                Traced(
                  LedgerBlockEvent.Send(
                    sequencerAddressedEventTimestamp,
                    signedSubmissionRequest,
                    sequencerId,
                  )
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
                        LedgerBlockEvent.Send(`sequencerAddressedEventTimestamp`, _, _, _)
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
