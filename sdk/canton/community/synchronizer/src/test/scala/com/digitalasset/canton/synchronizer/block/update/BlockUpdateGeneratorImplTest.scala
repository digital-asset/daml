// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.sequencing.protocol.{
  AllMembersOfSynchronizer,
  Recipients,
  SequencersOfSynchronizer,
}
import com.digitalasset.canton.synchronizer.HasTopologyTransactionTestFactory
import com.digitalasset.canton.synchronizer.block.BlockEvents.TickTopology
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.{Acknowledgment, Send}
import com.digitalasset.canton.synchronizer.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.synchronizer.block.update.BlockUpdateGenerator.{
  EndOfBlock,
  MaybeTopologyTickChunk,
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

import java.time.{Duration, Instant}

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
            useTimeProofsToObserveEffectiveTime = true,
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

        val blockBaseSequencingTime1 = sequencingTimeLowerBoundExclusive.minusSeconds(5)
        val blockBaseSequencingTime1MicrosFromEpoch = blockBaseSequencingTime1.toMicros
        blockUpdateGenerator
          .extractBlockEvents(
            Traced(
              RawLedgerBlock(
                1L,
                blockBaseSequencingTime1MicrosFromEpoch,
                Seq(
                  RawBlockEvent.Send(
                    signedSubmissionRequest.toByteString,
                    blockBaseSequencingTime1MicrosFromEpoch,
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
            )(TraceContext.empty)
          )
          .value shouldBe BlockEvents(1L, blockBaseSequencingTime1, Seq.empty, None)

        val blockBaseSequencingTime2 = sequencingTimeLowerBoundExclusive.immediatePredecessor
        val blockBaseSequencingTime2MicrosFromEpoch = blockBaseSequencingTime2.toMicros
        blockUpdateGenerator
          .extractBlockEvents(
            Traced(
              RawLedgerBlock(
                1L,
                blockBaseSequencingTime2MicrosFromEpoch,
                Seq(
                  RawBlockEvent
                    .Acknowledgment(
                      acknowledgeRequest.toByteString,
                      blockBaseSequencingTime2MicrosFromEpoch,
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
            )
          )
          .value shouldBe BlockEvents(
          1L,
          blockBaseSequencingTime2,
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
            useTimeProofsToObserveEffectiveTime = true,
            SequencerTestMetrics,
            loggerFactory,
            memberValidatorMock,
          )

        blockUpdateGenerator
          .extractBlockEvents(
            Traced(
              RawLedgerBlock(
                1L,
                aTimestamp.toMicros,
                Seq.empty,
                tickTopologyAtMicrosFromEpoch = Some(aTimestamp.toMicros -> false),
              )
            )
          )
          .value shouldBe BlockEvents(
          1L,
          aTimestamp,
          Seq.empty,
          Some(TickTopology(aTimestamp, Right(SequencersOfSynchronizer))),
        )

        blockUpdateGenerator
          .extractBlockEvents(
            Traced(RawLedgerBlock(1L, aTimestamp.toMicros, Seq.empty, None))
          )
          .value shouldBe BlockEvents(1L, aTimestamp, Seq.empty, None)
      }
    }
  }

  "BlockUpdateGeneratorImpl.chunkBlock" should {
    "append a tick chunk" when {
      "the block requires one" in {
        val sequencerAddressedEventTimestamp = aTimestamp.immediateSuccessor
        val topologyTickEventTimestamp = sequencerAddressedEventTimestamp.immediateSuccessor

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            testedProtocolVersion,
            synchronizerSyncCryptoApi =
              TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
                sequencerId,
                physicalSynchronizerId,
                topologyTickEventTimestamp,
              ),
            sequencerId,
            mock[SequencerRateLimitManager],
            OrderingTimeFixMode.ValidateOnly,
            sequencingTimeLowerBoundExclusive =
              SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive,
            useTimeProofsToObserveEffectiveTime = true,
            SequencerTestMetrics,
            loggerFactory,
            mock[SequencerMemberValidator],
          )

        for {
          signedSubmissionRequest <- FutureUnlessShutdown.outcomeF(
            senderSignedSubmissionRequest(
              topologyTransactionFactory.participant1,
              Recipients.cc(AllMembersOfSynchronizer),
            )
          )
          events = Seq(
            Traced(
              LedgerBlockEvent.Send(
                sequencerAddressedEventTimestamp,
                signedSubmissionRequest,
                sequencerId,
              )
            )(TraceContext.empty)
          )

          chunkBlock = (addressee: Either[
            AllMembersOfSynchronizer.type,
            SequencersOfSynchronizer.type,
          ]) =>
            blockUpdateGenerator.chunkBlock(
              BlockEvents(
                height = 1L,
                aTimestamp,
                events,
                tickTopology = Some(TickTopology(topologyTickEventTimestamp, addressee)),
              )
            )

          chunks1 = chunkBlock(Right(SequencersOfSynchronizer))
          chunks2 = chunkBlock(Left(AllMembersOfSynchronizer))
        } yield {
          Table(
            ("chunks, expected tick recipient"),
            chunks1 -> Right(SequencersOfSynchronizer),
            chunks2 -> Left(AllMembersOfSynchronizer),
          ).forEvery { case (chunks, expectedTickRecipient) =>
            chunks should matchPattern {
              case Seq(
                    NextChunk(
                      1L,
                      0,
                      Seq(
                        Traced(
                          LedgerBlockEvent.Send(`sequencerAddressedEventTimestamp`, _, _, _)
                        )
                      ),
                    ),
                    TopologyTickChunk(
                      1L,
                      `topologyTickEventTimestamp`,
                      addressee,
                    ),
                    EndOfBlock(1L),
                  ) if addressee == expectedTickRecipient =>
            }
          }
        }
      }.failOnShutdown
    }

    "append a maybe tick chunk" when {
      "getting to the end of the block and useTimeProofsToObserveEffectiveTime is false" in {
        val sequencerAddressedEventTimestamp = aTimestamp.immediateSuccessor
        val topologyTickEventTimestamp = sequencerAddressedEventTimestamp.immediateSuccessor

        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            testedProtocolVersion,
            synchronizerSyncCryptoApi =
              TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
                sequencerId,
                physicalSynchronizerId,
                topologyTickEventTimestamp,
              ),
            sequencerId,
            mock[SequencerRateLimitManager],
            OrderingTimeFixMode.ValidateOnly,
            sequencingTimeLowerBoundExclusive =
              SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive,
            useTimeProofsToObserveEffectiveTime = false,
            SequencerTestMetrics,
            loggerFactory,
            mock[SequencerMemberValidator],
          )

        for {
          signedSubmissionRequest <- FutureUnlessShutdown.outcomeF(
            senderSignedSubmissionRequest(
              topologyTransactionFactory.participant1,
              Recipients.cc(AllMembersOfSynchronizer),
            )
          )
          events = Seq(
            Traced(
              LedgerBlockEvent.Send(
                sequencerAddressedEventTimestamp,
                signedSubmissionRequest,
                sequencerId,
              )
            )(TraceContext.empty)
          )

          chunks1 = blockUpdateGenerator.chunkBlock(
            BlockEvents(
              height = 1L,
              aTimestamp,
              events,
            )
          )
        } yield {
          chunks1 should matchPattern {
            case Seq(
                  NextChunk(
                    1L,
                    0,
                    Seq(
                      Traced(
                        LedgerBlockEvent.Send(`sequencerAddressedEventTimestamp`, _, _, _)
                      )
                    ),
                  ),
                  MaybeTopologyTickChunk(
                    1L
                  ),
                  EndOfBlock(1L),
                ) =>
          }
        }
      }.failOnShutdown
    }

    "process a maybe tick chunk" when {
      "receiving the MaybeTopologyTickChunk at the end of the block" in {
        val epsilon = defaultStaticSynchronizerParameters.topologyChangeDelay.duration
        val blockUpdateGenerator =
          new BlockUpdateGeneratorImpl(
            testedProtocolVersion,
            synchronizerSyncCryptoApi =
              TestingIdentityFactory(loggerFactory).forOwnerAndSynchronizer(
                sequencerId,
                physicalSynchronizerId,
                aTimestamp,
              ),
            sequencerId,
            mock[SequencerRateLimitManager],
            OrderingTimeFixMode.ValidateOnly,
            sequencingTimeLowerBoundExclusive =
              SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive,
            useTimeProofsToObserveEffectiveTime = false,
            SequencerTestMetrics,
            loggerFactory,
            mock[SequencerMemberValidator],
          )

        val state = BlockUpdateGeneratorImpl.State(
          lastBlockTs = aTimestamp,
          lastChunkTs = aTimestamp,
          latestSequencerEventTimestamp = None,
          inFlightAggregations = Map.empty,
          pendingTopologyTimestamps = Vector.empty,
        )

        val t1 = aTimestamp.minus(epsilon)
        val t0 = t1.minus(Duration.ofMillis(100))
        val t2 = t1.immediateSuccessor
        val t3 = aTimestamp.immediateSuccessor

        for {
          noOpResult <- blockUpdateGenerator.processBlockChunk(state, MaybeTopologyTickChunk(1L))

          result2 <- blockUpdateGenerator.processBlockChunk(
            state.copy(pendingTopologyTimestamps = Vector(t0, t1, t2)),
            MaybeTopologyTickChunk(1L),
          )

          result3 <- blockUpdateGenerator.processBlockChunk(
            state.copy(pendingTopologyTimestamps = Vector(t0, t1, t2, t3)),
            MaybeTopologyTickChunk(1L),
          )
        } yield {
          // no pending topology transaction timestamps, so nothing to do
          noOpResult shouldBe (state, ChunkUpdate.noop)

          // in this case t1 is considered the highest newly active topology timestamp, so a tick is created
          // t2 still remains pending
          result2._1 shouldBe state.copy(
            lastChunkTs = aTimestamp.immediateSuccessor,
            latestSequencerEventTimestamp = Some(aTimestamp.immediateSuccessor),
            pendingTopologyTimestamps = Vector(t2),
          )

          // in this case the topology transaction with timestamp t3 will act as a tick for the newly active ones
          // with timestamps t0 and t1, so nothing happens (other than updating the pending topology timestamps).
          result3 shouldBe (state.copy(
            lastChunkTs = aTimestamp,
            latestSequencerEventTimestamp = None,
            pendingTopologyTimestamps = Vector(t2, t3),
          ), ChunkUpdate.noop)
        }
      }.failOnShutdown
    }
  }
}
