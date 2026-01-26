// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.config.{BatchingConfig, ProcessingTimeout}
import com.digitalasset.canton.data.{CantonTimestamp, SequencingTimeBound}
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
import com.digitalasset.canton.version.ProtocolVersion
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
            SequencingTimeBound(Some(sequencingTimeLowerBoundExclusive)),
            producePostOrderingTopologyTicks = false,
            SequencerTestMetrics,
            BatchingConfig(),
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
            SequencingTimeBound(
              SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive
            ),
            producePostOrderingTopologyTicks = false,
            SequencerTestMetrics,
            BatchingConfig(),
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
            SequencingTimeBound(
              SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive
            ),
            producePostOrderingTopologyTicks = false,
            SequencerTestMetrics,
            BatchingConfig(),
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
                    MaybeTopologyTickChunk(
                      1L,
                      `aTimestamp`,
                      Some(TickTopology(`topologyTickEventTimestamp`, addressee)),
                    ),
                    EndOfBlock(1L),
                  ) if addressee == expectedTickRecipient =>
            }
          }
        }
      }.failOnShutdown
    }
    if (testedProtocolVersion >= ProtocolVersion.v35) {
      "append a maybe tick chunk" when {
        "getting to the end of the block and producePostOrderingTopologyTicks is true" in {
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
              SequencingTimeBound(
                SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive
              ),
              producePostOrderingTopologyTicks = true,
              SequencerTestMetrics,
              BatchingConfig(),
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
                      1L,
                      `aTimestamp`,
                      None,
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
              SequencingTimeBound(
                SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive
              ),
              producePostOrderingTopologyTicks = true,
              SequencerTestMetrics,
              BatchingConfig(),
              loggerFactory,
              mock[SequencerMemberValidator],
            )

          val state = BlockUpdateGeneratorImpl.State(
            lastBlockTs = aTimestamp.immediatePredecessor,
            lastChunkTs = aTimestamp,
            latestSequencerEventTimestamp = None,
            inFlightAggregations = Map.empty,
            latestPendingTopologyTransactionTimestamp = None,
          )

          val t1 = aTimestamp.minus(epsilon)
          val t2 = t1.immediateSuccessor
          val t3 = aTimestamp.immediateSuccessor.immediateSuccessor

          for {
            noOpResult <- blockUpdateGenerator.processBlockChunk(
              state,
              MaybeTopologyTickChunk(1L, aTimestamp, None),
            )

            result2 <- blockUpdateGenerator.processBlockChunk(
              state.copy(latestPendingTopologyTransactionTimestamp = Some(t1)),
              MaybeTopologyTickChunk(1L, aTimestamp, None),
            )

            result3 <- blockUpdateGenerator.processBlockChunk(
              state.copy(latestPendingTopologyTransactionTimestamp = Some(t2)),
              MaybeTopologyTickChunk(1L, aTimestamp, None),
            )

            result4 <- blockUpdateGenerator.processBlockChunk(
              state.copy(latestPendingTopologyTransactionTimestamp = Some(t2)),
              MaybeTopologyTickChunk(1L, t3, None),
            )
          } yield {
            // no pending topology transaction timestamps, so nothing to do
            noOpResult shouldBe (state, ChunkUpdate.noop)

            // in this case t1 is considered the highest newly effective topology timestamp, so a tick is created
            result2._1 shouldBe state.copy(
              lastChunkTs = aTimestamp.immediateSuccessor,
              latestSequencerEventTimestamp = Some(aTimestamp.immediateSuccessor),
              latestPendingTopologyTransactionTimestamp = None,
            )
            result2._2 should matchPattern {
              // the tick is created
              case c: ChunkUpdate if c.submissionsOutcomes.sizeIs == 1 =>
            }

            // in this case t2 is not yet effective (by a microsecond), so no tick is created
            result3 shouldBe (state.copy(
              lastChunkTs = aTimestamp,
              latestSequencerEventTimestamp = None,
              latestPendingTopologyTransactionTimestamp = Some(t2),
            ), ChunkUpdate.noop)

            // in this case, the block is empty and the baseBlockSequencingTime was taken into account
            // to conclude that t2 is effective.
            result4._1 shouldBe state.copy(
              lastChunkTs = t3,
              latestSequencerEventTimestamp = Some(t3),
              latestPendingTopologyTransactionTimestamp = None,
            )
            result4._2 should matchPattern {
              // the tick is created
              case c: ChunkUpdate if c.submissionsOutcomes.sizeIs == 1 =>
            }
          }
        }.failOnShutdown
      }
    }
  }
}
