// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{
  RepairIndex,
  SequencerIndex,
  SynchronizerIndex,
}
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestData
import com.digitalasset.canton.participant.protocol.{
  MessageCleanReplayStartingPoint,
  MessageProcessingStartingPoint,
  ProcessingStartingPoints,
}
import com.digitalasset.canton.participant.store.memory.InMemoryRequestJournalStore
import com.digitalasset.canton.participant.sync.SyncEphemeralStateFactory
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{SequencedSerializedEvent, SequencerTestUtils}
import com.digitalasset.canton.store.SequencedEventStore.SequencedEventWithTraceContext
import com.digitalasset.canton.store.memory.InMemorySequencedEventStore
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  CloseableTest,
  FailOnShutdown,
  RepairCounter,
  RequestCounter,
  SequencerCounter,
}
import org.scalatest.wordspec.AsyncWordSpec

class SyncEphemeralStateFactoryTest
    extends AsyncWordSpec
    with BaseTest
    with CloseableTest
    with FailOnShutdown {

  private lazy val synchronizerId = SynchronizerId.tryFromString("synchronizer::da")

  private def dummyEvent(
      synchronizerId: SynchronizerId
  )(timestamp: CantonTimestamp): SequencedSerializedEvent =
    SequencedEventWithTraceContext(
      SignedContent(
        SequencerTestUtils.mockDeliver(timestamp, synchronizerId = synchronizerId),
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      )
    )(
      TraceContext.empty
    )

  "startingPoints" when {
    "there is no clean request" should {
      "return the default" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val ses = new InMemorySequencedEventStore(loggerFactory, timeouts)

        for {
          startingPoints <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            None,
          )
        } yield {
          startingPoints shouldBe ProcessingStartingPoints.tryCreate(
            MessageCleanReplayStartingPoint.default,
            MessageProcessingStartingPoint.default,
          )
        }
      }
    }

    "there is only the clean head request" should {
      "return the clean head" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val ses = new InMemorySequencedEventStore(loggerFactory, timeouts)
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts = CantonTimestamp.Epoch
        for {
          _ <- rjs.insert(RequestData.clean(rc, ts, ts.plusSeconds(1)))
          _ <- ses.reinitializeFromDbOrSetLowerBound(sc - 1L)
          _ <- ses.store(Seq(dummyEvent(synchronizerId)(ts)))
          withCleanSc <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              SynchronizerIndex.of(
                SequencerIndex(ts)
              )
            ),
          )
        } yield {
          val cleanReplay = MessageCleanReplayStartingPoint(rc, sc, ts.immediatePredecessor)
          val processing =
            MessageProcessingStartingPoint(rc + 1L, sc + 1L, ts, ts, RepairCounter.Genesis)
          withCleanSc shouldBe ProcessingStartingPoints.tryCreate(
            cleanReplay,
            processing,
          )
        }
      }
    }

    "there are several requests" should {
      "return the right result" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val ses = new InMemorySequencedEventStore(loggerFactory, timeouts)
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts0 = CantonTimestamp.ofEpochSecond(0)
        val ts1 = CantonTimestamp.ofEpochSecond(1)
        val ts2 = CantonTimestamp.ofEpochSecond(2)
        val ts3 = CantonTimestamp.ofEpochSecond(5)
        val ts3plus = CantonTimestamp.ofEpochSecond(6)
        val ts4 = CantonTimestamp.ofEpochSecond(7)
        val ts5 = CantonTimestamp.ofEpochSecond(8)
        val ts6 = CantonTimestamp.ofEpochSecond(9)
        for {
          _ <- rjs.insert(RequestData.clean(rc, ts0, ts0.plusSeconds(2)))
          _ <- rjs.insert(RequestData.clean(rc + 1L, ts1, ts1.plusSeconds(1)))
          _ <- rjs.insert(RequestData.clean(rc + 2L, ts2, ts2.plusSeconds(4)))
          _ <- ses.reinitializeFromDbOrSetLowerBound(sc - 1L)
          _ <- ses.store(
            Seq(
              dummyEvent(synchronizerId)(ts0),
              dummyEvent(synchronizerId)(ts1),
              dummyEvent(synchronizerId)(ts2),
              dummyEvent(synchronizerId)(ts3),
              dummyEvent(synchronizerId)(ts4),
              dummyEvent(synchronizerId)(ts5),
              dummyEvent(synchronizerId)(ts6),
            )
          )
          sp1 <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              SynchronizerIndex.of(
                SequencerIndex(ts0)
              )
            ),
          )
          sp2 <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              SynchronizerIndex.of(
                SequencerIndex(ts1)
              )
            ),
          )
          synchronizerIndex = Some(
            SynchronizerIndex(
              repairIndex = None,
              sequencerIndex = Some(
                SequencerIndex(ts3)
              ),
              recordTime = ts3,
            )
          )
          sp3 <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            synchronizerIndex,
          )
          sp3WithRecordTimeIncrease <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            synchronizerIndex.map(_.copy(recordTime = ts3plus)),
          )
          _ <- rjs.insert(RequestData.initial(rc + 4L, ts6))
          _ <- rjs.insert(RequestData.initial(rc + 3L, ts5))
          sp3a <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            synchronizerIndex,
          )
          sp3b <- SyncEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              SynchronizerIndex(
                repairIndex = None,
                sequencerIndex = Some(
                  SequencerIndex(ts4)
                ),
                recordTime = ts4,
              )
            ),
          )
        } yield {
          // The clean sequencer index is ahead of the clean request index
          sp1.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc, sc, ts0.immediatePredecessor)
          sp1.processing shouldBe MessageProcessingStartingPoint(
            rc + 1L,
            sc + 1L,
            ts0,
            ts0,
            RepairCounter.Genesis,
          )

          // start with request 0 because its commit time is after ts1
          sp2.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc, sc, ts0.immediatePredecessor)
          sp2.processing shouldBe MessageProcessingStartingPoint(
            rc + 2L,
            sc + 2L,
            ts1,
            ts1,
            RepairCounter.Genesis,
          )

          // replay the latest clean request because the clean sequencer index is before the commit time
          sp3.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 2L,
            sc + 2L,
            ts2.immediatePredecessor,
          )
          sp3.processing shouldBe MessageProcessingStartingPoint(
            rc + 3L,
            sc + 4L,
            ts3,
            ts3,
            RepairCounter.Genesis,
          )

          // processing starting points propagate the floating record-time from the SynchronizerIndex
          sp3WithRecordTimeIncrease.processing shouldBe MessageProcessingStartingPoint(
            rc + 3L,
            sc + 4L,
            ts3,
            ts3plus,
            RepairCounter.Genesis,
          )
          // increase in record-time also affects replay-calculation (for example successfully excludes repair)
          // in this case the record time increase results in not taking RC=2 into consideration and replay is
          // computed from the processing starting point as no other commit-time is after
          sp3WithRecordTimeIncrease.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 3L,
            sc + 4L,
            ts3,
          )

          // we still have to replay the latest clean request
          // because we can't be sure that all subsequent requests have already been inserted into the request journal
          sp3a.cleanReplay shouldBe MessageCleanReplayStartingPoint(
            rc + 2L,
            sc + 2L,
            ts2.immediatePredecessor,
          )
          sp3a.processing shouldBe MessageProcessingStartingPoint(
            rc + 3L,
            sc + 4L,
            ts3,
            ts3,
            RepairCounter.Genesis,
          )

          // we don't have to replay the latest clean request
          // if the next request is known to be after the commit time.
          // As the clean sequencer counter index is after the commit time,
          // we start with the next inflight validation request
          sp3b.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc + 3L, sc + 5L, ts4)
          sp3b.processing shouldBe MessageProcessingStartingPoint(
            rc + 3L,
            sc + 5L,
            ts4,
            ts4,
            RepairCounter.Genesis,
          )
        }
      }

      "the commit times are reversed" should {
        "reprocess the clean request" in {
          val rjs = new InMemoryRequestJournalStore(loggerFactory)
          val ses = new InMemorySequencedEventStore(loggerFactory, timeouts)
          val rc = RequestCounter(0)
          val sc = SequencerCounter(10)
          val ts0 = CantonTimestamp.ofEpochSecond(0)
          val ts1 = CantonTimestamp.ofEpochSecond(1)
          val ts2 = CantonTimestamp.ofEpochSecond(2)
          val ts3 = CantonTimestamp.ofEpochSecond(3)

          for {
            _ <- rjs.insert(RequestData.clean(rc, ts0, ts0.plusSeconds(5)))
            _ <- rjs.insert(RequestData.clean(rc + 1L, ts1, ts1.plusSeconds(3)))
            _ <- rjs.insert(RequestData.initial(rc + 2L, ts3))
            _ <- ses.reinitializeFromDbOrSetLowerBound(sc - 1)
            _ <- ses.store(
              Seq(
                dummyEvent(synchronizerId)(ts0),
                dummyEvent(synchronizerId)(ts1),
                dummyEvent(synchronizerId)(ts2),
                dummyEvent(synchronizerId)(ts3),
              )
            )
            sp0 <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                SynchronizerIndex.of(
                  SequencerIndex(ts0)
                )
              ),
            )
            sp2 <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                SynchronizerIndex.of(
                  SequencerIndex(ts1)
                )
              ),
            )
          } yield {
            // start with request 0 because request 1 hasn't yet been marked as clean and request 0 commits after request 1 starts
            sp0.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              rc,
              sc,
              ts0.immediatePredecessor,
            )
            sp0.processing shouldBe MessageProcessingStartingPoint(
              rc + 1L,
              sc + 1L,
              ts0,
              ts0,
              RepairCounter.Genesis,
            )
            // replay from request 0 because request 2 starts before request 0 commits
            sp2.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              rc,
              sc,
              ts0.immediatePredecessor,
            )
            sp2.processing shouldBe MessageProcessingStartingPoint(
              rc + 2L,
              sc + 2L,
              ts1,
              ts1,
              RepairCounter.Genesis,
            )
          }
        }
      }

      "when there is a repair request" should {
        "return the right result" in {
          val rjs = new InMemoryRequestJournalStore(loggerFactory)
          val ses = new InMemorySequencedEventStore(loggerFactory, timeouts)
          val repairCounter = RepairCounter.Genesis
          val sc = SequencerCounter(10)
          val ts0 = CantonTimestamp.ofEpochSecond(0)
          val ts1 = CantonTimestamp.ofEpochSecond(1)
          val ts2 = CantonTimestamp.ofEpochSecond(2)

          for {
            _ <- ses.reinitializeFromDbOrSetLowerBound(sc - 1)
            _ <- ses.store(
              Seq(
                dummyEvent(synchronizerId)(ts0),
                dummyEvent(synchronizerId)(ts1),
                dummyEvent(synchronizerId)(ts2),
              )
            )
            noRepair <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                SynchronizerIndex(
                  repairIndex = None,
                  sequencerIndex = Some(
                    SequencerIndex(ts0)
                  ),
                  recordTime = ts0,
                )
              ),
            )
            repairAndSequencedEvent <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                SynchronizerIndex(
                  repairIndex = Some(
                    RepairIndex(
                      timestamp = ts1,
                      counter = repairCounter,
                    )
                  ),
                  sequencerIndex = Some(
                    SequencerIndex(ts1)
                  ),
                  recordTime = ts1,
                )
              ),
            )
            loneRepair <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                SynchronizerIndex(
                  repairIndex = Some(
                    RepairIndex(
                      timestamp = ts1,
                      counter = repairCounter + 1L,
                    )
                  ),
                  sequencerIndex = None,
                  recordTime = ts1,
                )
              ),
            )
            repairFollowedBySequencedEvent <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                SynchronizerIndex(
                  repairIndex = Some(
                    RepairIndex(
                      timestamp = ts1,
                      counter = repairCounter + 1L,
                    )
                  ),
                  sequencerIndex = Some(
                    SequencerIndex(ts2)
                  ),
                  recordTime = ts2,
                )
              ),
            )
          } yield {
            noRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              RequestCounter.Genesis,
              sc + 1L,
              ts0,
            )
            noRepair.processing shouldBe MessageProcessingStartingPoint(
              RequestCounter.Genesis,
              sc + 1L,
              ts0,
              ts0,
              RepairCounter.Genesis,
            )

            repairAndSequencedEvent.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              RequestCounter.Genesis,
              sc + 2L,
              ts1,
            )
            repairAndSequencedEvent.processing shouldBe MessageProcessingStartingPoint(
              RequestCounter.Genesis,
              sc + 2L,
              ts1,
              ts1,
              repairCounter + 1L,
            )
            loneRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              RequestCounter.Genesis,
              SequencerCounter.Genesis,
              CantonTimestamp.MinValue,
            )
            loneRepair.processing shouldBe MessageProcessingStartingPoint(
              RequestCounter.Genesis,
              SequencerCounter.Genesis,
              CantonTimestamp.MinValue,
              ts1,
              repairCounter + 2L,
            )
            repairFollowedBySequencedEvent.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              RequestCounter.Genesis,
              sc + 3L,
              ts2,
            )
            repairFollowedBySequencedEvent.processing shouldBe MessageProcessingStartingPoint(
              RequestCounter.Genesis,
              sc + 3L,
              ts2,
              ts2,
              RepairCounter.Genesis, // repair counter starts a genesis at the new record time ts2
            )
          }
        }
      }

      "there are only repair requests" should {
        "skip over the clean repair requests" in {
          val rjs = new InMemoryRequestJournalStore(loggerFactory)
          val ses = new InMemorySequencedEventStore(loggerFactory, timeouts)
          val repairTs = CantonTimestamp.MinValue

          for {
            oneRepair <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                SynchronizerIndex(
                  repairIndex = Some(
                    RepairIndex(
                      timestamp = repairTs,
                      counter = RepairCounter.Genesis,
                    )
                  ),
                  sequencerIndex = None,
                  recordTime = repairTs,
                )
              ),
            )
            synchronizerIndex = Some(
              SynchronizerIndex(
                repairIndex = Some(
                  RepairIndex(
                    timestamp = repairTs,
                    counter = RepairCounter.Genesis + 1L,
                  )
                ),
                sequencerIndex = None,
                recordTime = repairTs,
              )
            )
            twoRepairs <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              synchronizerIndex,
            )
            // Repair has crashed before advancing the clean request index
            crashedRepair <- SyncEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              synchronizerIndex,
            )
          } yield {
            val startOne = MessageProcessingStartingPoint(
              RequestCounter.Genesis,
              SequencerCounter.Genesis,
              CantonTimestamp.MinValue,
              CantonTimestamp.MinValue,
              RepairCounter.Genesis + 1L,
            )

            oneRepair shouldBe ProcessingStartingPoints.tryCreate(
              startOne.toMessageCleanReplayStartingPoint,
              startOne,
            )

            val startTwo = MessageProcessingStartingPoint(
              RequestCounter.Genesis,
              SequencerCounter.Genesis,
              CantonTimestamp.MinValue,
              CantonTimestamp.MinValue,
              RepairCounter.Genesis + 2L,
            )

            twoRepairs shouldBe ProcessingStartingPoints.tryCreate(
              startTwo.toMessageCleanReplayStartingPoint,
              startTwo,
            )

            crashedRepair shouldBe twoRepairs
          }
        }
      }
    }
  }
}
