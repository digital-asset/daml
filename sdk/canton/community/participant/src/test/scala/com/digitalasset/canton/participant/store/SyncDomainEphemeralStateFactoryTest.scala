// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, RequestIndex, SequencerIndex}
import com.digitalasset.canton.participant.admin.repair.RepairContext
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestData
import com.digitalasset.canton.participant.protocol.{
  MessageCleanReplayStartingPoint,
  MessageProcessingStartingPoint,
  ProcessingStartingPoints,
}
import com.digitalasset.canton.participant.store.memory.InMemoryRequestJournalStore
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{OrdinarySerializedEvent, SequencerTestUtils}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.InMemorySequencedEventStore
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  CloseableTest,
  FailOnShutdown,
  RequestCounter,
  SequencerCounter,
}
import org.scalatest.wordspec.AsyncWordSpec

class SyncDomainEphemeralStateFactoryTest
    extends AsyncWordSpec
    with BaseTest
    with CloseableTest
    with FailOnShutdown {

  private lazy val domainId = DomainId.tryFromString("domain::da")

  private def dummyEvent(
      domainId: DomainId
  )(sc: SequencerCounter, timestamp: CantonTimestamp): OrdinarySerializedEvent =
    OrdinarySequencedEvent(
      SignedContent(
        SequencerTestUtils.mockDeliver(sc.v, timestamp, domainId),
        SymbolicCrypto.emptySignature,
        None,
        testedProtocolVersion,
      )
    )(TraceContext.empty)

  "startingPoints" when {
    "there is no clean request" should {
      "return the default" in {
        val rjs = new InMemoryRequestJournalStore(loggerFactory)
        val ses = new InMemorySequencedEventStore(loggerFactory)

        for {
          startingPoints <- SyncDomainEphemeralStateFactory.startingPoints(
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
        val ses = new InMemorySequencedEventStore(loggerFactory)
        val rc = RequestCounter(0)
        val sc = SequencerCounter(10)
        val ts = CantonTimestamp.Epoch
        for {
          _ <- rjs.insert(RequestData.clean(rc, ts, ts.plusSeconds(1)))
          _ <- ses.store(Seq(dummyEvent(domainId)(sc, ts)))
          withCleanSc <- SyncDomainEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              DomainIndex.of(
                RequestIndex(
                  counter = rc,
                  sequencerCounter = Some(sc),
                  timestamp = ts,
                )
              )
            ),
          )
        } yield {
          val cleanReplay = MessageCleanReplayStartingPoint(rc, sc, ts.immediatePredecessor)
          val processing =
            MessageProcessingStartingPoint(rc + 1L, sc + 1L, ts, ts)
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
        val ses = new InMemorySequencedEventStore(loggerFactory)
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
          _ <- ses.store(
            Seq(
              dummyEvent(domainId)(sc, ts0),
              dummyEvent(domainId)(sc + 1L, ts1),
              dummyEvent(domainId)(sc + 2L, ts2),
              dummyEvent(domainId)(sc + 3L, ts3),
              dummyEvent(domainId)(sc + 4L, ts4),
              dummyEvent(domainId)(sc + 5L, ts5),
              dummyEvent(domainId)(sc + 6L, ts6),
            )
          )
          sp1 <- SyncDomainEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              DomainIndex.of(
                RequestIndex(
                  counter = rc,
                  sequencerCounter = Some(sc),
                  timestamp = ts0,
                )
              )
            ),
          )
          sp2 <- SyncDomainEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              DomainIndex.of(
                RequestIndex(
                  counter = rc + 1L,
                  sequencerCounter = Some(sc + 1L),
                  timestamp = ts1,
                )
              )
            ),
          )
          domainIndex = Some(
            DomainIndex(
              requestIndex = Some(
                RequestIndex(
                  counter = rc + 2L,
                  sequencerCounter = Some(sc + 2L),
                  timestamp = ts2,
                )
              ),
              sequencerIndex = Some(
                SequencerIndex(
                  counter = sc + 3L,
                  timestamp = ts3,
                )
              ),
              recordTime = ts3,
            )
          )
          sp3 <- SyncDomainEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            domainIndex,
          )
          sp3WithRecordTimeIncrease <- SyncDomainEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            domainIndex.map(_.copy(recordTime = ts3plus)),
          )
          _ <- rjs.insert(RequestData.initial(rc + 4L, ts6))
          _ <- rjs.insert(RequestData.initial(rc + 3L, ts5))
          sp3a <- SyncDomainEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            domainIndex,
          )
          sp3b <- SyncDomainEphemeralStateFactory.startingPoints(
            rjs,
            ses,
            Some(
              DomainIndex(
                requestIndex = Some(
                  RequestIndex(
                    counter = rc + 2L,
                    sequencerCounter = Some(sc + 2L),
                    timestamp = ts2,
                  )
                ),
                sequencerIndex = Some(
                  SequencerIndex(
                    counter = sc + 4L,
                    timestamp = ts4,
                  )
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
          )

          // start with request 0 because its commit time is after ts1
          sp2.cleanReplay shouldBe MessageCleanReplayStartingPoint(rc, sc, ts0.immediatePredecessor)
          sp2.processing shouldBe MessageProcessingStartingPoint(
            rc + 2L,
            sc + 2L,
            ts1,
            ts1,
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
          )

          // processing starting points propagate the floating record-time from the DomainIndex
          sp3WithRecordTimeIncrease.processing shouldBe MessageProcessingStartingPoint(
            rc + 3L,
            sc + 4L,
            ts3,
            ts3plus,
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
          )
        }
      }

      "the commit times are reversed" should {
        "reprocess the clean request" in {
          val rjs = new InMemoryRequestJournalStore(loggerFactory)
          val ses = new InMemorySequencedEventStore(loggerFactory)
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
            _ <- ses.store(
              Seq(
                dummyEvent(domainId)(sc, ts0),
                dummyEvent(domainId)(sc + 1L, ts1),
                dummyEvent(domainId)(sc + 2L, ts2),
                dummyEvent(domainId)(sc + 3L, ts3),
              )
            )
            sp0 <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                DomainIndex.of(
                  RequestIndex(
                    counter = rc,
                    sequencerCounter = Some(sc),
                    timestamp = ts0,
                  )
                )
              ),
            )
            sp2 <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                DomainIndex.of(
                  RequestIndex(
                    counter = rc + 1L,
                    sequencerCounter = Some(sc + 1L),
                    timestamp = ts1,
                  )
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
            )
          }
        }
      }

      "when there is a dirty repair request" should {
        "return the right result" in {
          val rjs = new InMemoryRequestJournalStore(loggerFactory)
          val ses = new InMemorySequencedEventStore(loggerFactory)
          val rc = RequestCounter.Genesis
          val sc = SequencerCounter(10)
          val ts0 = CantonTimestamp.ofEpochSecond(0)
          val ts1 = CantonTimestamp.ofEpochSecond(1)

          for {
            _ <- ses.store(
              Seq(dummyEvent(domainId)(sc, ts0), dummyEvent(domainId)(sc + 1L, ts1))
            )
            _ <- rjs.insert(
              RequestData.clean(rc + 1L, ts1, ts1, Some(RepairContext.tryCreate("repair1")))
            )
            noCleanRepair <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                DomainIndex(
                  requestIndex = None,
                  sequencerIndex = Some(
                    SequencerIndex(
                      counter = sc,
                      timestamp = ts0,
                    )
                  ),
                  recordTime = ts0,
                )
              ),
            )
            _ <- rjs.insert(
              RequestData.clean(rc, ts0, ts0, Some(RepairContext.tryCreate("repair0")))
            )
            withDirtyRepair <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                DomainIndex(
                  requestIndex = Some(
                    RequestIndex(
                      counter = rc,
                      sequencerCounter = None,
                      timestamp = ts0,
                    )
                  ),
                  sequencerIndex = Some(
                    SequencerIndex(
                      counter = sc + 1L,
                      timestamp = ts1,
                    )
                  ),
                  recordTime = ts1,
                )
              ),
            )
            withCleanRepair <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                DomainIndex.of(
                  RequestIndex(
                    counter = rc + 1L,
                    sequencerCounter = Some(sc + 1L),
                    timestamp = ts1,
                  )
                )
              ),
            )
          } yield {
            noCleanRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              RequestCounter.Genesis,
              sc + 1L,
              ts0,
            )
            noCleanRepair.processing shouldBe MessageProcessingStartingPoint(
              RequestCounter.Genesis,
              sc + 1L,
              ts0,
              ts0,
            )

            withDirtyRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              rc + 1L,
              sc + 2L,
              ts1,
            )
            withDirtyRepair.processing shouldBe MessageProcessingStartingPoint(
              rc + 1L,
              sc + 2L,
              ts1,
              ts1,
            )
            withCleanRepair.cleanReplay shouldBe MessageCleanReplayStartingPoint(
              rc + 2L,
              sc + 2L,
              ts1,
            )
            withCleanRepair.processing shouldBe MessageProcessingStartingPoint(
              rc + 2L,
              sc + 2L,
              ts1,
              ts1,
            )
          }
        }
      }

      "there are only repair requests" should {
        "skip over the clean repair requests" in {
          val rjs = new InMemoryRequestJournalStore(loggerFactory)
          val ses = new InMemorySequencedEventStore(loggerFactory)
          val repairTs = CantonTimestamp.MinValue

          for {
            _ <- rjs.insert(
              RequestData.clean(
                RequestCounter.Genesis,
                repairTs,
                repairTs,
                Some(RepairContext.tryCreate("repair0")),
              )
            )
            oneRepair <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              Some(
                DomainIndex(
                  requestIndex = Some(
                    RequestIndex(
                      counter = RequestCounter.Genesis,
                      sequencerCounter = None,
                      timestamp = repairTs,
                    )
                  ),
                  sequencerIndex = None,
                  recordTime = repairTs,
                )
              ),
            )
            _ <- rjs.insert(
              RequestData.clean(
                RequestCounter.Genesis + 1L,
                repairTs,
                repairTs,
                Some(RepairContext.tryCreate("repair1")),
              )
            )
            domainIndex = Some(
              DomainIndex(
                requestIndex = Some(
                  RequestIndex(
                    counter = RequestCounter.Genesis + 1L,
                    sequencerCounter = None,
                    timestamp = repairTs,
                  )
                ),
                sequencerIndex = None,
                recordTime = repairTs,
              )
            )
            twoRepairs <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              domainIndex,
            )
            _ <- rjs.insert(
              RequestData
                .clean(
                  RequestCounter.Genesis + 2L,
                  repairTs,
                  repairTs,
                  Some(RepairContext.tryCreate("crashed repair")),
                )
            )
            // Repair has crashed before advancing the clean request index
            crashedRepair <- SyncDomainEphemeralStateFactory.startingPoints(
              rjs,
              ses,
              domainIndex,
            )
          } yield {
            val startOne = MessageProcessingStartingPoint(
              RequestCounter.Genesis + 1L,
              SequencerCounter.Genesis,
              CantonTimestamp.MinValue,
              CantonTimestamp.MinValue,
            )

            oneRepair shouldBe ProcessingStartingPoints.tryCreate(
              startOne.toMessageCleanReplayStartingPoint,
              startOne,
            )

            val startTwo = MessageProcessingStartingPoint(
              RequestCounter.Genesis + 2L,
              SequencerCounter.Genesis,
              CantonTimestamp.MinValue,
              CantonTimestamp.MinValue,
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
