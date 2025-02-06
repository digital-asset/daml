// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.syntax.functor.*
import com.daml.nonempty.NonEmptyUtil
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{SyncCryptoClient, SynchronizerSnapshotSyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestampSecond
import com.digitalasset.canton.participant.event.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  RecordTime,
}
import com.digitalasset.canton.participant.pruning
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.RunningCommitments
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  ParticipantId,
  SynchronizerId,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, FailOnShutdown, LfPartyId, ReassignmentCounter}
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.mutable

@nowarn("msg=match may not be exhaustive")
class AcsCommitmentMultiHostedPartyTrackerTest
    extends AsyncWordSpec
    with SortedReconciliationIntervalsHelpers
    with BaseTest
    with HasTestCloseContext
    with FailOnShutdown {

  private val intervalLength = 1L
  private val intervalSeconds = PositiveSeconds.tryOfSeconds(intervalLength)
  private val emptyTopology = Map.empty[LfPartyId, (Int, Set[ParticipantId])]

  protected def ts(i: Long): CantonTimestampSecond =
    CantonTimestampSecond.ofEpochSecond(i.longValue)

  protected def period(i: Long): CommitmentPeriod =
    new CommitmentPeriod(ts(i), intervalSeconds)

  protected lazy val synchronizerId: SynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::da")
  )

  protected lazy val List(localId, remoteId1, remoteId2, remoteId3, remoteId4, remoteId5) =
    List(
      "localParticipant::synchronizer",
      "remoteParticipant1::synchronizer",
      "remoteParticipant2::synchronizer",
      "remoteParticipant3::synchronizer",
      "remoteParticipant4::synchronizer",
      "remoteParticipant5::synchronizer",
    )
      .map(UniqueIdentifier.tryFromProtoPrimitive)
      .map(ParticipantId(_))

  protected lazy val List(alice, bob, carol, danna, ed) =
    List("Alice::1", "Bob::2", "Carol::3", "Danna::4", "Ed::5")
      .map(LfPartyId.assertFromString)

  private def mk(): AcsCommitmentMultiHostedPartyTracker =
    new AcsCommitmentMultiHostedPartyTracker(localId, timeouts, loggerFactory)

  private def buildPartyTracker(
      party: LfPartyId,
      threshold: Int,
      participant: Seq[ParticipantId],
  ): MultiHostedPartyTracker =
    new MultiHostedPartyTracker(party, new AtomicInteger(threshold), mutable.Set(participant*))

  /** this check if map contains a given party tracker, for a given period */
  private def checkIfMapContains(
      tracker: AcsCommitmentMultiHostedPartyTracker,
      period: CommitmentPeriod,
      partyTracker: MultiHostedPartyTracker,
  ): Unit = {
    val trackedPeriod = tracker.commitmentThresholdsMap.get(period)
    trackedPeriod match {
      case None =>
        fail(s"$period was not present in the AcsCommitmentMultiHostedPartyTracker's map")
      case Some(partyTrackerSet) =>
        val party = partyTrackerSet.find(_.partyId == partyTracker.partyId)
        party match {
          case None =>
            fail(
              s"${partyTracker.partyId} was not present in AcsCommitmentMultiHostedPartyTracker's map for period $period"
            )
          case Some(matchedParty) =>
            matchedParty.threshold.get shouldBe partyTracker.threshold.get()
            matchedParty.missingParticipants should contain theSameElementsAs partyTracker.missingParticipants
        }
    }
  }

  /** same as checkIfMapContains, however this one succeeds if the party is missing. It still fails if the period is missing */
  private def checkIfMapDoesNotContains(
      tracker: AcsCommitmentMultiHostedPartyTracker,
      period: CommitmentPeriod,
      partyId: LfPartyId,
  ): Unit = {
    val trackedPeriod = tracker.commitmentThresholdsMap.get(period)
    trackedPeriod match {
      case None =>
        fail(s"$period was not present in the AcsCommitmentMultiHostedPartyTracker's map")
      case Some(partyTrackerSet) =>
        val party = partyTrackerSet.find(_.partyId == partyId)
        party match {
          case None =>
            ()
          case Some(matchedParty) =>
            fail(s"found $matchedParty")
        }
    }
  }

  /** builds a threshold based topology */
  protected def cryptoSetup(
      owner: ParticipantId,
      topology: Map[LfPartyId, (Int, Set[ParticipantId])],
  ): SyncCryptoClient[SynchronizerSnapshotSyncCryptoApi] = {

    val topologyWithPermissions =
      topology.fmap { case (threshold, participants) =>
        (
          PositiveInt.tryCreate(threshold),
          participants.map(p => (p, ParticipantPermission.Submission)).toSeq,
        )
      }

    TestingTopology()
      .withThreshold(topologyWithPermissions)
      .build(loggerFactory)
      .forOwnerAndSynchronizer(owner)
  }

  protected def rt(timestamp: Long, tieBreaker: Int): RecordTime =
    RecordTime(ts(timestamp).forgetRefinement, tieBreaker.toLong)

  protected val coid: (Int, Int) => LfContractId = (txId, discriminator) =>
    ExampleTransactionFactory.suffixedId(txId, discriminator)

  // builds a simple contract setup so all participants are active
  protected def contractSetup(
      topology: Map[LfPartyId, (Int, Set[ParticipantId])]
  ): RunningCommitments = {

    val uniqueParties = topology.keys.toSet

    val rc =
      new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)

    val party1 = uniqueParties.headOption

    uniqueParties.zipWithIndex.foreach { case (value, index) =>
      val changes = AcsChange(
        activations = Map(
          coid(0, index) -> ContractStakeholdersAndReassignmentCounter(
            Set(
              party1.getOrElse(
                throw new IllegalArgumentException("invalid topology, no parties, but parties")
              ),
              value,
            ),
            ReassignmentCounter.Genesis + index,
          )
        ),
        deactivations = Map.empty,
      )

      rc.update(rt(0, index), changes)
    }

    rc
  }

  "BuildNewPeriod" must {
    "Build a simple period" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1, remoteId2)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
      } yield {
        checkIfMapContains(tracker, period0, buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2)))
        succeed
      }

    }
    "Build multiple periods" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1, remoteId2)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period1 = period(1)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ <- tracker.trackPeriod(period1, snapshot, acs.snapshot(), Set.empty)
      } yield {
        checkIfMapContains(tracker, period0, buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2)))
        checkIfMapContains(tracker, period1, buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2)))
        succeed
      }
    }

    "Idempotent behavior when tracking new periods" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1, remoteId2)),
      )
      val topology2 = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1)),
      )
      val crypto = cryptoSetup(localId, topology)
      val crypto2 = cryptoSetup(localId, topology2)
      val acs = contractSetup(topology)
      val acs2 = contractSetup(topology2)
      val period0 = period(0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        snapshot2 <- crypto2.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ <- tracker.trackPeriod(period0, snapshot2, acs2.snapshot(), Set.empty)
      } yield {
        checkIfMapContains(tracker, period0, buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2)))
        succeed
      }
    }

    "Handle an empty topology" in {

      val tracker = mk()
      val crypto = cryptoSetup(localId, emptyTopology)
      val acs = contractSetup(emptyTopology)
      val period0 = period(0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
      } yield {
        tracker.commitmentThresholdsMap.isEmpty shouldBe true
      }
    }

    "Handle a topology with only local participant" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (1, Set(localId)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
      } yield {
        tracker.commitmentThresholdsMap.isEmpty shouldBe true
      }
    }

    "Exclude a locally single hosted party" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = checkIfMapDoesNotContains(tracker, period0, alice)
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 1, Seq(remoteId1)),
        )
      } yield {
        succeed
      }
    }

    "threshold should be limited by no wait configurations" in {
      val tracker = mk()
      val topology = Map(
        alice -> (4, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
        bob -> (3, Set(localId, remoteId1, remoteId2)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set(remoteId3, remoteId4))
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 2, Seq(remoteId1, remoteId2)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(alice, 2, Seq(remoteId1, remoteId2)),
        )
      } yield {
        succeed
      }
    }
  }

  "NewCommit" must {
    "Correctly reduce threshold in map" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period0NE = NonEmptyUtil.fromElement(period0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = tracker.newCommit(remoteId4, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
      } yield {
        succeed
      }
    }

    "Correctly reduce multiple stakeholders" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
        carol -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period0NE = NonEmptyUtil.fromElement(period0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(carol, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = tracker.newCommit(remoteId4, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(carol, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
      } yield {
        succeed
      }
    }

    "only reduce relevant stakeholders" in {
      val tracker = mk()

      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
        carol -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
        danna -> (2, Set(localId, remoteId1, remoteId2, remoteId3)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period0NE = NonEmptyUtil.fromElement(period0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(carol, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(danna, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
        _ = tracker.newCommit(remoteId4, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(carol, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(danna, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
      } yield {
        succeed
      }
    }

    "correctly clear a period" in {
      val tracker = mk()

      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
        carol -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
        danna -> (2, Set(localId, remoteId1, remoteId2, remoteId3)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period0NE = NonEmptyUtil.fromElement(period0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(carol, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(danna, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
        _ = tracker.newCommit(remoteId3, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2, remoteId4)),
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(carol, 1, Seq(remoteId1, remoteId2, remoteId4)),
        )
        _ = checkIfMapDoesNotContains(
          tracker,
          period0,
          danna,
        )
        _ = tracker.newCommit(remoteId4, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Cleared)
        )
      } yield {
        succeed
      }
    }

    "Correctly remove completed period" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period0NE = NonEmptyUtil.fromElement(period0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 2, Seq(remoteId1, remoteId2, remoteId3, remoteId4)),
        )
        _ = tracker.newCommit(remoteId4, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = checkIfMapContains(
          tracker,
          period0,
          buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2, remoteId3)),
        )
        _ = tracker.newCommit(remoteId3, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Cleared)
        )
        _ = tracker.commitmentThresholdsMap.isEmpty shouldBe true
      } yield {
        succeed
      }
    }

    "Handle an unknown period" in {
      val tracker = mk()

      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1, remoteId2)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period1 = period(1)
      val period1NE = NonEmptyUtil.fromElement(period1)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = tracker.newCommit(remoteId2, period1NE) shouldBe Set(
          (period1, TrackedPeriodState.NotTracked)
        )
      } yield {
        checkIfMapContains(tracker, period0, buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2)))
        succeed
      }
    }

    "Handle an unknown sender" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1, remoteId2)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period0NE = NonEmptyUtil.fromElement(period0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = tracker.newCommit(remoteId3, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
      } yield {
        checkIfMapContains(tracker, period0, buildPartyTracker(bob, 1, Seq(remoteId1, remoteId2)))
        succeed
      }
    }

    "A party with no active contracts should not be included" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1)),
      )

      val topologyCarol = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1)),
        carol -> (2, Set(localId, remoteId1, remoteId2)),
      )
      // we build the active contracts based on the first topology, but uses the second topology for the topologySnapshot
      // this is similar to carol being there, but not having any active contracts
      val acs = contractSetup(topology)
      val crypto = cryptoSetup(localId, topologyCarol)
      val period0 = period(0)

      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
      } yield {
        checkIfMapDoesNotContains(tracker, period0, carol)
        succeed
      }
    }
  }

  "prune" must {
    "prune only old periods when given timestamp" in {
      val tracker = mk()
      val topology = Map(
        alice -> (1, Set(localId)),
        bob -> (2, Set(localId, remoteId1)),
      )
      val crypto = cryptoSetup(localId, topology)
      val acs = contractSetup(topology)
      val period0 = period(0)
      val period1 = period(1)
      val period2 = period(2)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ <- tracker.trackPeriod(period1, snapshot, acs.snapshot(), Set.empty)
        _ <- tracker.trackPeriod(period2, snapshot, acs.snapshot(), Set.empty)
        _ = tracker.pruneOldPeriods(period2.toInclusive.forgetRefinement)
      } yield {
        tracker.commitmentThresholdsMap.keys should contain only period2
        succeed
      }
    }
  }

  "Complex Scenario" must {
    val tracker = mk()

    // Alice is multi-hosted on r1,r2,r3,r4 (threshold = 3)
    // Bob is multi-hosted on r1,r2 (threshold = 1)
    // Carol is only hosted r3
    // Danna is only hosted on r2
    // Ed is multi-hosted on r2,r4,r5 (threshold = 1)
    val topology = Map(
      alice -> (3, Set(localId, remoteId1, remoteId2, remoteId3, remoteId4)),
      bob -> (1, Set(remoteId1, remoteId2)),
      carol -> (1, Set(remoteId3)),
      danna -> (1, Set(remoteId2)),
      ed -> (1, Set(remoteId2, remoteId4, remoteId5)),
    )
    val crypto = cryptoSetup(localId, topology)
    val acs = contractSetup(topology)

    // since carol is only hosted on r3 we need a response from r3
    "period is not completed before remoteId3 response" in {
      val period0 = period(0)
      val period0NE = NonEmptyUtil.fromElement(period0)
      for {
        snapshot <- crypto.ipsSnapshot(period0.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period0, snapshot, acs.snapshot(), Set.empty)
        _ = tracker.commitmentThresholdsMap(period0).size shouldBe 5
        _ = tracker.newCommit(remoteId1, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId2, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId4, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId5, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Outstanding)
        )
        _ = checkIfMapContains(tracker, period0, buildPartyTracker(carol, 1, Seq(remoteId3)))
        _ = tracker.newCommit(remoteId3, period0NE) shouldBe Set(
          (period0, TrackedPeriodState.Cleared)
        )
      } yield {
        !tracker.commitmentThresholdsMap.contains(period0) shouldBe true
        succeed
      }
    }

    // Since alice and ed are both multi-hosted, then we can complete without r4 or r5 responding
    "period is completed without remoteId4" in {
      val period1 = period(1)
      val period1NE = NonEmptyUtil.fromElement(period1)
      for {
        snapshot <- crypto.ipsSnapshot(period1.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period1, snapshot, acs.snapshot(), Set.empty)
        _ = tracker.commitmentThresholdsMap(period1).size shouldBe 5
        _ = tracker.newCommit(remoteId1, period1NE) shouldBe Set(
          (period1, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId2, period1NE) shouldBe Set(
          (period1, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId3, period1NE) shouldBe Set(
          (period1, TrackedPeriodState.Cleared)
        )
      } yield {
        !tracker.commitmentThresholdsMap.contains(period1) shouldBe true
        succeed
      }
    }

    // since alice, bob and ed is multi-hosted and carol is not hosted on r2
    // then when r2 is missing it should only be danna missing
    "when we are only missing remoteId2, map should only contain Danna" in {
      val period2 = period(2)
      val period2NE = NonEmptyUtil.fromElement(period2)
      for {
        snapshot <- crypto.ipsSnapshot(period2.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period2, snapshot, acs.snapshot(), Set.empty)
        _ = tracker.commitmentThresholdsMap(period2).size shouldBe 5
        _ = tracker.newCommit(remoteId1, period2NE) shouldBe Set(
          (period2, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId3, period2NE) shouldBe Set(
          (period2, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId4, period2NE) shouldBe Set(
          (period2, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId5, period2NE) shouldBe Set(
          (period2, TrackedPeriodState.Outstanding)
        )
        _ = checkIfMapContains(tracker, period2, buildPartyTracker(danna, 1, Seq(remoteId2)))
        _ = tracker.commitmentThresholdsMap(period2).size shouldBe 1
      } yield {
        succeed
      }
    }

    // since remoteId2 and remoteId3 covers everybody, those two alone should be able to complete a period
    "complete with only remoteId2 and remoteId3" in {
      val period3 = period(3)
      val period3NE = NonEmptyUtil.fromElement(period3)
      for {
        snapshot <- crypto.ipsSnapshot(period3.fromExclusive.forgetRefinement)
        _ <- tracker.trackPeriod(period3, snapshot, acs.snapshot(), Set.empty)
        _ = tracker.commitmentThresholdsMap(period3).size shouldBe 5
        _ = tracker.newCommit(remoteId3, period3NE) shouldBe Set(
          (period3, TrackedPeriodState.Outstanding)
        )
        _ = tracker.newCommit(remoteId2, period3NE) shouldBe Set(
          (period3, TrackedPeriodState.Cleared)
        )
      } yield {
        !tracker.commitmentThresholdsMap.contains(period3) shouldBe true
        succeed
      }
    }
  }
}
