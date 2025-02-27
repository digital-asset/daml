// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  LtHash16,
  Signature,
  SigningKeyUsage,
  SigningPublicKey,
  TestHash,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.{
  SortedReconciliationIntervalsHelpers,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.AcsCommitmentStore.ParticipantCommitmentData
import com.digitalasset.canton.protocol.ContractMetadata
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  CommitmentPeriodState,
  SignedProtocolMessage,
}
import com.digitalasset.canton.pruning.ConfigForNoWaitCounterParticipants
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.{
  BaseTest,
  CloseableTest,
  FailOnShutdown,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAsyncWordSpec,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext

trait CommitmentStoreBaseTest
    extends AsyncWordSpec
    with FailOnShutdown
    with BaseTest
    with CloseableTest
    with HasExecutionContext {

  protected lazy val synchronizerId: SynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::synchronizer")
  )

  protected lazy val crypto: SymbolicCrypto = SymbolicCrypto.create(
    testedReleaseProtocolVersion,
    timeouts,
    loggerFactory,
  )

  lazy val testKey: SigningPublicKey =
    crypto.generateSymbolicSigningKey(usage = SigningKeyUsage.ProtocolOnly)

  protected lazy val localId: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::synchronizer")
  )
  protected lazy val remoteId: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant::synchronizer")
  )
  protected lazy val remoteId2: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::synchronizer")
  )
  protected lazy val remoteId3: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant3::synchronizer")
  )
  protected lazy val remoteId4: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant4::synchronizer")
  )

  protected lazy val remoteIdNESet: NonEmpty[Set[ParticipantId]] =
    NonEmptyUtil.fromElement(remoteId).toSet
  protected lazy val remoteId1And2NESet: NonEmpty[Set[ParticipantId]] =
    NonEmptyUtil.fromUnsafe(Set(remoteId, remoteId2))

  protected lazy val intervalInt: Int = 1
  protected lazy val interval: PositiveSeconds = PositiveSeconds.tryOfSeconds(intervalInt.toLong)

  def ts(time: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(time.toLong)
  def meta(stakeholders: LfPartyId*): ContractMetadata =
    ContractMetadata.tryCreate(
      Set.empty,
      stakeholders.toSet,
      maybeKeyWithMaintainersVersioned = None,
    )
  def period(fromExclusive: Int, toInclusive: Int): CommitmentPeriod =
    CommitmentPeriod.create(ts(fromExclusive), ts(toInclusive), interval).value
  def periods(fromExclusive: Int, toInclusive: Int): NonEmpty[Set[CommitmentPeriod]] =
    NonEmptyUtil
      .fromUnsafe(
        (fromExclusive until toInclusive by intervalInt).map { i =>
          CommitmentPeriod.create(ts(i), ts(i + intervalInt), interval).value
        }
      )
      .toSet

  def deriveFullPeriod(commitmentPeriods: NonEmpty[Set[CommitmentPeriod]]): CommitmentPeriod = {
    val fromExclusive = commitmentPeriods.minBy1(_.fromExclusive).fromExclusive
    val toInclusive = commitmentPeriods.maxBy1(_.toInclusive).toInclusive
    new CommitmentPeriod(
      fromExclusive,
      PositiveSeconds
        .create(toInclusive - fromExclusive)
        .valueOrFail(s"could not convert ${(toInclusive - fromExclusive)} to PositiveSeconds"),
    )
  }

  lazy val dummyCommitment: AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("blah".getBytes())
    h.getByteString()
  }
  lazy val hashedDummyCommitment: AcsCommitment.HashedCommitmentType =
    AcsCommitment.hashCommitment(dummyCommitment)
  lazy val dummyCommitment2: AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("yah mon".getBytes())
    h.getByteString()
  }
  lazy val hashedDummyCommitment2: AcsCommitment.HashedCommitmentType =
    AcsCommitment.hashCommitment(dummyCommitment2)

  lazy val dummyCommitment3: AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("it's 42".getBytes())
    h.getByteString()
  }

  lazy val dummyCommitment4: AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("impossibility results".getBytes())
    h.getByteString()
  }

  lazy val dummyCommitment5: AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("mayday".getBytes())
    h.getByteString()
  }

  lazy val dummySignature: Signature =
    crypto.sign(
      crypto.pureCrypto
        .digest(TestHash.testHashPurpose, hashedDummyCommitment.getCryptographicEvidence),
      testKey.id,
      SigningKeyUsage.ProtocolOnly,
    )

  lazy val dummyCommitmentMsg: AcsCommitment =
    AcsCommitment.create(
      synchronizerId,
      remoteId,
      localId,
      deriveFullPeriod(periods(0, 1)),
      dummyCommitment,
      testedProtocolVersion,
    )
  lazy val dummySigned: SignedProtocolMessage[AcsCommitment] =
    SignedProtocolMessage.from(dummyCommitmentMsg, testedProtocolVersion, dummySignature)

  lazy val alice: LfPartyId = LfPartyId.assertFromString("Alice")
  lazy val bob: LfPartyId = LfPartyId.assertFromString("bob")
  lazy val charlie: LfPartyId = LfPartyId.assertFromString("charlie")
}

trait AcsCommitmentStoreTest
    extends CommitmentStoreBaseTest
    with PrunableByTimeTest
    with SortedReconciliationIntervalsHelpers
    with ProtocolVersionChecksAsyncWordSpec {

  lazy val srip: SortedReconciliationIntervalsProvider =
    constantSortedReconciliationIntervalsProvider(interval)

  def acsCommitmentStore(mkWith: ExecutionContext => AcsCommitmentStore): Unit = {

    behave like prunableByTime(mkWith)

    def mk() = mkWith(executionContext)

    "successfully get a stored computed commitment" in {
      val store = mk()

      for {
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId,
              deriveFullPeriod(periods(0, 1)),
              hashedDummyCommitment,
            ),
          )
        )
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId,
              deriveFullPeriod(periods(1, 2)),
              hashedDummyCommitment,
            ),
          )
        )
        found1 <- store.getComputed(period(0, 1), remoteId)
        found2 <- store.getComputed(period(0, 2), remoteId)
        found3 <- store.getComputed(period(0, 1), remoteId2)
      } yield {
        found1.toList shouldBe List(period(0, 1) -> hashedDummyCommitment)
        found2.toList shouldBe List(
          period(0, 1) -> hashedDummyCommitment,
          period(1, 2) -> hashedDummyCommitment,
        )
        found3.toList shouldBe empty
      }
    }

    "correctly compute outstanding commitments" in {
      val store = mk()

      for {
        outstanding0 <- store.outstanding(ts(0), ts(10))
        _ <- store.markOutstanding(periods(1, 5), remoteId1And2NESet)
        outstanding1 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, periods(1, 2))
        outstanding2 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId2, periods(2, 3))
        outstanding3 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, periods(4, 6))
        outstanding4 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId2, periods(1, 5))
        outstanding5 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, periods(2, 4))
        outstanding6 <- store.outstanding(ts(0), ts(10))
      } yield {
        outstanding0.toSet shouldBe Set.empty
        outstanding1.toSet shouldBe Set(
          periods(1, 5).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(1, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
        outstanding2.toSet shouldBe Set(
          periods(1, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
          periods(2, 5).map((_, remoteId, CommitmentPeriodState.Outstanding)),
        ).flatten
        outstanding3.toSet shouldBe Set(
          periods(2, 5).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(1, 2).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
          periods(3, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
        outstanding4.toSet shouldBe Set(
          periods(2, 4).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(1, 2).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
          periods(3, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
        outstanding5.toSet shouldBe Set(
          periods(2, 4).map((_, remoteId, CommitmentPeriodState.Outstanding))
        ).flatten
        outstanding6.toSet shouldBe Set.empty
      }
    }

    "correctly compute matched periods in the outstanding table" in {
      val store = mk()
      for {
        outstanding <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        _ <- store.markOutstanding(periods(1, 5), remoteId1And2NESet)
        outstandingMarked <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
        _ <- store.markSafe(remoteId, periods(1, 2))
        _ <- store.markSafe(remoteId2, periods(2, 3))
        _ <- store.markSafe(remoteId, periods(4, 5))
        _ <- store.markSafe(remoteId2, periods(1, 5))
        _ <- store.markSafe(remoteId, periods(2, 4))
        outstandingAfter <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
      } yield {
        outstanding.toSet shouldBe Set.empty
        outstandingMarked.toSet shouldBe Set(
          periods(1, 5).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(1, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
        outstandingAfter.toSet shouldBe Set(
          periods(1, 5).map((_, remoteId2, CommitmentPeriodState.Matched)),
          periods(2, 4).map((_, remoteId, CommitmentPeriodState.Matched)),
          periods(4, 5).map((_, remoteId, CommitmentPeriodState.Matched)),
          periods(1, 2).map((_, remoteId, CommitmentPeriodState.Matched)),
        ).flatten
      }
    }

    "correctly store surrounding commits in their correct state when marking period" in {
      val store = mk()
      for {
        outstanding <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        _ <- store.markOutstanding(periods(1, 5), remoteIdNESet)
        _ <- store.markUnsafe(remoteId, periods(1, 5))
        _ <- store.markSafe(remoteId, periods(2, 4))
        outstandingAfter <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
      } yield {
        outstanding.toSet shouldBe Set.empty
        outstandingAfter.toSet shouldBe Set(
          periods(1, 2).map((_, remoteId, CommitmentPeriodState.Mismatched)),
          periods(2, 4).map((_, remoteId, CommitmentPeriodState.Matched)),
          periods(4, 5).map((_, remoteId, CommitmentPeriodState.Mismatched)),
        ).flatten
      }
    }

    "correctly perform state transition in the the outstanding table" in {
      val store = mk()
      for {
        outstanding <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        _ <- store.markOutstanding(periods(1, 5), remoteIdNESet)
        _ <- store.markUnsafe(remoteId, periods(1, 5))
        outstandingUnsafe <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
        // we are allowed to transition from state Mismatched to state Matched
        _ <- store.markSafe(remoteId, periods(1, 5))
        outstandingSafe <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        // we are not allowed to transition from state Matched to state Mismatch
        _ <- store.markUnsafe(remoteId, periods(1, 5))
        outstandingStillSafe <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
        _ <- store.markOutstanding(periods(1, 5), remoteIdNESet)
      } yield {
        outstanding.toSet shouldBe Set.empty
        outstandingUnsafe.toSet shouldBe Set(
          periods(1, 5).map((_, remoteId, CommitmentPeriodState.Mismatched))
        ).flatten
        outstandingSafe.toSet shouldBe Set(
          periods(1, 5).map((_, remoteId, CommitmentPeriodState.Matched))
        ).flatten
        outstandingStillSafe.toSet shouldBe Set(
          periods(1, 5).map((_, remoteId, CommitmentPeriodState.Matched))
        ).flatten
      }
    }

    "correctly compute the no outstanding commitment limit" in {
      val store = mk()

      val endOfTime = ts(10)
      for {
        limit0 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(0, 2)))
        limit1 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markOutstanding(periods(2, 4), remoteId1And2NESet)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(2, 4)))
        limit2 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, periods(2, 3))
        limit3 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId2, periods(3, 4))
        limit4 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId2, periods(2, 3))
        limit5 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, periods(3, 4))
        limit6 <- store.noOutstandingCommitments(endOfTime)
      } yield {
        limit0 shouldBe None
        limit1 shouldBe Some(ts(2))
        limit2 shouldBe Some(ts(2))
        limit3 shouldBe Some(ts(2))
        limit4 shouldBe Some(ts(2))
        limit5 shouldBe Some(ts(3))
        limit6 shouldBe Some(ts(4))
      }
    }

    "correctly compute the no outstanding when ignoring participants" in {
      val store = mk()
      val configRemoteId1 = ConfigForNoWaitCounterParticipants(
        synchronizerId,
        remoteId,
      )
      val configRemoteId2 = ConfigForNoWaitCounterParticipants(
        synchronizerId,
        remoteId2,
      )
      val configRemoteId3 = ConfigForNoWaitCounterParticipants(
        synchronizerId,
        remoteId3,
      )
      val endOfTime = ts(10)
      for {
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(0, 2)))
        _ <- store.markOutstanding(periods(2, 4), remoteIdNESet)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(2, 4)))
        _ <- store.markOutstanding(periods(4, 6), remoteId1And2NESet)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(4, 6)))
        _ <- store.markOutstanding(
          periods(6, 8),
          NonEmptyUtil.fromUnsafe(Set(remoteId, remoteId2, remoteId3)),
        )
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(6, 8)))
        limitNoIgnore <- store.noOutstandingCommitments(endOfTime)
        _ <- store.acsCounterParticipantConfigStore
          .addNoWaitCounterParticipant(Seq(configRemoteId1))
        limitIgnoreRemoteId <- store.noOutstandingCommitments(endOfTime)

        _ <- store.acsCounterParticipantConfigStore.addNoWaitCounterParticipant(
          Seq(configRemoteId2)
        )
        limitOnlyRemoteId3 <- store.noOutstandingCommitments(endOfTime)

        _ <- store.acsCounterParticipantConfigStore.addNoWaitCounterParticipant(
          Seq(configRemoteId3)
        )
        limitIgnoreAll <- store.noOutstandingCommitments(endOfTime)
        _ <- store.acsCounterParticipantConfigStore
          .removeNoWaitCounterParticipant(Seq(synchronizerId), Seq(remoteId, remoteId2, remoteId3))
      } yield {
        limitNoIgnore shouldBe Some(ts(2)) // remoteId stopped after ts(2)
        // remoteId is ignored
        limitIgnoreRemoteId shouldBe Some(ts(4)) // remoteId2 stopped after ts(4)
        // remoteId,remoteId2 is ignored
        limitOnlyRemoteId3 shouldBe Some(ts(6)) // remoteId3 stopped after ts(6)
        // remoteId,remoteId2,remoteId3 is ignored
        limitIgnoreAll shouldBe Some(ts(8)) // latest commit
      }
    }

    "correctly compute the no outstanding commitment limit with gaps in commitments" in {
      val store = mk()

      val endOfTime = ts(10)
      for {
        limit0 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(0, 2)))
        limit1 <- store.noOutstandingCommitments(endOfTime)
        limit11 <- store.noOutstandingCommitments(ts(1))
        _ <- store.markOutstanding(periods(2, 4), remoteId1And2NESet)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(2, 4)))
        limit2 <- store.noOutstandingCommitments(endOfTime)
        limit21 <- store.noOutstandingCommitments(ts(2))
        limit22 <- store.noOutstandingCommitments(ts(3))
        limit23 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId, periods(2, 3))
        limit3 <- store.noOutstandingCommitments(endOfTime)
        limit31 <- store.noOutstandingCommitments(ts(2))
        limit32 <- store.noOutstandingCommitments(ts(3))
        limit33 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId2, periods(3, 4))
        limit4 <- store.noOutstandingCommitments(endOfTime)
        limit41 <- store.noOutstandingCommitments(ts(2))
        limit42 <- store.noOutstandingCommitments(ts(3))
        limit43 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId, periods(3, 4))
        limit5 <- store.noOutstandingCommitments(endOfTime)
        limit51 <- store.noOutstandingCommitments(ts(2))
        limit52 <- store.noOutstandingCommitments(ts(3))
        limit53 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId2, periods(2, 3))
        limit6 <- store.noOutstandingCommitments(endOfTime)
        limit61 <- store.noOutstandingCommitments(ts(3))
        _ <- store.markOutstanding(periods(4, 6), remoteId1And2NESet)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(4, 6)))
        limit7 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(6, 10)))
        limit8 <- store.noOutstandingCommitments(endOfTime)
        limit81 <- store.noOutstandingCommitments(ts(6))
      } yield {
        limit0 shouldBe None
        limit1 shouldBe Some(ts(2))
        limit11 shouldBe Some(ts(1))
        limit2 shouldBe Some(ts(2))
        limit21 shouldBe Some(ts(2))
        limit22 shouldBe Some(ts(2))
        limit23 shouldBe Some(ts(2))
        limit3 shouldBe Some(ts(2))
        limit31 shouldBe Some(ts(2))
        limit32 shouldBe Some(ts(2))
        limit33 shouldBe Some(ts(2))
        limit4 shouldBe Some(ts(2))
        limit41 shouldBe Some(ts(2))
        limit42 shouldBe Some(ts(2))
        limit43 shouldBe Some(ts(2))
        limit5 shouldBe Some(ts(4))
        limit51 shouldBe Some(ts(2))
        limit52 shouldBe Some(ts(2))
        limit53 shouldBe Some(ts(4))
        limit6 shouldBe Some(ts(4))
        limit61 shouldBe Some(ts(3))
        limit7 shouldBe Some(ts(4))
        limit8 shouldBe Some(ts(10))
        limit81 shouldBe Some(ts(4))
      }
    }

    "correctly compute the no outstanding commitment limit when periods are marked un-safe" in {
      val store = mk()

      val endOfTime = ts(10)
      for {
        limit0 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(0, 2)))
        limit1 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markOutstanding(periods(2, 5), remoteId1And2NESet)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(2, 5)))
        limit2 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, periods(2, 3))
        _ <- store.markSafe(remoteId2, periods(2, 3))
        limit3 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, periods(3, 4))
        _ <- store.markUnsafe(remoteId2, periods(3, 4))
        limit4 <- store.noOutstandingCommitments(endOfTime)
      } yield {
        limit0 shouldBe None
        limit1 shouldBe Some(ts(2))
        limit2 shouldBe Some(ts(2))
        limit3 shouldBe Some(ts(3))
        limit4 shouldBe Some(ts(3))
      }
    }

    "correctly search stored computed commitments" in {
      val store = mk()

      for {
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId,
              deriveFullPeriod(periods(0, 1)),
              hashedDummyCommitment,
            ),
          )
        )
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId2,
              deriveFullPeriod(periods(1, 2)),
              hashedDummyCommitment,
            ),
          )
        )
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId,
              deriveFullPeriod(periods(1, 2)),
              hashedDummyCommitment,
            ),
          )
        )
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId,
              deriveFullPeriod(periods(2, 3)),
              hashedDummyCommitment,
            ),
          )
        )
        found1 <- store.searchComputedBetween(ts(0), ts(1), Seq(remoteId))
        found2 <- store.searchComputedBetween(ts(0), ts(2))
        found3 <- store.searchComputedBetween(ts(1), ts(1))
        found4 <- store.searchComputedBetween(ts(0), ts(0))
        found5 <- store.searchComputedBetween(ts(2), ts(2), Seq(remoteId, remoteId2))

      } yield {
        found1.toSet shouldBe Set((period(0, 1), remoteId, hashedDummyCommitment))
        found2.toSet shouldBe Set(
          (period(0, 1), remoteId, hashedDummyCommitment),
          (period(1, 2), remoteId, hashedDummyCommitment),
          (period(1, 2), remoteId2, hashedDummyCommitment),
        )
        found3.toSet shouldBe Set((period(0, 1), remoteId, hashedDummyCommitment))
        found4.toSet shouldBe Set.empty
        found5.toSet shouldBe Set(
          (period(1, 2), remoteId, hashedDummyCommitment),
          (period(1, 2), remoteId2, hashedDummyCommitment),
        )
      }
    }

    "correctly search stored remote commitment messages" in {
      val store = mk()

      val dummyMsg2 = AcsCommitment.create(
        synchronizerId,
        remoteId,
        localId,
        deriveFullPeriod(periods(2, 3)),
        dummyCommitment,
        testedProtocolVersion,
      )
      val dummySigned2 =
        SignedProtocolMessage.from(dummyMsg2, testedProtocolVersion, dummySignature)
      val dummyMsg3 = AcsCommitment.create(
        synchronizerId,
        remoteId2,
        localId,
        deriveFullPeriod(periods(0, 1)),
        dummyCommitment,
        testedProtocolVersion,
      )
      val dummySigned3 =
        SignedProtocolMessage.from(dummyMsg3, testedProtocolVersion, dummySignature)

      for {
        _ <- store.storeReceived(dummySigned).failOnShutdown
        _ <- store.storeReceived(dummySigned2).failOnShutdown
        _ <- store.storeReceived(dummySigned3).failOnShutdown
        found1 <- store.searchReceivedBetween(ts(0), ts(1))
        found2 <-
          store.searchReceivedBetween(ts(0), ts(1), Seq(remoteId))
        found3 <-
          store.searchReceivedBetween(ts(0), ts(3), Seq(remoteId, remoteId2))
      } yield {
        found1.toSet shouldBe Set(dummySigned, dummySigned3)
        found2.toSet shouldBe Set(dummySigned)
        found3.toSet shouldBe Set(dummySigned, dummySigned2, dummySigned3)
      }
    }

    "correctly prune the outstanding table" in {
      val store = mk()

      val endOfTime = ts(10)
      for {
        start <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markOutstanding(periods(0, 10), remoteId1And2NESet)
        _ <- store.markComputedAndSent(deriveFullPeriod(periods(0, 10)))
        _ <- store.markSafe(remoteId, periods(0, 2))
        _ <- store.markSafe(remoteId, periods(2, 4))
        _ <- store.markSafe(remoteId, periods(4, 6))

        _ <- store.markSafe(remoteId2, periods(0, 2))
        _ <- store.markUnsafe(remoteId2, periods(2, 4))
        _ <- store.markUnsafe(remoteId2, periods(4, 6))

        _ <- store.prune(ts(3))
        prune1 <- store.outstanding(ts(0), ts(10), includeMatchedPeriods = true)
        _ <- store.prune(ts(6))
        prune2 <- store.outstanding(ts(0), ts(10), includeMatchedPeriods = true)
      } yield {
        start shouldBe None
        // we pruned everything that has a toInclusive timestamp < 3
        // this means all periods(0, 2) since periods(2,4) gets split into Period(2,3) & Period(3,4)
        prune1.toSet shouldBe Set(
          periods(2, 4).map((_, remoteId, CommitmentPeriodState.Matched)),
          periods(4, 6).map((_, remoteId, CommitmentPeriodState.Matched)),
          periods(6, 10).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(2, 4).map((_, remoteId2, CommitmentPeriodState.Mismatched)),
          periods(4, 6).map((_, remoteId2, CommitmentPeriodState.Mismatched)),
          periods(6, 10).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
        // we pruned everything that has a toInclusive timestamp < 6
        // this means all periods(0, 2), all periods(2, 4) and half the periods(4,6) since periods(4,6) gets split into period(4,5) (which is less than 6) and period(5,6)
        prune2.toSet shouldBe Set(
          periods(5, 6).map((_, remoteId, CommitmentPeriodState.Matched)),
          periods(6, 10).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(5, 6).map((_, remoteId2, CommitmentPeriodState.Mismatched)),
          periods(6, 10).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
      }
    }

    "allow storing different remote commitment messages for the same period" in {
      val store = mk()

      val dummyMsg2 = AcsCommitment.create(
        synchronizerId,
        remoteId,
        localId,
        deriveFullPeriod(periods(0, 1)),
        dummyCommitment2,
        testedProtocolVersion,
      )
      val dummySigned2 =
        SignedProtocolMessage.from(dummyMsg2, testedProtocolVersion, dummySignature)

      for {
        _ <- store.storeReceived(dummySigned).failOnShutdown
        _ <- store.storeReceived(dummySigned2).failOnShutdown
        found1 <- store.searchReceivedBetween(ts(0), ts(1))
      } yield {
        found1.toSet shouldBe Set(dummySigned, dummySigned2)
      }
    }

    "be idempotent when storing the same remote commitment messages for the same period" in {
      val store = mk()

      for {
        _ <- store.storeReceived(dummySigned).failOnShutdown
        _ <- store.storeReceived(dummySigned).failOnShutdown
        found1 <- store.searchReceivedBetween(ts(0), ts(1))
      } yield {
        found1.toList shouldBe List(dummySigned)
      }
    }

    "be idempotent when storing the same computed commitment messages" in {
      val store = mk()

      for {
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId,
              deriveFullPeriod(periods(0, 1)),
              hashedDummyCommitment,
            ),
          )
        )
        _ <- store.storeComputed(
          NonEmpty(
            List,
            ParticipantCommitmentData(
              remoteId,
              deriveFullPeriod(periods(0, 1)),
              hashedDummyCommitment,
            ),
          )
        )
        found1 <- store.searchComputedBetween(ts(0), ts(1))
      } yield {
        found1.toList shouldBe List((period(0, 1), remoteId, hashedDummyCommitment))
      }
    }

    "fails when storing different computed commitments for the same period and counter participant" in {
      val store = mk()

      loggerFactory.suppressWarningsAndErrors {
        recoverToSucceededIf[Throwable] {
          (for {
            _ <- store.storeComputed(
              NonEmpty(
                List,
                ParticipantCommitmentData(
                  remoteId,
                  deriveFullPeriod(periods(0, 1)),
                  hashedDummyCommitment,
                ),
              )
            )
            _ <- store.storeComputed(
              NonEmpty(
                List,
                ParticipantCommitmentData(
                  remoteId,
                  deriveFullPeriod(periods(0, 1)),
                  hashedDummyCommitment2,
                ),
              )
            )
          } yield ()).failOnShutdown
        }
      }
    }

    "compute reasonable clean periods before on small examples" in {
      val beforeOrAt = ts(20)

      def times(i: Integer, j: Integer) = ts(i) -> ts(j)
      val uncleanPeriodsWithResults = List(
        List() -> ts(20),
        List(times(0, 5)) -> ts(20),
        List(times(0, 5), times(0, 5)) -> ts(20),
        List(times(15, 20)) -> ts(15),
        List(times(0, 5), times(15, 20)) -> ts(15),
        List(times(5, 15), times(15, 20)) -> ts(5),
        List(times(10, 15), times(5, 10), times(15, 20)) -> ts(5),
        List(times(5, 15), times(5, 10), times(10, 15), times(10, 15), times(15, 20)) -> ts(5),
        List(times(5, 15), times(10, 15), times(10, 15), times(15, 20), times(5, 10)) -> ts(5),
        List(times(0, 5), times(5, 10), times(15, 20)) -> ts(15),
        List(times(0, 5), times(20, 25)) -> ts(20),
        List(times(15, 20), times(20, 25)) -> ts(15),
        List(times(0, 20)) -> ts(0),
        List(times(15, 20), times(5, 15), times(0, 5)) -> ts(0),
        List(times(0, 5), times(5, 10), times(10, 15), times(15, 20)) -> ts(0),
        List(times(0, 5), times(5, 10), times(15, 20), times(10, 15)) -> ts(0),
        List(times(0, 10), times(10, 20)) -> ts(0),
        List(times(0, 10), times(0, 5), times(5, 10), times(15, 20), times(10, 20)) -> ts(0),
        List(times(25, 30)) -> ts(20),
        List(times(0, 10), times(25, 30)) -> ts(20),
        List(times(5, 15), times(10, 20), times(25, 30)) -> ts(5),
      )

      forAll(uncleanPeriodsWithResults) { case (uncleans, expected) =>
        AcsCommitmentStore.latestCleanPeriod(beforeOrAt, uncleans) shouldBe expected
      }
    }

    "can tolerate overlapping outstanding periods" in {
      val store = mk()

      for {
        _ <- store.markOutstanding(periods(0, 1), remoteIdNESet)
        _ <- store.markOutstanding(periods(0, 2), remoteIdNESet)
        _ <- store.markSafe(remoteId, periods(1, 2))
        outstandingWithId <- store.outstanding(ts(0), ts(2), Seq(remoteId))

        outstandingWithoutId <- store.outstanding(ts(0), ts(2))
      } yield {
        outstandingWithId.toSet shouldBe Set(
          (period(0, 1), remoteId, CommitmentPeriodState.Outstanding)
        )
        outstandingWithoutId.toSet shouldBe Set(
          (period(0, 1), remoteId, CommitmentPeriodState.Outstanding)
        )
      }
    }

    "can combine markSafe and markUnsafe and compute correct outstanding" in {
      val store = mk()

      for {
        outstanding0 <- store.outstanding(ts(0), ts(10))
        _ <- store.markOutstanding(periods(0, 5), remoteId1And2NESet)

        _ <- store.markSafe(remoteId, periods(1, 2))
        _ <- store.markUnsafe(remoteId2, periods(1, 2))
        outstanding1 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, periods(2, 3))
        _ <- store.markUnsafe(remoteId2, periods(2, 3))
        outstanding2 <- store.outstanding(ts(0), ts(10))
        outstanding3 <- store.outstanding(ts(0), ts(10), includeMatchedPeriods = true)

      } yield {
        outstanding0.toSet shouldBe Set.empty
        outstanding1.toSet shouldBe Set(
          // remoteId & remoteId2 one still has period (0,1) outstanding
          periods(0, 1).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(0, 1).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
          // remoteId2 has an outstanding period (1,2) in state mismatched
          periods(1, 2).map((_, remoteId2, CommitmentPeriodState.Mismatched)),
          // remoteId & remoteId2 one still has period (2,5) outstanding
          periods(2, 5).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(2, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
        outstanding2.toSet shouldBe Set(
          // remoteId & remoteId2 one still has period (0,1) outstanding
          periods(0, 1).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(0, 1).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
          // remoteId2 has a period (1,2) & (2,3) that is mismatched
          periods(1, 3).map((_, remoteId2, CommitmentPeriodState.Mismatched)),
          // remoteId & remoteId2 one still has period (3,5) outstanding
          periods(3, 5).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(3, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten

        outstanding3.toSet shouldBe Set(
          // remoteId & remoteId2 one still has period (0,1) outstanding
          periods(0, 1).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(0, 1).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
          // remoteId has a period (1,2) & (2,3) that is matched, but we are including matched periods
          periods(1, 3).map((_, remoteId, CommitmentPeriodState.Matched)),
          // remoteId2 has a period (1,2) & (2,3) that is mismatched
          periods(1, 3).map((_, remoteId2, CommitmentPeriodState.Mismatched)),
          // remoteId & remoteId2 one still has period (3,5) outstanding
          periods(3, 5).map((_, remoteId, CommitmentPeriodState.Outstanding)),
          periods(3, 5).map((_, remoteId2, CommitmentPeriodState.Outstanding)),
        ).flatten
      }
    }

    "can idempotent mark period as multi hosted cleared" in {
      val store = mk()
      for {
        _ <- store.markOutstanding(periods(0, 1), remoteIdNESet)
        _ <- store.markComputedAndSent(period(0, 1))
        noOutstanding <- store.noOutstandingCommitments(ts(10))
        _ <- store.markMultiHostedCleared(period(0, 1))
        noOutstandingAfterCleared <- store.noOutstandingCommitments(ts(10))
        _ <- store.markMultiHostedCleared(period(0, 1))
        noOutstandingAfterClearedIdempotent <- store.noOutstandingCommitments(ts(10))

      } yield {
        noOutstanding shouldBe Some(ts(0))
        noOutstandingAfterCleared shouldBe Some(ts(1))
        noOutstandingAfterClearedIdempotent shouldBe noOutstandingAfterCleared
      }
    }

    "can mark non-existing period without problems" in {
      val store = mk()
      for {
        _ <- store.markOutstanding(periods(0, 1), remoteIdNESet)
        _ <- store.markComputedAndSent(period(0, 1))
        noOutstanding <- store.noOutstandingCommitments(ts(10))
        _ <- store.markMultiHostedCleared(period(0, 2))
        noOutstandingAfterCleared <- store.noOutstandingCommitments(ts(10))

      } yield {
        noOutstanding shouldBe Some(ts(0))
        // we marked an invalid period so it shouldn't advance
        noOutstandingAfterCleared shouldBe noOutstanding
      }
    }

    "marked periods are fine even with mismatches" in {
      val store = mk()
      for {
        _ <- store.markOutstanding(periods(0, 1), remoteIdNESet)
        _ <- store.markComputedAndSent(period(0, 1))
        noOutstanding <- store.noOutstandingCommitments(ts(10))
        _ <- store.markUnsafe(remoteId, periods(0, 1))
        noOutstandingUnsafe <- store.noOutstandingCommitments(ts(10))
        _ <- store.markMultiHostedCleared(period(0, 1))
        noOutstandingAfterCleared <- store.noOutstandingCommitments(ts(10))

        // we try to remark the period as unsafe, this should not cause an update
        _ <- store.markUnsafe(remoteId, periods(0, 1))
        noOutStandingAfterMarkUnsafe <- store.noOutstandingCommitments(ts(10))
      } yield {
        noOutstanding shouldBe Some(ts(0))
        noOutstandingUnsafe shouldBe noOutstanding

        noOutstandingAfterCleared shouldBe Some(ts(1))
        noOutStandingAfterMarkUnsafe shouldBe noOutstandingAfterCleared
      }
    }

  }

}

trait IncrementalCommitmentStoreTest extends CommitmentStoreBaseTest {
  import com.digitalasset.canton.lfPartyOrdering

  def commitmentSnapshotStore(mkWith: ExecutionContext => IncrementalCommitmentStore): Unit = {

    def mk() = mkWith(executionContext)

    def rt(timestamp: Int, tieBreaker: Int) = RecordTime(ts(timestamp), tieBreaker.toLong)

    "give correct snapshots on a small example" in {
      val snapshot = mk()

      val snapAB10 = ByteString.copyFromUtf8("AB10")
      val snapBC10 = ByteString.copyFromUtf8("BC10")
      val snapBC11 = ByteString.copyFromUtf8("BC11")
      val snapAB2 = ByteString.copyFromUtf8("AB21")
      val snapAC2 = ByteString.copyFromUtf8("AC21")

      for {
        res0 <- snapshot.get()
        wm0 <- snapshot.watermark

        _ <- snapshot.update(
          rt(1, 0),
          updates = Map(SortedSet(alice, bob) -> snapAB10, SortedSet(bob, charlie) -> snapBC10),
          deletes = Set.empty,
        )
        res1 <- snapshot.get()
        wm1 <- snapshot.watermark

        _ <- snapshot.update(
          rt(1, 1),
          updates = Map(SortedSet(bob, charlie) -> snapBC11),
          deletes = Set.empty,
        )
        res11 <- snapshot.get()
        wm11 <- snapshot.watermark

        _ <- snapshot.update(
          rt(2, 0),
          updates = Map(SortedSet(alice, bob) -> snapAB2, SortedSet(alice, charlie) -> snapAC2),
          deletes = Set(SortedSet(bob, charlie)),
        )
        res2 <- snapshot.get()
        ts2 <- snapshot.watermark

        _ <- snapshot.update(
          rt(3, 0),
          updates = Map.empty,
          deletes = Set(SortedSet(alice, bob), SortedSet(alice, charlie)),
        )
        res3 <- snapshot.get()
        ts3 <- snapshot.watermark

      } yield {
        wm0 shouldBe RecordTime.MinValue
        res0 shouldBe (RecordTime.MinValue -> Map.empty)

        wm1 shouldBe rt(1, 0)
        res1 shouldBe (rt(1, 0) -> Map(
          SortedSet(alice, bob) -> snapAB10,
          SortedSet(bob, charlie) -> snapBC10,
        ))

        wm11 shouldBe rt(1, 1)
        res11 shouldBe (rt(1, 1) -> Map(
          SortedSet(alice, bob) -> snapAB10,
          SortedSet(bob, charlie) -> snapBC11,
        ))

        ts2 shouldBe rt(2, 0)
        res2 shouldBe (rt(2, 0) -> Map(
          SortedSet(alice, bob) -> snapAB2,
          SortedSet(alice, charlie) -> snapAC2,
        ))

        ts3 shouldBe rt(3, 0)
        res3 shouldBe (rt(3, 0) -> Map.empty)
      }
    }
  }

}

trait CommitmentQueueTest extends CommitmentStoreBaseTest {

  def commitmentQueue(mkWith: ExecutionContext => CommitmentQueue): Unit = {
    def mk(): CommitmentQueue = mkWith(executionContext)
    def commitment(
        remoteId: ParticipantId,
        start: Int,
        end: Int,
        cmt: AcsCommitment.CommitmentType,
    ) =
      AcsCommitment.create(
        synchronizerId,
        remoteId,
        localId,
        CommitmentPeriod.create(ts(start), ts(end), PositiveSeconds.tryOfSeconds(5)).value,
        cmt,
        testedProtocolVersion,
      )

    "work sensibly in a basic scenario" in {
      val queue = mk()
      val c11 = commitment(remoteId, 0, 5, dummyCommitment)
      val c12 = commitment(remoteId2, 0, 5, dummyCommitment2)
      val c21 = commitment(remoteId, 5, 10, dummyCommitment)
      val c22 = commitment(remoteId2, 5, 10, dummyCommitment2)
      val c31 = commitment(remoteId, 10, 15, dummyCommitment)
      val c32 = commitment(remoteId2, 10, 15, dummyCommitment2)
      val c41 = commitment(remoteId, 15, 20, dummyCommitment)

      (for {
        _ <- queue.enqueue(c11)
        _ <- queue.enqueue(c11) // Idempotent enqueueUS
        _ <- queue.enqueue(c12)
        _ <- queue.enqueue(c21)
        at5 <- queue.peekThrough(ts(5))
        at10 <- queue.peekThrough(ts(10))
        _ <- queue.enqueue(c22)
        at10with22 <- queue.peekThrough(ts(10))
        _ <- queue.enqueue(c32)
        at10with32 <- queue.peekThrough(ts(10))
        at15 <- queue.peekThrough(ts(15))
        _ <- queue.deleteThrough(ts(5))
        at15AfterDelete <- queue.peekThrough(ts(15))
        _ <- queue.enqueue(c31)
        at15with31 <- queue.peekThrough(ts(15))
        _ <- queue.deleteThrough(ts(15))
        at20AfterDelete <- queue.peekThrough(ts(20))
        _ <- queue.enqueue(c41)
        at20with41 <- queue.peekThrough(ts(20))
      } yield {
        // We don't really care how the priority queue breaks the ties, so just use sets here
        at5.toSet shouldBe Set(c11, c12)
        at10.toSet shouldBe Set(c11, c12, c21)
        at10with22.toSet shouldBe Set(c11, c12, c21, c22)
        at10with32.toSet shouldBe Set(c11, c12, c21, c22)
        at15.toSet shouldBe Set(c11, c12, c21, c22, c32)
        at15AfterDelete.toSet shouldBe Set(c21, c22, c32)
        at15with31.toSet shouldBe Set(c21, c22, c32, c31)
        at20AfterDelete shouldBe List.empty
        at20with41 shouldBe List(c41)
      }).failOnShutdown
    }

    "peekThroughAtOrAfter works as expected" in {
      val queue = mk()
      val c11 = commitment(remoteId, 0, 5, dummyCommitment)
      val c12 = commitment(remoteId2, 0, 5, dummyCommitment2)
      val c21 = commitment(remoteId, 5, 10, dummyCommitment)
      val c22 = commitment(remoteId2, 5, 10, dummyCommitment2)
      val c31 = commitment(remoteId, 10, 15, dummyCommitment)
      val c32 = commitment(remoteId2, 10, 15, dummyCommitment2)
      val c41 = commitment(remoteId, 15, 20, dummyCommitment)

      for {
        _ <- queue.enqueue(c11)
        _ <- queue.enqueue(c11) // Idempotent enqueue
        _ <- queue.enqueue(c12)
        _ <- queue.enqueue(c21)
        at5 <- queue.peekThroughAtOrAfter(ts(5))
        at10 <- queue.peekThroughAtOrAfter(ts(10))
        _ <- queue.enqueue(c22)
        at10with22 <- queue.peekThroughAtOrAfter(ts(10))
        at15 <- queue.peekThroughAtOrAfter(ts(15))
        _ <- queue.enqueue(c32)
        at10with32 <- queue.peekThroughAtOrAfter(ts(10))
        at15with32 <- queue.peekThroughAtOrAfter(ts(15))
        _ <- queue.deleteThrough(ts(5))
        at15AfterDelete <- queue.peekThroughAtOrAfter(ts(15))
        _ <- queue.enqueue(c31)
        at15with31 <- queue.peekThroughAtOrAfter(ts(15))
        _ <- queue.deleteThrough(ts(15))
        at20AfterDelete <- queue.peekThroughAtOrAfter(ts(20))
        _ <- queue.enqueue(c41)
        at20with41 <- queue.peekThroughAtOrAfter(ts(20))
      } yield {
        // We don't really care how the priority queue breaks the ties, so just use sets here
        at5.toSet shouldBe Set(c11, c12, c21)
        at10.toSet shouldBe Set(c21)
        at10with22.toSet shouldBe Set(c21, c22)
        at15.toSet shouldBe empty
        at10with32.toSet shouldBe Set(c21, c22, c32)
        at15with32.toSet shouldBe Set(c32)
        at15AfterDelete.toSet shouldBe Set(c32)
        at15with31.toSet shouldBe Set(c32, c31)
        at20AfterDelete shouldBe List.empty
        at20with41 shouldBe List(c41)
      }
    }

    "peekOverlapsForCounterParticipant works as expected" in {

      val dummyCommitmentMsg =
        AcsCommitment.create(
          synchronizerId,
          remoteId,
          localId,
          deriveFullPeriod(periods(0, 5)),
          dummyCommitment,
          testedProtocolVersion,
        )

      val dummyCommitmentMsg2 =
        AcsCommitment.create(
          synchronizerId,
          remoteId,
          localId,
          deriveFullPeriod(periods(10, 15)),
          dummyCommitment2,
          testedProtocolVersion,
        )

      val dummyCommitmentMsg3 =
        AcsCommitment.create(
          synchronizerId,
          remoteId,
          localId,
          deriveFullPeriod(periods(0, 10)),
          dummyCommitment3,
          testedProtocolVersion,
        )

      val queue = mk()
      val c11 = commitment(remoteId, 0, 5, dummyCommitment)
      val c21 = commitment(remoteId2, 0, 5, dummyCommitment4)
      val c12 = commitment(remoteId, 0, 10, dummyCommitment3)
      val c13 = commitment(remoteId, 10, 15, dummyCommitment2)
      val c22 = commitment(remoteId2, 5, 10, dummyCommitment5)

      for {
        _ <- queue.enqueue(c11)
        _ <- queue.enqueue(c12)
        _ <- queue.enqueue(c21)
        at05 <- queue.peekOverlapsForCounterParticipant(deriveFullPeriod(periods(0, 5)), remoteId)(
          nonEmptyTraceContext1
        )
        at010 <- queue.peekOverlapsForCounterParticipant(
          deriveFullPeriod(periods(0, 10)),
          remoteId,
        )(
          nonEmptyTraceContext1
        )
        at510 <- queue.peekOverlapsForCounterParticipant(
          deriveFullPeriod(periods(5, 10)),
          remoteId,
        )(
          nonEmptyTraceContext1
        )
        at1015 <- queue.peekOverlapsForCounterParticipant(
          deriveFullPeriod(periods(10, 15)),
          remoteId,
        )(
          nonEmptyTraceContext1
        )
        _ <- queue.enqueue(c13)
        _ <- queue.enqueue(c22)
        at1015after <- queue.peekOverlapsForCounterParticipant(
          deriveFullPeriod(periods(10, 15)),
          remoteId,
        )(
          nonEmptyTraceContext1
        )
        at510after <- queue.peekOverlapsForCounterParticipant(
          deriveFullPeriod(periods(5, 10)),
          remoteId,
        )(
          nonEmptyTraceContext1
        )
        at515after <- queue.peekOverlapsForCounterParticipant(
          deriveFullPeriod(periods(5, 15)),
          remoteId,
        )(
          nonEmptyTraceContext1
        )
        at015after <- queue.peekOverlapsForCounterParticipant(
          deriveFullPeriod(periods(0, 15)),
          remoteId,
        )(
          nonEmptyTraceContext1
        )
      } yield {
        // We don't really care how the priority queue breaks the ties, so just use sets here
        at05.toSet shouldBe Set(dummyCommitmentMsg, dummyCommitmentMsg3)
        at010.toSet shouldBe Set(dummyCommitmentMsg, dummyCommitmentMsg3)
        at510.toSet shouldBe Set(dummyCommitmentMsg3)
        at1015 shouldBe empty
        at1015after.toSet shouldBe Set(dummyCommitmentMsg2)
        at510after.toSet shouldBe Set(dummyCommitmentMsg3)
        at515after.toSet shouldBe Set(dummyCommitmentMsg3, dummyCommitmentMsg2)
        at015after.toSet shouldBe Set(dummyCommitmentMsg3, dummyCommitmentMsg2, dummyCommitmentMsg)
      }
    }

  }
}
