// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{LtHash16, Signature, SigningPublicKey, TestHash}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.pruning.{
  SortedReconciliationIntervalsHelpers,
  SortedReconciliationIntervalsProvider,
}
import com.digitalasset.canton.participant.store.AcsCommitmentStore.CommitmentData
import com.digitalasset.canton.protocol.ContractMetadata
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  CommitmentPeriodState,
  SignedProtocolMessage,
}
import com.digitalasset.canton.store.PrunableByTimeTest
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.{
  BaseTest,
  CloseableTest,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAsyncWordSpec,
}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}

trait CommitmentStoreBaseTest
    extends AsyncWordSpec
    with BaseTest
    with CloseableTest
    with HasExecutionContext {

  lazy val domainId: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::domain"))

  lazy val crypto: SymbolicCrypto = SymbolicCrypto.create(
    testedReleaseProtocolVersion,
    timeouts,
    loggerFactory,
  )

  lazy val testKey: SigningPublicKey = crypto.generateSymbolicSigningKey()

  lazy val localId: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::domain")
  )
  lazy val remoteId: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant::domain")
  )
  lazy val remoteId2: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::domain")
  )
  lazy val remoteId3: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant3::domain")
  )
  lazy val remoteId4: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant4::domain")
  )
  lazy val interval: PositiveSeconds = PositiveSeconds.tryOfSeconds(1)

  def ts(time: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(time.toLong)
  def meta(stakeholders: LfPartyId*): ContractMetadata =
    ContractMetadata.tryCreate(
      Set.empty,
      stakeholders.toSet,
      maybeKeyWithMaintainersVersioned = None,
    )
  def period(fromExclusive: Int, toInclusive: Int): CommitmentPeriod =
    CommitmentPeriod.create(ts(fromExclusive), ts(toInclusive), interval).value

  lazy val dummyCommitment: AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("blah".getBytes())
    h.getByteString()
  }
  lazy val dummyCommitment2: AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("yah mon".getBytes())
    h.getByteString()
  }

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
      crypto.pureCrypto.digest(TestHash.testHashPurpose, dummyCommitment),
      testKey.id,
    )

  lazy val dummyCommitmentMsg: AcsCommitment =
    AcsCommitment.create(
      domainId,
      remoteId,
      localId,
      period(0, 1),
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
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId, period(0, 1), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId, period(1, 2), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        found1 <- store.getComputed(period(0, 1), remoteId)
        found2 <- store.getComputed(period(0, 2), remoteId)
        found3 <- store.getComputed(period(0, 1), remoteId2)
      } yield {
        found1.toList shouldBe List(period(0, 1) -> dummyCommitment)
        found2.toList shouldBe List(
          period(0, 1) -> dummyCommitment,
          period(1, 2) -> dummyCommitment,
        )
        found3.toList shouldBe empty
      }
    }

    "correctly compute outstanding commitments" in {
      val store = mk()

      for {
        outstanding0 <- store.outstanding(ts(0), ts(10))
        _ <- store.markOutstanding(period(1, 5), Set(remoteId, remoteId2))
        outstanding1 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, period(1, 2), srip)
        outstanding2 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId2, period(2, 3), srip)
        outstanding3 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, period(4, 6), srip)
        outstanding4 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId2, period(1, 5), srip)
        outstanding5 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, period(2, 4), srip)
        outstanding6 <- store.outstanding(ts(0), ts(10))
      } yield {
        outstanding0.toSet shouldBe Set.empty
        outstanding1.toSet shouldBe Set(
          (period(1, 5), remoteId, CommitmentPeriodState.Outstanding),
          (period(1, 5), remoteId2, CommitmentPeriodState.Outstanding),
        )
        outstanding2.toSet shouldBe Set(
          (period(1, 5), remoteId2, CommitmentPeriodState.Outstanding),
          (period(2, 5), remoteId, CommitmentPeriodState.Outstanding),
        )
        outstanding3.toSet shouldBe Set(
          (period(2, 5), remoteId, CommitmentPeriodState.Outstanding),
          (period(1, 2), remoteId2, CommitmentPeriodState.Outstanding),
          (period(3, 5), remoteId2, CommitmentPeriodState.Outstanding),
        )
        outstanding4.toSet shouldBe Set(
          (period(2, 4), remoteId, CommitmentPeriodState.Outstanding),
          (period(1, 2), remoteId2, CommitmentPeriodState.Outstanding),
          (period(3, 5), remoteId2, CommitmentPeriodState.Outstanding),
        )
        outstanding5.toSet shouldBe Set(
          (period(2, 4), remoteId, CommitmentPeriodState.Outstanding)
        )
        outstanding6.toSet shouldBe Set.empty
      }
    }

    "correctly compute matched periods in the outstanding table" in {
      val store = mk()
      for {
        outstanding <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        _ <- store.markOutstanding(period(1, 5), Set(remoteId, remoteId2))
        outstandingMarked <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
        _ <- store.markSafe(remoteId, period(1, 2), srip)
        _ <- store.markSafe(remoteId2, period(2, 3), srip)
        _ <- store.markSafe(remoteId, period(4, 5), srip)
        _ <- store.markSafe(remoteId2, period(1, 5), srip)
        _ <- store.markSafe(remoteId, period(2, 4), srip)
        outstandingAfter <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
      } yield {
        outstanding.toSet shouldBe Set.empty
        outstandingMarked.toSet shouldBe Set(
          (period(1, 5), remoteId, CommitmentPeriodState.Outstanding),
          (period(1, 5), remoteId2, CommitmentPeriodState.Outstanding),
        )
        outstandingAfter.toSet shouldBe Set(
          (period(1, 5), remoteId2, CommitmentPeriodState.Matched),
          (period(2, 4), remoteId, CommitmentPeriodState.Matched),
          (period(4, 5), remoteId, CommitmentPeriodState.Matched),
          (period(1, 2), remoteId, CommitmentPeriodState.Matched),
        )
      }
    }

    "correctly store surrounding commits in their correct state when marking period" in {
      val store = mk()
      for {
        outstanding <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        _ <- store.markOutstanding(period(1, 5), Set(remoteId))
        _ <- store.markUnsafe(remoteId, period(1, 5), srip)
        _ <- store.markSafe(remoteId, period(2, 4), srip)
        outstandingAfter <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
      } yield {
        outstanding.toSet shouldBe Set.empty
        outstandingAfter.toSet shouldBe Set(
          (period(1, 2), remoteId, CommitmentPeriodState.Mismatched),
          (period(2, 4), remoteId, CommitmentPeriodState.Matched),
          (period(4, 5), remoteId, CommitmentPeriodState.Mismatched),
        )
      }
    }

    "correctly perform state transition in the the outstanding table" in {
      val store = mk()
      for {
        outstanding <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        _ <- store.markOutstanding(period(1, 5), Set(remoteId))
        _ <- store.markUnsafe(remoteId, period(1, 5), srip)
        outstandingUnsafe <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
        // we are allowed to transition from state Mismatched to state Matched
        _ <- store.markSafe(remoteId, period(1, 5), srip)
        outstandingSafe <- store.outstanding(ts(0), ts(10), Seq.empty, includeMatchedPeriods = true)
        // we are not allowed to transition from state Matched to state Mismatch
        _ <- store.markUnsafe(remoteId, period(1, 5), srip)
        outstandingStillSafe <- store.outstanding(
          ts(0),
          ts(10),
          Seq.empty,
          includeMatchedPeriods = true,
        )
        _ <- store.markOutstanding(period(1, 5), Set(remoteId))
      } yield {
        outstanding.toSet shouldBe Set.empty
        outstandingUnsafe.toSet shouldBe Set(
          (period(1, 5), remoteId, CommitmentPeriodState.Mismatched)
        )
        outstandingSafe.toSet shouldBe Set(
          (period(1, 5), remoteId, CommitmentPeriodState.Matched)
        )
        outstandingStillSafe.toSet shouldBe Set(
          (period(1, 5), remoteId, CommitmentPeriodState.Matched)
        )
      }
    }
    /*
     This test is disabled for protocol versions for which the reconciliation interval is
     static because the described setting cannot occur.
     */
    "correctly compute outstanding commitments when intersection contains no tick" in {
      /*
        This copies the scenario of the test
        `work when commitment tick falls between two participants connection to the domain`
        in ACSCommitmentProcessorTest.

        We check that when markSafe yield an outstanding interval which contains no tick,
        then this "empty" interval is not inserted in the store.
       */
      val store = mk()

      lazy val sortedReconciliationIntervalsProvider: SortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(interval, domainBootstrappingTime = ts(6))

      for {
        outstanding0 <- store.outstanding(ts(0), ts(10))
        _ <- store.markOutstanding(period(0, 10), Set(remoteId))
        outstanding1 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(
          remoteId,
          period(5, 10),
          sortedReconciliationIntervalsProvider,
        )
        outstanding2 <- store.outstanding(ts(0), ts(10))
      } yield {
        outstanding0.toSet shouldBe Set.empty
        outstanding1.toSet shouldBe Set(
          (period(0, 10), remoteId, CommitmentPeriodState.Outstanding)
        )

        /*
          Period (0, 5) is not explicitly marked as safe but since it contains no tick
          (because domainBootstrapping=6), then it is dropped and we get an empty result.
         */
        outstanding2.toSet shouldBe Set.empty
      }
    }

    "correctly compute the no outstanding commitment limit" in {
      val store = mk()

      val endOfTime = ts(10)
      for {
        limit0 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markComputedAndSent(period(0, 2))
        limit1 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markOutstanding(period(2, 4), Set(remoteId, remoteId2))
        _ <- store.markComputedAndSent(period(2, 4))
        limit2 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, period(2, 3), srip)
        limit3 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId2, period(3, 4), srip)
        limit4 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId2, period(2, 3), srip)
        limit5 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, period(3, 4), srip)
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

    "correctly compute the no outstanding commitment limit with gaps in commitments" in {
      val store = mk()

      val endOfTime = ts(10)
      for {
        limit0 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markComputedAndSent(period(0, 2))
        limit1 <- store.noOutstandingCommitments(endOfTime)
        limit11 <- store.noOutstandingCommitments(ts(1))
        _ <- store.markOutstanding(period(2, 4), Set(remoteId, remoteId2))
        _ <- store.markComputedAndSent(period(2, 4))
        limit2 <- store.noOutstandingCommitments(endOfTime)
        limit21 <- store.noOutstandingCommitments(ts(2))
        limit22 <- store.noOutstandingCommitments(ts(3))
        limit23 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId, period(2, 3), srip)
        limit3 <- store.noOutstandingCommitments(endOfTime)
        limit31 <- store.noOutstandingCommitments(ts(2))
        limit32 <- store.noOutstandingCommitments(ts(3))
        limit33 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId2, period(3, 4), srip)
        limit4 <- store.noOutstandingCommitments(endOfTime)
        limit41 <- store.noOutstandingCommitments(ts(2))
        limit42 <- store.noOutstandingCommitments(ts(3))
        limit43 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId, period(3, 4), srip)
        limit5 <- store.noOutstandingCommitments(endOfTime)
        limit51 <- store.noOutstandingCommitments(ts(2))
        limit52 <- store.noOutstandingCommitments(ts(3))
        limit53 <- store.noOutstandingCommitments(ts(4))
        _ <- store.markSafe(remoteId2, period(2, 3), srip)
        limit6 <- store.noOutstandingCommitments(endOfTime)
        limit61 <- store.noOutstandingCommitments(ts(3))
        _ <- store.markOutstanding(period(4, 6), Set(remoteId, remoteId2))
        _ <- store.markComputedAndSent(period(4, 6))
        limit7 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markOutstanding(period(6, 10), Set())
        _ <- store.markComputedAndSent(period(6, 10))
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
        _ <- store.markComputedAndSent(period(0, 2))
        limit1 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markOutstanding(period(2, 5), Set(remoteId, remoteId2))
        _ <- store.markComputedAndSent(period(2, 5))
        limit2 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, period(2, 3), srip)
        _ <- store.markSafe(remoteId2, period(2, 3), srip)
        limit3 <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markSafe(remoteId, period(3, 4), srip)
        _ <- store.markUnsafe(remoteId2, period(3, 4), srip)
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
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId, period(0, 1), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId2, period(1, 2), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId, period(1, 2), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId, period(2, 3), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        found1 <- store.searchComputedBetween(ts(0), ts(1), Seq(remoteId))
        found2 <- store.searchComputedBetween(ts(0), ts(2))
        found3 <- store.searchComputedBetween(ts(1), ts(1))
        found4 <- store.searchComputedBetween(ts(0), ts(0))
        found5 <- store.searchComputedBetween(ts(2), ts(2), Seq(remoteId, remoteId2))
      } yield {
        found1.toSet shouldBe Set((period(0, 1), remoteId, dummyCommitment))
        found2.toSet shouldBe Set(
          (period(0, 1), remoteId, dummyCommitment),
          (period(1, 2), remoteId, dummyCommitment),
          (period(1, 2), remoteId2, dummyCommitment),
        )
        found3.toSet shouldBe Set((period(0, 1), remoteId, dummyCommitment))
        found4.toSet shouldBe Set.empty
        found5.toSet shouldBe Set(
          (period(1, 2), remoteId, dummyCommitment),
          (period(1, 2), remoteId2, dummyCommitment),
        )
      }
    }

    "correctly search stored remote commitment messages" in {
      val store = mk()

      val dummyMsg2 = AcsCommitment.create(
        domainId,
        remoteId,
        localId,
        period(2, 3),
        dummyCommitment,
        testedProtocolVersion,
      )
      val dummySigned2 =
        SignedProtocolMessage.from(dummyMsg2, testedProtocolVersion, dummySignature)
      val dummyMsg3 = AcsCommitment.create(
        domainId,
        remoteId2,
        localId,
        period(0, 1),
        dummyCommitment,
        testedProtocolVersion,
      )
      val dummySigned3 =
        SignedProtocolMessage.from(dummyMsg3, testedProtocolVersion, dummySignature)

      for {
        _ <- store.storeReceived(dummySigned)
        _ <- store.storeReceived(dummySigned2)
        _ <- store.storeReceived(dummySigned3)
        found1 <- store.searchReceivedBetween(ts(0), ts(1))
        found2 <- store.searchReceivedBetween(ts(0), ts(1), Seq(remoteId))
        found3 <- store.searchReceivedBetween(ts(0), ts(3), Seq(remoteId, remoteId2))
      } yield {
        found1.toSet shouldBe Set(dummySigned, dummySigned3)
        found2.toSet shouldBe Set(dummySigned)
        found3.toSet shouldBe Set(dummySigned, dummySigned2, dummySigned3)
      }
    }

    // currently it should only prune matching state, this might change in the future
    "correctly prune the outstanding table" in {
      val store = mk()

      val endOfTime = ts(10)
      for {
        start <- store.noOutstandingCommitments(endOfTime)
        _ <- store.markOutstanding(period(0, 10), Set(remoteId, remoteId2))
        _ <- store.markComputedAndSent(period(0, 10))

        _ <- store.markSafe(remoteId, period(0, 2), srip)
        _ <- store.markSafe(remoteId, period(2, 4), srip)
        _ <- store.markSafe(remoteId, period(4, 6), srip)

        _ <- store.markSafe(remoteId2, period(0, 2), srip)
        _ <- store.markUnsafe(remoteId2, period(2, 4), srip)
        _ <- store.markUnsafe(remoteId2, period(4, 6), srip)

        _ <- store.prune(ts(3))
        prune1 <- store.outstanding(ts(0), ts(10), includeMatchedPeriods = true)
        _ <- store.prune(ts(6))
        prune2 <- store.outstanding(ts(0), ts(10), includeMatchedPeriods = true)
      } yield {
        start shouldBe None
        prune1.toSet shouldBe Set(
          (period(2, 4), remoteId, CommitmentPeriodState.Matched),
          (period(4, 6), remoteId, CommitmentPeriodState.Matched),
          (period(6, 10), remoteId, CommitmentPeriodState.Outstanding),
          (period(2, 4), remoteId2, CommitmentPeriodState.Mismatched),
          (period(4, 6), remoteId2, CommitmentPeriodState.Mismatched),
          (period(6, 10), remoteId2, CommitmentPeriodState.Outstanding),
        )
        prune2.toSet shouldBe Set(
          (period(4, 6), remoteId, CommitmentPeriodState.Matched),
          (period(6, 10), remoteId, CommitmentPeriodState.Outstanding),
          (period(4, 6), remoteId2, CommitmentPeriodState.Mismatched),
          (period(6, 10), remoteId2, CommitmentPeriodState.Outstanding),
          (period(2, 4), remoteId2, CommitmentPeriodState.Mismatched),
        )
      }
    }

    "allow storing different remote commitment messages for the same period" in {
      val store = mk()

      val dummyMsg2 = AcsCommitment.create(
        domainId,
        remoteId,
        localId,
        period(0, 1),
        dummyCommitment2,
        testedProtocolVersion,
      )
      val dummySigned2 =
        SignedProtocolMessage.from(dummyMsg2, testedProtocolVersion, dummySignature)

      for {
        _ <- store.storeReceived(dummySigned)
        _ <- store.storeReceived(dummySigned2)
        found1 <- store.searchReceivedBetween(ts(0), ts(1))
      } yield {
        found1.toSet shouldBe Set(dummySigned, dummySigned2)
      }
    }

    "be idempotent when storing the same remote commitment messages for the same period" in {
      val store = mk()

      for {
        _ <- store.storeReceived(dummySigned)
        _ <- store.storeReceived(dummySigned)
        found1 <- store.searchReceivedBetween(ts(0), ts(1))
      } yield {
        found1.toList shouldBe List(dummySigned)
      }
    }

    "be idempotent when storing the same computed commitment messages" in {
      val store = mk()

      for {
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId, period(0, 1), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        _ <- NonEmpty
          .from(List(CommitmentData(remoteId, period(0, 1), dummyCommitment)))
          .fold(Future.unit)(store.storeComputed(_))
        found1 <- store.searchComputedBetween(ts(0), ts(1))
      } yield {
        found1.toList shouldBe List((period(0, 1), remoteId, dummyCommitment))
      }
    }

    "fails when storing different computed commitments for the same period and counter participant" in {
      val store = mk()

      loggerFactory.suppressWarningsAndErrors {
        recoverToSucceededIf[Throwable] {
          for {
            _ <- NonEmpty
              .from(List(CommitmentData(remoteId, period(0, 1), dummyCommitment)))
              .fold(Future.unit)(store.storeComputed(_))
            _ <- NonEmpty
              .from(List(CommitmentData(remoteId, period(0, 1), dummyCommitment2)))
              .fold(Future.unit)(store.storeComputed(_))
          } yield ()
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
        _ <- store.markOutstanding(period(0, 1), Set(remoteId))
        _ <- store.markOutstanding(period(0, 2), Set(remoteId))
        _ <- store.markSafe(remoteId, period(1, 2), srip)
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
        _ <- store.markOutstanding(period(0, 5), Set(remoteId, remoteId2))

        _ <- store.markSafe(remoteId, period(1, 2), srip)
        _ <- store.markUnsafe(remoteId2, period(1, 2), srip)
        outstanding1 <- store.outstanding(ts(0), ts(10))
        _ <- store.markSafe(remoteId, period(2, 3), srip)
        _ <- store.markUnsafe(remoteId2, period(2, 3), srip)
        outstanding2 <- store.outstanding(ts(0), ts(10))
        outstanding3 <- store.outstanding(ts(0), ts(10), includeMatchedPeriods = true)

      } yield {
        outstanding0.toSet shouldBe Set.empty
        outstanding1.toSet shouldBe Set(
          // remoteId & remoteId2 one still has period (0,1) outstanding
          (period(0, 1), remoteId, CommitmentPeriodState.Outstanding),
          (period(0, 1), remoteId2, CommitmentPeriodState.Outstanding),
          // remoteId2 has an outstanding period (1,2) in state mismatched
          (period(1, 2), remoteId2, CommitmentPeriodState.Mismatched),
          // remoteId & remoteId2 one still has period (2,5) outstanding
          (period(2, 5), remoteId, CommitmentPeriodState.Outstanding),
          (period(2, 5), remoteId2, CommitmentPeriodState.Outstanding),
        )
        outstanding2.toSet shouldBe Set(
          // remoteId & remoteId2 one still has period (0,1) outstanding
          (period(0, 1), remoteId, CommitmentPeriodState.Outstanding),
          (period(0, 1), remoteId2, CommitmentPeriodState.Outstanding),
          // remoteId2 has a period (1,2) & (2,3) that is mismatched
          (period(1, 2), remoteId2, CommitmentPeriodState.Mismatched),
          (period(2, 3), remoteId2, CommitmentPeriodState.Mismatched),
          // remoteId & remoteId2 one still has period (3,5) outstanding
          (period(3, 5), remoteId, CommitmentPeriodState.Outstanding),
          (period(3, 5), remoteId2, CommitmentPeriodState.Outstanding),
        )

        outstanding3.toSet shouldBe Set(
          // remoteId & remoteId2 one still has period (0,1) outstanding
          (period(0, 1), remoteId, CommitmentPeriodState.Outstanding),
          (period(0, 1), remoteId2, CommitmentPeriodState.Outstanding),
          // remoteId has a period (1,2) & (2,3) that is matched, but we are including matched periods
          (period(1, 2), remoteId, CommitmentPeriodState.Matched),
          (period(2, 3), remoteId, CommitmentPeriodState.Matched),
          // remoteId2 has a period (1,2) & (2,3) that is mismatched
          (period(1, 2), remoteId2, CommitmentPeriodState.Mismatched),
          (period(2, 3), remoteId2, CommitmentPeriodState.Mismatched),
          // remoteId & remoteId2 one still has period (3,5) outstanding
          (period(3, 5), remoteId, CommitmentPeriodState.Outstanding),
          (period(3, 5), remoteId2, CommitmentPeriodState.Outstanding),
        )
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
        domainId,
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

      for {
        _ <- queue.enqueue(c11)
        _ <- queue.enqueue(c11) // Idempotent enqueue
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
      }
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
          domainId,
          remoteId,
          localId,
          period(0, 5),
          dummyCommitment,
          testedProtocolVersion,
        )

      val dummyCommitmentMsg2 =
        AcsCommitment.create(
          domainId,
          remoteId,
          localId,
          period(10, 15),
          dummyCommitment2,
          testedProtocolVersion,
        )

      val dummyCommitmentMsg3 =
        AcsCommitment.create(
          domainId,
          remoteId,
          localId,
          period(0, 10),
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
        at05 <- queue.peekOverlapsForCounterParticipant(period(0, 5), remoteId)(
          nonEmptyTraceContext1
        )
        at010 <- queue.peekOverlapsForCounterParticipant(period(0, 10), remoteId)(
          nonEmptyTraceContext1
        )
        at510 <- queue.peekOverlapsForCounterParticipant(period(5, 10), remoteId)(
          nonEmptyTraceContext1
        )
        at1015 <- queue.peekOverlapsForCounterParticipant(period(10, 15), remoteId)(
          nonEmptyTraceContext1
        )
        _ <- queue.enqueue(c13)
        _ <- queue.enqueue(c22)
        at1015after <- queue.peekOverlapsForCounterParticipant(period(10, 15), remoteId)(
          nonEmptyTraceContext1
        )
        at510after <- queue.peekOverlapsForCounterParticipant(period(5, 10), remoteId)(
          nonEmptyTraceContext1
        )
        at515after <- queue.peekOverlapsForCounterParticipant(period(5, 15), remoteId)(
          nonEmptyTraceContext1
        )
        at015after <- queue.peekOverlapsForCounterParticipant(period(0, 15), remoteId)(
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
