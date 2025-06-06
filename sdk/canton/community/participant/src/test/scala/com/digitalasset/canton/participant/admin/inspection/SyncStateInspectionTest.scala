// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  LtHash16,
  Signature,
  SigningKeyUsage,
  SigningPublicKey,
  SyncCryptoApiParticipantProvider,
  TestHash,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsHelpers
import com.digitalasset.canton.participant.store.AcsCommitmentStore.ParticipantCommitmentData
import com.digitalasset.canton.participant.store.db.DbAcsCommitmentStore
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  AcsCounterParticipantConfigStore,
  ParticipantNodePersistentState,
  SyncPersistentState,
}
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncPersistentStateManager,
}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedSynchronizer
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{
  ParticipantId,
  PhysicalSynchronizerId,
  SynchronizerId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  CloseableTest,
  FailOnShutdown,
  HasExecutionContext,
  SynchronizerAlias,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

sealed trait SyncStateInspectionTest
    extends AsyncWordSpec
    with Matchers
    with BaseTest
    with CloseableTest
    with SortedReconciliationIntervalsHelpers
    with FailOnShutdown
    with HasExecutionContext {
  this: DbTest =>

  override def cleanDb(
      storage: DbStorage
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    import storage.api.*
    storage.update(
      DBIO.seq(
        sqlu"truncate table par_computed_acs_commitments",
        sqlu"truncate table par_received_acs_commitments",
        sqlu"truncate table par_outstanding_acs_commitments",
        sqlu"truncate table par_last_computed_acs_commitments",
        sqlu"truncate table par_commitment_pruning",
        sqlu"truncate table par_commitment_queue",
      ),
      functionFullName,
    )
  }

  private lazy val localId: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::synchronizer")
  )
  lazy val remoteId: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant::synchronizer")
  )
  lazy val remoteIdNESet: NonEmpty[Set[ParticipantId]] = NonEmptyUtil.fromElement(remoteId).toSet

  lazy val remoteId2: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::synchronizer")
  )

  lazy val remoteId2NESet: NonEmpty[Set[ParticipantId]] = NonEmptyUtil.fromElement(remoteId2).toSet

  // values for synchronizer1
  lazy val synchronizerId: PhysicalSynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::synchronizer")
  ).toPhysical
  lazy val synchronizerIdAlias: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer")
  lazy val indexedSynchronizer: IndexedSynchronizer =
    IndexedSynchronizer.tryCreate(synchronizerId, 1)
  // values for synchronizer2
  lazy val synchronizerId2: PhysicalSynchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::synchronizer2")
  ).toPhysical
  lazy val synchronizerId2Alias: SynchronizerAlias = SynchronizerAlias.tryCreate("synchronizer2")
  lazy val indexedSynchronizer2: IndexedSynchronizer =
    IndexedSynchronizer.tryCreate(synchronizerId2, 2)

  def buildSyncState(): (SyncStateInspection, SyncPersistentStateManager) = {
    val stateManager = mock[SyncPersistentStateManager]
    val participantNodePersistentState = mock[ParticipantNodePersistentState]

    val syncStateInspection = new SyncStateInspection(
      stateManager,
      Eval.now(participantNodePersistentState),
      timeouts,
      JournalGarbageCollectorControl.NoOp,
      mock[ConnectedSynchronizersLookup],
      mock[SyncCryptoApiParticipantProvider],
      localId,
      futureSupervisor,
      loggerFactory,
    )
    (syncStateInspection, stateManager)
  }

  protected def addSynchronizerToSyncState(
      stateManager: SyncPersistentStateManager,
      synchronizerId: SynchronizerId,
      synchronizerAlias: SynchronizerAlias,
  ): AcsCommitmentStore = {
    val syncPersistentState = mock[SyncPersistentState]
    val acsCounterParticipantConfigStore = mock[AcsCounterParticipantConfigStore]

    val acsCommitmentStore = new DbAcsCommitmentStore(
      storage,
      indexedSynchronizer,
      acsCounterParticipantConfigStore,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )
    when(syncPersistentState.acsCommitmentStore).thenReturn(acsCommitmentStore)
    when(stateManager.aliasForSynchronizerId(synchronizerId)).thenReturn(Some(synchronizerAlias))
    when(stateManager.get(synchronizerId)).thenReturn(Some(syncPersistentState))
    when(stateManager.getByAlias(synchronizerAlias)).thenReturn(Some(syncPersistentState))

    acsCommitmentStore
  }

  def createDummyHash(default: Boolean = true): AcsCommitment.CommitmentType = {
    val h = LtHash16()
    h.add("blah".getBytes())
    if (!default)
      h.add("a bit less blah".getBytes())
    h.getByteString()
  }

  /** a small class to define the hashes between received and sent.
    *
    * @param isVerbose
    *   (default: false) tell if hashes should be included.
    * @param isOwnDefault
    *   (default: true) tells if the generation should use default for its own hash.
    * @param isCounterDefault
    *   (default: true) tell if the generation should use default for its counters hash.
    *
    * you can use the inverse() to build the corresponding HashingState for the other dummy
    */
  class HashingState(
      val isVerbose: Boolean = false,
      val isOwnDefault: Boolean = true,
      val isCounterDefault: Boolean = true,
  ) {
    def inverse(): HashingState = new HashingState(
      isVerbose = this.isVerbose,
      isOwnDefault = this.isCounterDefault,
      isCounterDefault = this.isOwnDefault,
    )
  }

  def deriveFullPeriod(commitmentPeriods: NonEmpty[Set[CommitmentPeriod]]): CommitmentPeriod = {
    val fromExclusive = commitmentPeriods.minBy1(_.fromExclusive).fromExclusive
    val toInclusive = commitmentPeriods.maxBy1(_.toInclusive).toInclusive
    new CommitmentPeriod(
      fromExclusive,
      PositiveSeconds
        .create(toInclusive - fromExclusive) value,
    )
  }

  /** This message creates a dummy commitment that can be persisted with store.storeReceived method.
    * It also returns the corresponding ReceivedAcsCommitment for comparison. The
    * ReceivedAcsCommitment is in state Outstanding.
    */
  def createDummyReceivedCommitment(
      synchronizerId: PhysicalSynchronizerId,
      remoteParticipant: ParticipantId,
      commitmentPeriod: CommitmentPeriod,
      hashingState: HashingState = new HashingState(),
      state: CommitmentPeriodState = CommitmentPeriodState.Outstanding,
  ): (ReceivedAcsCommitment, SignedProtocolMessage[AcsCommitment]) = {

    val dummyCommitment = createDummyHash(hashingState.isOwnDefault)
    val hashedDummyCommitment: AcsCommitment.HashedCommitmentType =
      AcsCommitment.hashCommitment(dummyCommitment)
    val dummyCounterCommitment = createDummyHash(hashingState.isCounterDefault)
    val hashedDummyCounterCommitment: AcsCommitment.HashedCommitmentType =
      AcsCommitment.hashCommitment(dummyCounterCommitment)

    val dummySignature: Signature =
      symbolicCrypto.sign(
        symbolicCrypto.pureCrypto.digest(TestHash.testHashPurpose, dummyCommitment),
        testKey.id,
        SigningKeyUsage.ProtocolOnly,
      )
    val dummyCommitmentMsg: AcsCommitment =
      AcsCommitment.create(
        synchronizerId,
        remoteParticipant,
        localId,
        commitmentPeriod,
        dummyCommitment,
        testedProtocolVersion,
      )
    val signed =
      SignedProtocolMessage.from(dummyCommitmentMsg, testedProtocolVersion, dummySignature)
    val received = ReceivedAcsCommitment(
      synchronizerId,
      commitmentPeriod,
      remoteParticipant,
      Option.when(hashingState.isVerbose)(hashedDummyCommitment),
      Option.when(hashingState.isVerbose)(hashedDummyCounterCommitment),
      state,
    )
    (received, signed)
  }

  /** This message creates a dummy commitment that can be persisted with store.storeComputed method.
    * It also returns the corresponding SentAcsCommitment for comparison. The SentAcsCommitment is
    * in state Outstanding.
    */
  def createDummyComputedCommitment(
      synchronizerId: SynchronizerId,
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      hashingState: HashingState = new HashingState(),
      state: ValidSentPeriodState = CommitmentPeriodState.Outstanding,
  ): (SentAcsCommitment, AcsCommitmentStore.ParticipantCommitmentData) = {

    val dummyCommitment = createDummyHash(hashingState.isOwnDefault)
    val hashedDummyCommitment = AcsCommitment.hashCommitment(dummyCommitment)
    val dummyCounterCommitment = createDummyHash(hashingState.isCounterDefault)
    val hashedDummyCounterCommitment = AcsCommitment.hashCommitment(dummyCounterCommitment)
    val commitmentData =
      ParticipantCommitmentData(counterParticipant, period, hashedDummyCommitment)
    val sent = SentAcsCommitment(
      synchronizerId,
      period,
      counterParticipant,
      Option.when(hashingState.isVerbose)(hashedDummyCommitment),
      Option.when(hashingState.isVerbose)(hashedDummyCounterCommitment),
      state,
    )
    (sent, commitmentData)
  }

  lazy val intervalInt: Int = 1
  lazy val interval: PositiveSeconds = PositiveSeconds.tryOfSeconds(intervalInt.toLong)
  def ts(time: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(time.toLong)
  def period(fromExclusive: Int, toInclusive: Int): CommitmentPeriod = CommitmentPeriod
    .create(ts(fromExclusive), ts(toInclusive), interval)
    .value

  def periods(fromExclusive: Int, toInclusive: Int): NonEmpty[Set[CommitmentPeriod]] =
    NonEmptyUtil
      .fromUnsafe(
        (fromExclusive until toInclusive by intervalInt).map { i =>
          CommitmentPeriod.create(ts(i), ts(i + intervalInt), interval).value
        }
      )
      .toSet

  lazy val symbolicCrypto: SymbolicCrypto = SymbolicCrypto.create(
    testedReleaseProtocolVersion,
    timeouts,
    loggerFactory,
  )

  lazy val testKey: SigningPublicKey =
    symbolicCrypto.generateSymbolicSigningKey(usage = SigningKeyUsage.ProtocolOnly)

  "fetch empty sets if no synchronizers exists" in {
    val (syncStateInspection, _) = buildSyncState()
    val crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    val crossSynchronizerComputed = syncStateInspection.crossSynchronizerSentCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    crossSynchronizerReceived.value.toSet shouldBe Set.empty
    crossSynchronizerComputed.value.toSet shouldBe Set.empty
  }

  "fetch empty sets if no commitments exists" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    val crossSynchronizerComputed = syncStateInspection.crossSynchronizerSentCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    crossSynchronizerReceived.value.toSet shouldBe Set.empty
    crossSynchronizerComputed.value.toSet shouldBe Set.empty
  }

  "fetch a received commitment if it has been stored" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyCommitment) =
      createDummyReceivedCommitment(synchronizerId, remoteId, testPeriod)
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      _ <- store.storeReceived(dummyCommitment)

      crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossSynchronizerReceived.value.toSet shouldBe Set(
      received
    )
  }

  "fetch a computed commitment if it has been computed" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (sent, dummyCommitment) =
      createDummyComputedCommitment(synchronizerId, remoteId, testPeriod)
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty = NonEmpty
        .from(Seq(dummyCommitment))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store.storeComputed(nonEmpty)

      crossSynchronizerSent = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossSynchronizerSent.value.toSet shouldBe Set(sent)
  }

  "fetch matched received and computed commitments with hashes" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )
    val receivedHashingState = new HashingState(isVerbose = true)

    val (received, dummyRecCommitment) =
      createDummyReceivedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        receivedHashingState,
        CommitmentPeriodState.Matched,
      )
    val (sent, dummySentCommitment) =
      createDummyComputedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        receivedHashingState.inverse(),
        CommitmentPeriodState.Matched,
      )

    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty = NonEmpty
        .from(Seq(dummySentCommitment))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store.storeComputed(nonEmpty)
      _ <- store.storeReceived(dummyRecCommitment)
      _ <- store.markSafe(remoteId, NonEmptyUtil.fromElement(testPeriod))

      crossSynchronizerSent = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = true,
      )
      crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = true,
      )
    } yield {
      crossSynchronizerSent.value.toSet shouldBe Set(sent)
      crossSynchronizerReceived.value.toSet shouldBe Set(
        received
      )
    }
  }

  "fetch buffering commitments" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyCommitment) =
      createDummyReceivedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Buffered,
      )
    for {
      _ <- store.queue.enqueue(dummyCommitment.message).failOnShutdown

      crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossSynchronizerReceived.value.toSet shouldBe Set(
      received
    )
  }

  "only fetch requested synchronizers" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val store2 = addSynchronizerToSyncState(stateManager, synchronizerId2, synchronizerId2Alias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyRecCommitment) =
      createDummyReceivedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sent, dummySentCommitment) =
      createDummyComputedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )

    val (_, dummyRecCommitment2) =
      createDummyReceivedCommitment(
        synchronizerId2,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (_, dummySentCommitment2) =
      createDummyComputedCommitment(
        synchronizerId2,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )

    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty = NonEmpty
        .from(Seq(dummySentCommitment))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store.storeComputed(nonEmpty)
      _ <- store.storeReceived(dummyRecCommitment)
      _ <- store.markSafe(remoteId, NonEmptyUtil.fromElement(testPeriod))

      // same thing, but for the second store
      _ <- store2.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty2 = NonEmpty
        .from(Seq(dummySentCommitment2))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store2.storeComputed(nonEmpty2)
      _ <- store2.storeReceived(dummyRecCommitment2)
      _ <- store2.markSafe(remoteId, NonEmptyUtil.fromElement(testPeriod))

      crossSynchronizerSent = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
      crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield {
      crossSynchronizerSent.value.toSet shouldBe Set(sent)
      crossSynchronizerReceived.value.toSet shouldBe Set(
        received
      )
    }
  }

  "fetch requested counter participant from multiple synchronizers" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val store2 = addSynchronizerToSyncState(stateManager, synchronizerId2, synchronizerId2Alias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )
    val synchronizerSearchPeriod2 = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer2,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyRecCommitment) =
      createDummyReceivedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sent, dummySentCommitment) =
      createDummyComputedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )

    val (received2, dummyRecCommitment2) =
      createDummyReceivedCommitment(
        synchronizerId2,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sent2, dummySentCommitment2) =
      createDummyComputedCommitment(
        synchronizerId2,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )

    val (_, dummyRecCommitmentTrap) =
      createDummyReceivedCommitment(
        synchronizerId2,
        remoteId2,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (_, dummySentCommitmentTrap) =
      createDummyComputedCommitment(
        synchronizerId2,
        remoteId2,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty = NonEmpty
        .from(Seq(dummySentCommitment))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store.storeComputed(nonEmpty)
      _ <- store.storeReceived(dummyRecCommitment)
      _ <- store.markSafe(remoteId, NonEmptyUtil.fromElement(testPeriod))

      // same thing, but for the second store
      _ <- store2.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty2 = NonEmpty
        .from(Seq(dummySentCommitment2))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store2.storeComputed(nonEmpty2)
      _ <- store2.storeReceived(dummyRecCommitment2)
      _ <- store2.markSafe(remoteId, NonEmptyUtil.fromElement(testPeriod))

      // introduce commitments for remoteId2, since we filter by remoteId then these should not appear
      _ <- store2.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteId2NESet)
      nonEmptyTrap = NonEmpty
        .from(Seq(dummySentCommitmentTrap))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store2.storeComputed(nonEmptyTrap)
      _ <- store2.storeReceived(dummyRecCommitmentTrap)
      _ <- store2.markSafe(remoteId2, NonEmptyUtil.fromElement(testPeriod))

      crossSynchronizerSent = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod, synchronizerSearchPeriod2),
        Seq(remoteId),
        Seq.empty,
        verbose = false,
      )
      crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
        Seq(synchronizerSearchPeriod, synchronizerSearchPeriod2),
        Seq(remoteId),
        Seq.empty,
        verbose = false,
      )
    } yield {
      crossSynchronizerSent.value.toSet shouldBe Set(sent, sent2)
      crossSynchronizerReceived.value.toSet shouldBe Set(
        received,
        received2,
      )
    }
  }

  "only fetch requested states" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val testPeriod = period(1, 2) // period will be matches
    val testPeriod2 = period(2, 3) // period will be mismatched
    val testPeriod3 = period(3, 4) // period will be outstanding
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod3.toInclusive.forgetRefinement, // we want to cover all three periods above
    )
    val (receivedMatched, dummyRecCommitment) =
      createDummyReceivedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sentMatched, dummySentCommitment) =
      createDummyComputedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (receivedMismatched, dummyRecCommitment2) =
      createDummyReceivedCommitment(
        synchronizerId,
        remoteId,
        testPeriod2,
        state = CommitmentPeriodState.Mismatched,
      )
    val (sentMismatched, dummySentCommitment2) =
      createDummyComputedCommitment(
        synchronizerId,
        remoteId,
        testPeriod2,
        state = CommitmentPeriodState.Mismatched,
      )
    val (sentOutstanding, dummySentCommitment3) =
      createDummyComputedCommitment(
        synchronizerId,
        remoteId,
        testPeriod3,
        state = CommitmentPeriodState.Outstanding,
      )

    for {
      _ <- store.markOutstanding(periods(1, 4), remoteIdNESet)
      nonEmpty = NonEmptyUtil
        .fromUnsafe(Seq(dummySentCommitment, dummySentCommitment2, dummySentCommitment3))
      _ <- store.storeComputed(nonEmpty)
      _ <- store.storeReceived(dummyRecCommitment)
      _ <- store.storeReceived(dummyRecCommitment2)

      // test period 1 is matched
      _ <- store.markSafe(remoteId, NonEmptyUtil.fromElement(testPeriod))
      // test period 2 is mismatched
      _ <- store.markUnsafe(remoteId, NonEmptyUtil.fromElement(testPeriod2))

      crossSynchronizerSentMatched = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Matched),
        verbose = false,
      )
      crossSynchronizerSentMismatched = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Mismatched),
        verbose = false,
      )
      crossSynchronizerSentOutstanding = syncStateInspection
        .crossSynchronizerSentCommitmentMessages(
          Seq(synchronizerSearchPeriod),
          Seq.empty,
          Seq(CommitmentPeriodState.Outstanding),
          verbose = false,
        )
      crossSynchronizerAll = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq(
          CommitmentPeriodState.Matched,
          CommitmentPeriodState.Mismatched,
          CommitmentPeriodState.Outstanding,
        ),
        verbose = false,
      )
      crossSynchronizerReceivedMatched = syncStateInspection
        .crossSynchronizerReceivedCommitmentMessages(
          Seq(synchronizerSearchPeriod),
          Seq.empty,
          Seq(CommitmentPeriodState.Matched),
          verbose = false,
        )
      crossSynchronizerReceivedMismatched = syncStateInspection
        .crossSynchronizerReceivedCommitmentMessages(
          Seq(synchronizerSearchPeriod),
          Seq.empty,
          Seq(CommitmentPeriodState.Mismatched),
          verbose = false,
        )
      crossSynchronizerReceivedOutstanding = syncStateInspection
        .crossSynchronizerReceivedCommitmentMessages(
          Seq(synchronizerSearchPeriod),
          Seq.empty,
          Seq(CommitmentPeriodState.Outstanding),
          verbose = false,
        )
      crossSynchronizerReceivedAll = syncStateInspection
        .crossSynchronizerReceivedCommitmentMessages(
          Seq(synchronizerSearchPeriod),
          Seq.empty,
          Seq(
            CommitmentPeriodState.Matched,
            CommitmentPeriodState.Mismatched,
            CommitmentPeriodState.Outstanding,
          ),
          verbose = false,
        )
    } yield {
      crossSynchronizerSentMatched.value.toSet shouldBe Set(sentMatched)
      crossSynchronizerSentMismatched.value.toSet shouldBe Set(sentMismatched)
      crossSynchronizerSentOutstanding.value.toSet shouldBe Set(sentOutstanding)
      crossSynchronizerAll.value.toSet shouldBe Set(
        sentMatched,
        sentMismatched,
        sentOutstanding,
      )
      crossSynchronizerReceivedMatched.value.toSet shouldBe Set(
        receivedMatched
      )
      crossSynchronizerReceivedMismatched.value.toSet shouldBe Set(
        receivedMismatched
      )
      crossSynchronizerReceivedOutstanding.value.toSet shouldBe Set.empty
      crossSynchronizerReceivedAll.value.toSet shouldBe Set(
        receivedMatched,
        receivedMismatched,
      )
    }
  }
  "should fetch latest iteration if called with lastComputedAndSent" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.toInclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )
    val (sentMatched, dummySentCommitment) =
      createDummyComputedCommitment(
        synchronizerId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Outstanding,
      )
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty = NonEmpty
        .from(Seq(dummySentCommitment))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store.storeComputed(nonEmpty)

      crossSynchronizerReceived = syncStateInspection.crossSynchronizerSentCommitmentMessages(
        Seq(synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossSynchronizerReceived.value.toSet shouldBe Set(sentMatched)
  }

  "not include duplicates with overlapping time periods" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = addSynchronizerToSyncState(stateManager, synchronizerId, synchronizerIdAlias)
    val testPeriod = period(1, 2)
    val synchronizerSearchPeriod = SynchronizerSearchCommitmentPeriod(
      indexedSynchronizer,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (_, dummyCommitment) =
      createDummyReceivedCommitment(synchronizerId, remoteId, testPeriod)
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      _ <- store.storeReceived(dummyCommitment)

      crossSynchronizerReceived = syncStateInspection.crossSynchronizerReceivedCommitmentMessages(
        Seq(synchronizerSearchPeriod, synchronizerSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossSynchronizerReceived.value.size shouldBe 1 // we cant use toSet here since it removes duplicates
  }
}

class SyncStateInspectionTestPostgres extends SyncStateInspectionTest with PostgresTest
//class SyncStateInspectionTestH2 extends SyncStateInspectionTest with H2Test
