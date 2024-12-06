// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.inspection

import cats.Eval
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  LtHash16,
  Signature,
  SigningPublicKey,
  SyncCryptoApiProvider,
  TestHash,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.pruning.SortedReconciliationIntervalsHelpers
import com.digitalasset.canton.participant.store.AcsCommitmentStore.CommitmentData
import com.digitalasset.canton.participant.store.db.DbAcsCommitmentStore
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  AcsCounterParticipantConfigStore,
  ParticipantNodePersistentState,
  SyncDomainPersistentState,
}
import com.digitalasset.canton.participant.sync.{
  ConnectedDomainsLookup,
  SyncDomainPersistentStateManager,
}
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  CommitmentPeriodState,
  DomainSearchCommitmentPeriod,
  ReceivedAcsCommitment,
  SentAcsCommitment,
  SignedProtocolMessage,
  ValidSentPeriodState,
}
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  BaseTest,
  CloseableTest,
  DomainAlias,
  FailOnShutdown,
  HasExecutionContext,
}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

sealed trait SyncStateInspectionTest
    extends AsyncWordSpec
    with Matchers
    with BaseTest
    with CloseableTest
    with SortedReconciliationIntervalsHelpers
    with FailOnShutdown
    with HasExecutionContext {
  this: DbTest =>

  override def cleanDb(storage: DbStorage)(implicit traceContext: TraceContext): Future[Unit] = {
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
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::domain")
  )
  lazy val remoteId: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant::domain")
  )
  lazy val remoteIdNESet: NonEmpty[Set[ParticipantId]] = NonEmptyUtil.fromElement(remoteId).toSet

  lazy val remoteId2: ParticipantId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::domain")
  )

  lazy val remoteId2NESet: NonEmpty[Set[ParticipantId]] = NonEmptyUtil.fromElement(remoteId2).toSet

  // values for domain1
  lazy val domainId: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::domain"))
  lazy val domainIdAlias: DomainAlias = DomainAlias.tryCreate("domain")
  lazy val indexedDomain: IndexedDomain = IndexedDomain.tryCreate(domainId, 1)
  // values for domain2
  lazy val domainId2: DomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::domain2"))
  lazy val domainId2Alias: DomainAlias = DomainAlias.tryCreate("domain2")
  lazy val indexedDomain2: IndexedDomain = IndexedDomain.tryCreate(domainId2, 2)

  def buildSyncState(): (SyncStateInspection, SyncDomainPersistentStateManager) = {
    val stateManager = mock[SyncDomainPersistentStateManager]
    val participantNodePersistentState = mock[ParticipantNodePersistentState]

    val syncStateInspection = new SyncStateInspection(
      stateManager,
      Eval.now(participantNodePersistentState),
      timeouts,
      JournalGarbageCollectorControl.NoOp,
      mock[ConnectedDomainsLookup],
      mock[SyncCryptoApiProvider],
      localId,
      futureSupervisor,
      loggerFactory,
    )
    (syncStateInspection, stateManager)
  }

  def AddDomainToSyncState(
      stateManager: SyncDomainPersistentStateManager,
      domainId: DomainId,
      domainAlias: DomainAlias,
  ): AcsCommitmentStore = {
    val persistentStateDomain = mock[SyncDomainPersistentState]
    val acsCounterParticipantConfigStore = mock[AcsCounterParticipantConfigStore]

    val acsCommitmentStore = new DbAcsCommitmentStore(
      storage,
      indexedDomain,
      acsCounterParticipantConfigStore,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )
    when(persistentStateDomain.acsCommitmentStore).thenReturn(acsCommitmentStore)
    when(stateManager.aliasForDomainId(domainId)).thenReturn(Some(domainAlias))
    when(stateManager.get(domainId)).thenReturn(Some(persistentStateDomain))
    when(stateManager.getByAlias(domainAlias)).thenReturn(Some(persistentStateDomain))

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
    * @param isVerbose (default: false) tell if hashes should be included.
    * @param isOwnDefault (default: true) tells if the generation should use default for its own hash.
    * @param isCounterDefault (default: true) tell if the generation should use default for its counters hash.
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
        .create(toInclusive - fromExclusive)
        .valueOrFail(s"could not convert ${toInclusive - fromExclusive} to PositiveSeconds"),
    )
  }

  /** This message creates a dummy commitment that can be persisted with store.storeReceived method.
    * It also returns the corresponding ReceivedAcsCommitment for comparison.
    * The ReceivedAcsCommitment is in state Outstanding.
    */
  def createDummyReceivedCommitment(
      domainId: DomainId,
      remoteParticipant: ParticipantId,
      commitmentPeriod: CommitmentPeriod,
      hashingState: HashingState = new HashingState(),
      state: CommitmentPeriodState = CommitmentPeriodState.Outstanding,
  ): (ReceivedAcsCommitment, SignedProtocolMessage[AcsCommitment]) = {

    val dummyCommitment = createDummyHash(hashingState.isOwnDefault)
    val dummySignature: Signature =
      symbolicCrypto.sign(
        symbolicCrypto.pureCrypto.digest(TestHash.testHashPurpose, dummyCommitment),
        testKey.id,
      )
    val dummyCommitmentMsg: AcsCommitment =
      AcsCommitment.create(
        domainId,
        remoteParticipant,
        localId,
        commitmentPeriod,
        dummyCommitment,
        testedProtocolVersion,
      )
    val signed =
      SignedProtocolMessage.from(dummyCommitmentMsg, testedProtocolVersion, dummySignature)
    val received = ReceivedAcsCommitment(
      domainId,
      commitmentPeriod,
      remoteParticipant,
      Option.when(hashingState.isVerbose)(dummyCommitment),
      Option.when(hashingState.isVerbose)(createDummyHash(hashingState.isCounterDefault)),
      state,
    )
    (received, signed)
  }

  /** This message creates a dummy commitment that can be persisted with store.storeComputed method.
    * It also returns the corresponding SentAcsCommitment for comparison.
    * The SentAcsCommitment is in state Outstanding.
    */
  def createDummyComputedCommitment(
      domainId: DomainId,
      counterParticipant: ParticipantId,
      period: CommitmentPeriod,
      hashingState: HashingState = new HashingState(),
      state: ValidSentPeriodState = CommitmentPeriodState.Outstanding,
  ): (SentAcsCommitment, AcsCommitmentStore.CommitmentData) = {

    val dummyCommitment = createDummyHash(hashingState.isOwnDefault)
    val commitmentData = CommitmentData(counterParticipant, period, dummyCommitment)
    val sent = SentAcsCommitment(
      domainId,
      period,
      counterParticipant,
      Option.when(hashingState.isVerbose)(dummyCommitment),
      Option.when(hashingState.isVerbose)(createDummyHash(hashingState.isCounterDefault)),
      state,
    )
    (sent, commitmentData)
  }

  lazy val intervalInt: Int = 1
  lazy val interval: PositiveSeconds = PositiveSeconds.tryOfSeconds(intervalInt.toLong)
  def ts(time: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(time.toLong)
  def period(fromExclusive: Int, toInclusive: Int): CommitmentPeriod = CommitmentPeriod
    .create(ts(fromExclusive), ts(toInclusive), interval)
    .valueOrFail(s"could not create period $fromExclusive -> $toInclusive")
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

  lazy val testKey: SigningPublicKey = symbolicCrypto.generateSymbolicSigningKey()

  "fetch empty sets if no domains exists" in {
    val (syncStateInspection, _) = buildSyncState()
    val crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    val crossDomainComputed = syncStateInspection.crossDomainSentCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set.empty
    crossDomainComputed.valueOrFail("crossDomainSent").toSet shouldBe Set.empty
  }

  "fetch empty sets if no commitments exists" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    val crossDomainComputed = syncStateInspection.crossDomainSentCommitmentMessages(
      Seq.empty,
      Seq.empty,
      Seq.empty,
      verbose = false,
    )
    crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set.empty
    crossDomainComputed.valueOrFail("crossDomainSent").toSet shouldBe Set.empty
  }

  "fetch a received commitment if it has been stored" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyCommitment) =
      createDummyReceivedCommitment(domainId, remoteId, testPeriod)
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      _ <- store.storeReceived(dummyCommitment)

      crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set(
      received
    )
  }

  "fetch a computed commitment if it has been computed" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (sent, dummyCommitment) =
      createDummyComputedCommitment(domainId, remoteId, testPeriod)
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      nonEmpty = NonEmpty
        .from(Seq(dummyCommitment))
        .getOrElse(throw new IllegalStateException("How is this empty?"))
      _ <- store.storeComputed(nonEmpty)

      crossDomainSent = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossDomainSent.valueOrFail("crossDomainSent").toSet shouldBe Set(sent)
  }

  "fetch matched received and computed commitments with hashes" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )
    val receivedHashingState = new HashingState(isVerbose = true)

    val (received, dummyRecCommitment) =
      createDummyReceivedCommitment(
        domainId,
        remoteId,
        testPeriod,
        receivedHashingState,
        CommitmentPeriodState.Matched,
      )
    val (sent, dummySentCommitment) =
      createDummyComputedCommitment(
        domainId,
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

      crossDomainSent = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = true,
      )
      crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = true,
      )
    } yield {
      crossDomainSent.valueOrFail("crossDomainSent").toSet shouldBe Set(sent)
      crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set(received)
    }
  }

  "fetch buffering commitments" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyCommitment) =
      createDummyReceivedCommitment(
        domainId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Buffered,
      )
    for {
      _ <- store.queue.enqueue(dummyCommitment.message).failOnShutdown

      crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set(received)
  }

  "only fetch requested domains" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val store2 = AddDomainToSyncState(stateManager, domainId2, domainId2Alias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyRecCommitment) =
      createDummyReceivedCommitment(
        domainId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sent, dummySentCommitment) =
      createDummyComputedCommitment(
        domainId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )

    val (_, dummyRecCommitment2) =
      createDummyReceivedCommitment(
        domainId2,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (_, dummySentCommitment2) =
      createDummyComputedCommitment(
        domainId2,
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

      crossDomainSent = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
      crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield {
      crossDomainSent.valueOrFail("crossDomainSent").toSet shouldBe Set(sent)
      crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set(received)
    }
  }

  "fetch requested counter participant from multiple domains" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val store2 = AddDomainToSyncState(stateManager, domainId2, domainId2Alias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )
    val domainSearchPeriod2 = DomainSearchCommitmentPeriod(
      indexedDomain2,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (received, dummyRecCommitment) =
      createDummyReceivedCommitment(
        domainId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sent, dummySentCommitment) =
      createDummyComputedCommitment(
        domainId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )

    val (received2, dummyRecCommitment2) =
      createDummyReceivedCommitment(
        domainId2,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sent2, dummySentCommitment2) =
      createDummyComputedCommitment(
        domainId2,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )

    val (_, dummyRecCommitmentTrap) =
      createDummyReceivedCommitment(
        domainId2,
        remoteId2,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (_, dummySentCommitmentTrap) =
      createDummyComputedCommitment(
        domainId2,
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

      crossDomainSent = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod, domainSearchPeriod2),
        Seq(remoteId),
        Seq.empty,
        verbose = false,
      )
      crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod, domainSearchPeriod2),
        Seq(remoteId),
        Seq.empty,
        verbose = false,
      )
    } yield {
      crossDomainSent.valueOrFail("crossDomainSent").toSet shouldBe Set(sent, sent2)
      crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set(received, received2)
    }
  }

  "only fetch requested states" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val testPeriod = period(1, 2) // period will be matches
    val testPeriod2 = period(2, 3) // period will be mismatched
    val testPeriod3 = period(3, 4) // period will be outstanding
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod3.toInclusive.forgetRefinement, // we want to cover all three periods above
    )
    val (receivedMatched, dummyRecCommitment) =
      createDummyReceivedCommitment(
        domainId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (sentMatched, dummySentCommitment) =
      createDummyComputedCommitment(
        domainId,
        remoteId,
        testPeriod,
        state = CommitmentPeriodState.Matched,
      )
    val (receivedMismatched, dummyRecCommitment2) =
      createDummyReceivedCommitment(
        domainId,
        remoteId,
        testPeriod2,
        state = CommitmentPeriodState.Mismatched,
      )
    val (sentMismatched, dummySentCommitment2) =
      createDummyComputedCommitment(
        domainId,
        remoteId,
        testPeriod2,
        state = CommitmentPeriodState.Mismatched,
      )
    val (sentOutstanding, dummySentCommitment3) =
      createDummyComputedCommitment(
        domainId,
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

      crossDomainSentMatched = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Matched),
        verbose = false,
      )
      crossDomainSentMismatched = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Mismatched),
        verbose = false,
      )
      crossDomainSentOutstanding = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Outstanding),
        verbose = false,
      )
      crossDomainAll = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(
          CommitmentPeriodState.Matched,
          CommitmentPeriodState.Mismatched,
          CommitmentPeriodState.Outstanding,
        ),
        verbose = false,
      )
      crossDomainReceivedMatched = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Matched),
        verbose = false,
      )
      crossDomainReceivedMismatched = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Mismatched),
        verbose = false,
      )
      crossDomainReceivedOutstanding = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(CommitmentPeriodState.Outstanding),
        verbose = false,
      )
      crossDomainReceivedAll = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq(
          CommitmentPeriodState.Matched,
          CommitmentPeriodState.Mismatched,
          CommitmentPeriodState.Outstanding,
        ),
        verbose = false,
      )
    } yield {
      crossDomainSentMatched.valueOrFail("crossDomainSent").toSet shouldBe Set(sentMatched)
      crossDomainSentMismatched.valueOrFail("crossDomainSent").toSet shouldBe Set(sentMismatched)
      crossDomainSentOutstanding.valueOrFail("crossDomainSent").toSet shouldBe Set(sentOutstanding)
      crossDomainAll.valueOrFail("crossDomainSent").toSet shouldBe Set(
        sentMatched,
        sentMismatched,
        sentOutstanding,
      )
      crossDomainReceivedMatched.valueOrFail("crossDomainReceived").toSet shouldBe Set(
        receivedMatched
      )
      crossDomainReceivedMismatched.valueOrFail("crossDomainReceived").toSet shouldBe Set(
        receivedMismatched
      )
      crossDomainReceivedOutstanding.valueOrFail("crossDomainReceived").toSet shouldBe Set.empty
      crossDomainReceivedAll.valueOrFail("crossDomainReceived").toSet shouldBe Set(
        receivedMatched,
        receivedMismatched,
      )
    }
  }
  "should fetch latest iteration if called with lastComputedAndSent" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.toInclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )
    val (sentMatched, dummySentCommitment) =
      createDummyComputedCommitment(
        domainId,
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

      crossDomainReceived = syncStateInspection.crossDomainSentCommitmentMessages(
        Seq(domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossDomainReceived.valueOrFail("crossDomainReceived").toSet shouldBe Set(sentMatched)
  }

  "not include duplicates with overlapping time periods" in {
    val (syncStateInspection, stateManager) = buildSyncState()
    val store = AddDomainToSyncState(stateManager, domainId, domainIdAlias)
    val testPeriod = period(1, 2)
    val domainSearchPeriod = DomainSearchCommitmentPeriod(
      indexedDomain,
      testPeriod.fromExclusive.forgetRefinement,
      testPeriod.toInclusive.forgetRefinement,
    )

    val (_, dummyCommitment) =
      createDummyReceivedCommitment(domainId, remoteId, testPeriod)
    for {
      _ <- store.markOutstanding(NonEmptyUtil.fromElement(testPeriod), remoteIdNESet)
      _ <- store.storeReceived(dummyCommitment)

      crossDomainReceived = syncStateInspection.crossDomainReceivedCommitmentMessages(
        Seq(domainSearchPeriod, domainSearchPeriod),
        Seq.empty,
        Seq.empty,
        verbose = false,
      )
    } yield crossDomainReceived
      .valueOrFail("crossDomainReceived")
      .size shouldBe 1 // we cant use toSet here since it removes duplicates
  }
}

class SyncStateInspectionTestPostgres extends SyncStateInspectionTest with PostgresTest
//class SyncStateInspectionTestH2 extends SyncStateInspectionTest with H2Test
