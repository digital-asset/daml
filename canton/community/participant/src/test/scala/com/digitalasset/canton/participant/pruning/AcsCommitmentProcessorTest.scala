// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveNumeric}
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, NonNegativeDuration}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.participant.event.{AcsChange, ContractStakeholders, RecordTime}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet.{
  ArchivalCommit,
  CreationCommit,
  TransferInCommit,
  TransferOutCommit,
}
import com.digitalasset.canton.participant.protocol.submission.*
import com.digitalasset.canton.participant.pruning
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.Errors.DegradationError.{
  AcsCommitmentDegradation,
  AcsCommitmentDegradationWithIneffectiveConfig,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.{
  CachedCommitments,
  CommitmentSnapshot,
  CommitmentsPruningBound,
  RunningCommitments,
  commitmentsFromStkhdCmts,
  computeCommitmentsPerParticipant,
  emptyCommitment,
  initRunningCommitments,
}
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.*
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.CursorPrehead
import com.digitalasset.canton.store.memory.InMemorySequencerCounterTrackerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.{HasTestCloseContext, ProtocolVersion}
import org.scalatest.Assertion
import org.scalatest.wordspec.{AnyWordSpec, AsyncWordSpec}

import java.time.Duration as JDuration
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.{Seq, SortedSet}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

@nowarn("msg=match may not be exhaustive")
sealed trait AcsCommitmentProcessorBaseTest
    extends BaseTest
    with SortedReconciliationIntervalsHelpers
    with HasTestCloseContext {

  protected val interval = PositiveSeconds.tryOfSeconds(5)
  protected val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da"))
  protected val localId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::domain")
  )
  protected val remoteId1 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant1::domain")
  )
  protected val remoteId2 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::domain")
  )
  protected val remoteId3 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant3::domain")
  )

  protected val List(alice, bob, carol, danna, ed) =
    List("Alice::1", "Bob::2", "Carol::3", "Danna::4", "Ed::5").map(LfPartyId.assertFromString)

  protected val topology = Map(
    localId -> Set(alice),
    remoteId1 -> Set(bob),
    remoteId2 -> Set(carol, danna, ed),
  )

  protected lazy val dummyCmt = {
    val h = LtHash16()
    h.add("123".getBytes())
    h.getByteString()
  }

  protected def ts(i: CantonTimestamp): CantonTimestampSecond =
    CantonTimestampSecond.ofEpochSecond(i.getEpochSecond)

  protected def ts(i: Int): CantonTimestampSecond = CantonTimestampSecond.ofEpochSecond(i.longValue)

  protected def toc(timestamp: Int, requestCounter: Int = 0): TimeOfChange =
    TimeOfChange(RequestCounter(requestCounter), ts(timestamp).forgetRefinement)

  protected def mkChangeIdHash(index: Int) = ChangeIdHash(DefaultDamlValues.lfhash(index))

  protected def acsSetup(
      contracts: Map[LfContractId, NonEmpty[Seq[Lifespan]]]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[ActiveContractSnapshot] = {
    val acs = new InMemoryActiveContractStore(testedProtocolVersion, loggerFactory)
    contracts.toList
      .flatMap { case (cid, seq) => seq.forgetNE.map(lifespan => (cid, lifespan)) }
      .parTraverse_ { case (cid, lifespan) =>
        for {
          _ <- acs
            .markContractActive(
              cid,
              TimeOfChange(RequestCounter(0), lifespan.createdTs),
            )
            .value
          _ <- acs
            .archiveContract(
              cid,
              TimeOfChange(RequestCounter(0), lifespan.archivedTs),
            )
            .value
        } yield ()
      }
      .map(_ => acs)
  }

  protected def cryptoSetup(
      owner: ParticipantId,
      topology: Map[ParticipantId, Set[LfPartyId]],
      dynamicDomainParametersWithValidity: List[
        DomainParameters.WithValidity[DynamicDomainParameters]
      ] = List.empty,
  ): SyncCryptoClient[DomainSnapshotSyncCryptoApi] = {

    val topologyWithPermissions =
      topology.fmap(_.map(p => (p, ParticipantPermission.Submission)).toMap)

    val testingTopology = dynamicDomainParametersWithValidity match {
      // this way we get default values for an empty List
      case Nil => TestingTopology()
      case _ => TestingTopology(domainParameters = dynamicDomainParametersWithValidity)
    }
    testingTopology
      .withReversedTopology(topologyWithPermissions)
      .build()
      .forOwnerAndDomain(owner)
  }

  protected def changesAtToc(
      contractSetup: Map[
        LfContractId,
        (Set[LfPartyId], TimeOfChange, TimeOfChange),
      ]
  )(toc: TimeOfChange): (CantonTimestamp, RequestCounter, AcsChange) = {
    (
      toc.timestamp,
      toc.rc,
      contractSetup.foldLeft(AcsChange.empty) {
        case (
              acsChange,
              (
                cid,
                (stkhs, creationToc, archivalToc),
              ),
            ) =>
          val metadata = ContractMetadata.tryCreate(Set.empty, stkhs, None)
          AcsChange(
            deactivations = acsChange.deactivations ++ (if (archivalToc == toc)
                                                          Map(
                                                            cid -> withTestHash(
                                                              ContractStakeholders(stkhs)
                                                            )
                                                          )
                                                        else Map.empty),
            activations = acsChange.activations ++ (if (creationToc == toc)
                                                      Map(cid -> withTestHash(metadata))
                                                    else Map.empty),
          )
      },
    )
  }

  // Create the processor, but return the changes instead of publishing them, such that the user can decide when
  // to publish
  protected def testSetupDontPublish(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[
        LfContractId,
        (Set[LfPartyId], TimeOfChange, TimeOfChange),
      ],
      topology: Map[ParticipantId, Set[LfPartyId]],
      optCommitmentStore: Option[AcsCommitmentStore] = None,
      overrideDefaultSortedReconciliationIntervalsProvider: Option[
        SortedReconciliationIntervalsProvider
      ] = None,
      catchUpModeEnabled: Boolean = false,
      domainParametersUpdates: List[DomainParameters.WithValidity[DynamicDomainParameters]] =
        List.empty,
      reconciliationIntervalsUpdates: List[DynamicDomainParametersWithValidity] = List.empty,
  )(implicit ec: ExecutionContext): (
      AcsCommitmentProcessor,
      AcsCommitmentStore,
      SequencerClient,
      List[(CantonTimestamp, RequestCounter, AcsChange)],
  ) = {

    val catchUpConfig =
      if (catchUpModeEnabled)
        Some(CatchUpConfig(PositiveInt.tryCreate(2), PositiveInt.tryCreate(1)))
      else None

    val domainCrypto =
      cryptoSetup(
        localId,
        topology,
        domainParametersUpdates.appended(
          DomainParameters.WithValidity(
            validFrom = CantonTimestamp.MinValue,
            validUntil = domainParametersUpdates
              .sortBy(_.validFrom)
              .headOption
              .fold(Some(CantonTimestamp.MaxValue))(param => Some(param.validFrom)),
            parameter = defaultParameters.tryUpdate(catchUpConfig = catchUpConfig),
          )
        ),
      )

    val sequencerClient = mock[SequencerClient]
    when(
      sequencerClient.sendAsync(
        any[Batch[DefaultOpenEnvelope]],
        any[SendType],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[SendCallback],
      )(anyTraceContext)
    )
      .thenReturn(EitherT.rightT[Future, SendAsyncClientError](()))

    val changeTimes =
      (timeProofs
        .map(time => time.plusSeconds(1))
        .map(ts => TimeOfChange(RequestCounter(0), ts)) ++ contractSetup.values.toList
        .flatMap { case (_, creationTs, archivalTs) =>
          List(creationTs, archivalTs)
        }).distinct.sorted
    val changes = changeTimes.map(changesAtToc(contractSetup))
    val store = optCommitmentStore.getOrElse(new InMemoryAcsCommitmentStore(loggerFactory))

    val sortedReconciliationIntervalsProvider =
      overrideDefaultSortedReconciliationIntervalsProvider.getOrElse {
        constantSortedReconciliationIntervalsProvider(interval)
      }

    val acsCommitmentProcessor = new AcsCommitmentProcessor(
      domainId,
      localId,
      sequencerClient,
      domainCrypto,
      sortedReconciliationIntervalsProvider,
      store,
      _ => (),
      ParticipantTestMetrics.pruning,
      testedProtocolVersion,
      DefaultProcessingTimeouts.testing
        .copy(storageMaxRetryInterval = NonNegativeDuration.tryFromDuration(1.millisecond)),
      futureSupervisor,
      new InMemoryActiveContractStore(testedProtocolVersion, loggerFactory),
      new InMemoryContractStore(loggerFactory),
      // no additional consistency checks; if enabled, one needs to populate the above ACS and contract stores
      // correctly, otherwise the test will fail
      false,
      loggerFactory,
    )
    (acsCommitmentProcessor, store, sequencerClient, changes)
  }

  protected def testSetup(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[
        LfContractId,
        (Set[LfPartyId], TimeOfChange, TimeOfChange),
      ],
      topology: Map[ParticipantId, Set[LfPartyId]],
      optCommitmentStore: Option[AcsCommitmentStore] = None,
      overrideDefaultSortedReconciliationIntervalsProvider: Option[
        SortedReconciliationIntervalsProvider
      ] = None,
  )(implicit
      ec: ExecutionContext
  ): (AcsCommitmentProcessor, AcsCommitmentStore, SequencerClient) = {

    val (acsCommitmentProcessor, store, sequencerClient, changes) =
      testSetupDontPublish(
        timeProofs,
        contractSetup,
        topology,
        optCommitmentStore,
        overrideDefaultSortedReconciliationIntervalsProvider,
      )

    changes.foreach { case (ts, rc, acsChange) =>
      acsCommitmentProcessor.publish(RecordTime(ts, rc.v), acsChange)
    }
    (acsCommitmentProcessor, store, sequencerClient)
  }

  protected def setupContractsAndAcsChanges(): (
      Map[LfContractId, (Set[Ref.IdString.Party], NonEmpty[Seq[Lifespan]])],
      Map[CantonTimestampSecond, AcsChange],
  ) = {
    val contracts = Map(
      (
        coid(0, 0),
        (
          Set(alice, bob),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(4).forgetRefinement),
          ),
        ),
      ),
      (
        coid(1, 0),
        (
          Set(alice, bob),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(4).forgetRefinement),
            Lifespan(ts(7).forgetRefinement, ts(8).forgetRefinement),
          ),
        ),
      ),
      (
        coid(2, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(7).forgetRefinement, ts(8).forgetRefinement),
            Lifespan(ts(10).forgetRefinement, ts(12).forgetRefinement),
          ),
        ),
      ),
      (
        coid(3, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(9).forgetRefinement, ts(9).forgetRefinement),
          ),
        ),
      ),
    )

    val cs2 = CommitSet(
      creations = Map[LfContractId, WithContractHash[CreationCommit]](
        coid(0, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None)
          )
        ),
        coid(1, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None)
          )
        ),
      ),
      archivals = Map.empty[LfContractId, WithContractHash[ArchivalCommit]],
      transferOuts = Map.empty[LfContractId, WithContractHash[TransferOutCommit]],
      transferIns = Map.empty[LfContractId, WithContractHash[TransferInCommit]],
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs2 = AcsChange.fromCommitSet(cs2)

    val cs4 = CommitSet(
      creations = Map.empty[LfContractId, WithContractHash[CreationCommit]],
      archivals = Map[LfContractId, WithContractHash[ArchivalCommit]](
        coid(0, 0) -> withTestHash(
          ArchivalCommit(
            Set(alice, bob)
          )
        )
      ),
      transferOuts = Map[LfContractId, WithContractHash[TransferOutCommit]](
        coid(1, 0) -> withTestHash(
          TransferOutCommit(
            TargetDomainId(domainId),
            Set(alice, bob),
          )
        )
      ),
      transferIns = Map.empty[LfContractId, WithContractHash[TransferInCommit]],
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs4 = AcsChange.fromCommitSet(cs4)

    val cs7 = CommitSet(
      creations = Map[LfContractId, WithContractHash[CreationCommit]](
        coid(2, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None)
          )
        )
      ),
      archivals = Map.empty[LfContractId, WithContractHash[ArchivalCommit]],
      transferOuts = Map.empty[LfContractId, WithContractHash[TransferOutCommit]],
      transferIns = Map[LfContractId, WithContractHash[TransferInCommit]](
        coid(1, 0) -> withTestHash(
          TransferInCommit(
            TransferId(SourceDomainId(domainId), ts(4).forgetRefinement),
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
          )
        )
      ),
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs7 = AcsChange.fromCommitSet(cs7)

    val cs8 = CommitSet(
      creations = Map.empty[LfContractId, WithContractHash[CreationCommit]],
      archivals = Map[LfContractId, WithContractHash[ArchivalCommit]](
        coid(1, 0) -> withTestHash(
          ArchivalCommit(
            Set(alice, bob)
          )
        )
      ),
      transferOuts = Map[LfContractId, WithContractHash[TransferOutCommit]](
        coid(2, 0) -> withTestHash(
          TransferOutCommit(
            TargetDomainId(domainId),
            Set(alice, bob, carol),
          )
        )
      ),
      transferIns = Map.empty[LfContractId, WithContractHash[TransferInCommit]],
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs8 = AcsChange.fromCommitSet(cs8)

    val cs9 = CommitSet(
      creations = Map[LfContractId, WithContractHash[CreationCommit]](
        coid(3, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None)
          )
        )
      ),
      archivals = Map[LfContractId, WithContractHash[ArchivalCommit]](
        coid(3, 0) -> withTestHash(
          ArchivalCommit(
            Set(alice, bob, carol)
          )
        )
      ),
      transferOuts = Map.empty[LfContractId, WithContractHash[TransferOutCommit]],
      transferIns = Map.empty[LfContractId, WithContractHash[TransferInCommit]],
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs9 = AcsChange.fromCommitSet(cs9)

    val cs10 = CommitSet(
      creations = Map.empty[LfContractId, WithContractHash[CreationCommit]],
      archivals = Map.empty[LfContractId, WithContractHash[ArchivalCommit]],
      transferOuts = Map.empty[LfContractId, WithContractHash[TransferOutCommit]],
      transferIns = Map[LfContractId, WithContractHash[TransferInCommit]](
        coid(2, 0) -> withTestHash(
          TransferInCommit(
            TransferId(SourceDomainId(domainId), ts(8).forgetRefinement),
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None),
          )
        )
      ),
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs10 = AcsChange.fromCommitSet(cs10)

    val cs12 = CommitSet(
      creations = Map.empty[LfContractId, WithContractHash[CreationCommit]],
      archivals = Map.empty[LfContractId, WithContractHash[ArchivalCommit]],
      transferOuts = Map[LfContractId, WithContractHash[TransferOutCommit]](
        coid(2, 0) -> withTestHash(
          TransferOutCommit(
            TargetDomainId(domainId),
            Set(alice, bob, carol),
          )
        )
      ),
      transferIns = Map.empty[LfContractId, WithContractHash[TransferInCommit]],
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs12 = AcsChange.fromCommitSet(cs12)

    val acsChanges = Map(
      ts(2) -> acs2,
      ts(4) -> acs4,
      ts(7) -> acs7,
      ts(8) -> acs8,
      ts(9) -> acs9,
      ts(10) -> acs10,
      ts(12) -> acs12,
    )
    (contracts, acsChanges)
  }

  // participant "local" and participant "remoteId1" have two stakeholder groups in common (alice, bob), (alice, bob, charlie)
  // participant  "local" and participant "remoteId2" have three stakeholder groups in common (alice, bob, charlie), (alice, donna), (alice, ed)
  // all contracts are created at time 2
  // at time 4, a contract of (alice, bob), and a contract of (alice, bob, charlie) gets archived
  protected def setupContractsAndAcsChanges2(): (
      Map[LfContractId, (Set[Ref.IdString.Party], NonEmpty[Seq[Lifespan]])],
      Map[CantonTimestampSecond, AcsChange],
  ) = {
    val contracts = Map(
      (
        coid(0, 0),
        (
          Set(alice, bob),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(4).forgetRefinement),
          ),
        ),
      ),
      (
        coid(1, 0),
        (
          Set(alice, bob),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(6).forgetRefinement),
          ),
        ),
      ),
      (
        coid(2, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(10).forgetRefinement),
          ),
        ),
      ),
      (
        coid(3, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(4).forgetRefinement),
          ),
        ),
      ),
      (
        coid(4, 0),
        (
          Set(alice, danna),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(14).forgetRefinement),
          ),
        ),
      ),
      (
        coid(5, 0),
        (
          Set(alice, ed),
          NonEmpty.mk(
            Seq,
            Lifespan(ts(2).forgetRefinement, ts(18).forgetRefinement),
          ),
        ),
      ),
    )

    val cs2 = CommitSet(
      creations = Map[LfContractId, WithContractHash[CreationCommit]](
        coid(0, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None)
          )
        ),
        coid(1, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None)
          )
        ),
        coid(2, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None)
          )
        ),
        coid(3, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None)
          )
        ),
        coid(4, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, danna), None)
          )
        ),
        coid(5, 0) -> withTestHash(
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, ed), None)
          )
        ),
      ),
      archivals = Map.empty[LfContractId, WithContractHash[ArchivalCommit]],
      transferOuts = Map.empty[LfContractId, WithContractHash[TransferOutCommit]],
      transferIns = Map.empty[LfContractId, WithContractHash[TransferInCommit]],
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs2 = AcsChange.fromCommitSet(cs2)

    val cs4 = CommitSet(
      creations = Map.empty[LfContractId, WithContractHash[CreationCommit]],
      archivals = Map[LfContractId, WithContractHash[ArchivalCommit]](
        coid(0, 0) -> withTestHash(
          ArchivalCommit(
            Set(alice, bob)
          )
        ),
        coid(3, 0) -> withTestHash(
          ArchivalCommit(
            Set(alice, bob, carol)
          )
        ),
      ),
      transferOuts = Map.empty[LfContractId, WithContractHash[TransferOutCommit]],
      transferIns = Map.empty[LfContractId, WithContractHash[TransferInCommit]],
      keyUpdates = Map.empty[LfGlobalKey, ContractKeyJournal.Status],
    )
    val acs4 = AcsChange.fromCommitSet(cs4)

    val acsChanges = Map(ts(2) -> acs2, ts(4) -> acs4)
    (contracts, acsChanges)
  }

  val testHash: LfHash = ExampleTransactionFactory.lfHash(0)

  protected def withTestHash[A]: A => WithContractHash[A] = WithContractHash[A](_, testHash)

  protected def rt(timestamp: Int, tieBreaker: Int): RecordTime =
    RecordTime(ts(timestamp).forgetRefinement, tieBreaker.toLong)

  val coid = (txId, discriminator) => ExampleTransactionFactory.suffixedId(txId, discriminator)
}

class AcsCommitmentProcessorTest
    extends AsyncWordSpec
    with AcsCommitmentProcessorBaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  // This is duplicating the internal logic of the commitment computation, but I don't have a better solution at the moment
  // if we want to test whether commitment buffering works
  // Also assumes that all the contracts in the map have the same stakeholders
  private def stakeholderCommitment(
      contracts: Iterable[LfContractId]
  ): AcsCommitment.CommitmentType = {
    val h = LtHash16()
    contracts.foreach { cid =>
      h.add(
        (testHash.bytes.toByteString concat cid.encodeDeterministically).toByteArray
      )
    }
    h.getByteString()
  }

  private def commitmentsForCounterParticipants(
      stkhdCommitments: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      localId: ParticipantId,
      topology: Map[ParticipantId, Set[LfPartyId]],
  ): Map[ParticipantId, AcsCommitment.CommitmentType] = {

    def isCommonStakeholder(
        stkhd: Set[LfPartyId],
        localParties: Set[LfPartyId],
        remoteParticipantParties: Set[LfPartyId],
    ): Boolean =
      stkhd.intersect(localParties).nonEmpty && stkhd.intersect(remoteParticipantParties).nonEmpty

    val localParties = topology(localId)
    topology
      .filter { case (p, _) => p != localId }
      .map { case (participant, parties) =>
        (
          participant, {
            val x = stkhdCommitments
              .filter { case (stkhd, _) =>
                isCommonStakeholder(stkhd, localParties, parties)
              }
            commitmentsFromStkhdCmts(x.values.toSeq)
          },
        )
      }
      .filter { case (_, comm) => comm != LtHash16().getByteString() }
  }

  private def commitmentMsg(
      params: (
          ParticipantId,
          Iterable[LfContractId],
          CantonTimestampSecond,
          CantonTimestampSecond,
      )
  ): Future[SignedProtocolMessage[AcsCommitment]] = {
    val (remote, contracts, fromExclusive, toInclusive) = params

    val crypto =
      TestingTopology().withSimpleParticipants(remote).build().forOwnerAndDomain(remote)
    // we assume that the participant has a single stakeholder group
    val cmt = commitmentsFromStkhdCmts(Seq(stakeholderCommitment(contracts)))
    val snapshotF = crypto.snapshot(CantonTimestamp.Epoch)
    val period =
      CommitmentPeriod
        .create(fromExclusive.forgetRefinement, toInclusive.forgetRefinement, interval)
        .value
    val payload =
      AcsCommitment.create(domainId, remote, localId, period, cmt, testedProtocolVersion)

    snapshotF.flatMap { snapshot =>
      SignedProtocolMessage.trySignAndCreate(payload, snapshot, testedProtocolVersion)
    }
  }

  def commitmentsFromSnapshot(
      acs: ActiveContractSnapshot,
      at: CantonTimestampSecond,
      contractSetup: Map[LfContractId, (Set[Ref.IdString.Party], NonEmpty[Seq[Lifespan]])],
      crypto: SyncCryptoClient[DomainSnapshotSyncCryptoApi],
  ): Future[Map[ParticipantId, AcsCommitment.CommitmentType]] = {
    val stakeholderLookup = { (cid: LfContractId) =>
      contractSetup
        .map { case (cid, tuple) => (cid, tuple) }
        .get(cid)
        .map(_._1)
        .getOrElse(throw new Exception(s"unknown contract ID $cid"))
    }

    for {
      snapshot <- acs.snapshot(at.forgetRefinement)
      byStkhSet = snapshot
        .map { case (cid, _ts) =>
          cid -> stakeholderLookup(cid)
        }
        .groupBy { case (_, stakeholder) => stakeholder }
        .map { case (stkhs, m) =>
          logger.debug(
            s"adding to commitment for stakeholders $stkhs the parts cid and transfer counter in $m"
          )
          SortedSet(stkhs.toList *) -> stakeholderCommitment(m.map { case (cid, _) =>
            cid
          })

        }
      res <- AcsCommitmentProcessor.commitments(
        localId,
        byStkhSet,
        crypto,
        at,
        None,
        parallelism,
        new CachedCommitments(),
      )
    } yield res
  }

  // return active and delta-added commitments (byte strings)
  private def addCommonContractId(
      rc: RunningCommitments,
      hash: LfHash,
  ): (AcsCommitment.CommitmentType, AcsCommitment.CommitmentType) = {
    val commonContractId = coid(0, 0)
    rc.watermark shouldBe RecordTime.MinValue
    rc.snapshot() shouldBe CommitmentSnapshot(
      RecordTime.MinValue,
      Map.empty,
      Map.empty,
      Set.empty,
    )
    val ch1 = AcsChange(
      activations = Map(
        commonContractId -> WithContractHash(
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
          hash,
        )
      ),
      deactivations = Map.empty,
    )
    rc.update(rt(1, 0), ch1)
    rc.watermark shouldBe rt(1, 0)
    val snapshot = rc.snapshot()
    snapshot.recordTime shouldBe rt(1, 0)
    snapshot.active.keySet shouldBe Set(SortedSet(alice, bob))
    snapshot.delta.keySet shouldBe Set(SortedSet(alice, bob))
    snapshot.deleted shouldBe Set.empty
    (snapshot.active(SortedSet(alice, bob)), snapshot.delta(SortedSet(alice, bob)))
  }

  "AcsCommitmentProcessor.safeToPrune" must {
    "compute timestamp with no clean replay timestamp (no noOutstandingCommitment tick known)" in {
      val longInterval = PositiveSeconds.tryOfDays(100)
      for {
        res <- AcsCommitmentProcessor.safeToPrune_(
          cleanReplayF = Future.successful(CantonTimestamp.MinValue),
          commitmentsPruningBound =
            CommitmentsPruningBound.Outstanding(_ => Future.successful(None)),
          earliestInFlightSubmissionF = Future.successful(None),
          sortedReconciliationIntervalsProvider =
            constantSortedReconciliationIntervalsProvider(longInterval),
          domainId,
        )
      } yield res shouldBe None
    }

    "compute safeToPrune timestamp with no clean replay timestamp" in {
      val longInterval = PositiveSeconds.tryOfDays(100)
      for {
        res <- AcsCommitmentProcessor.safeToPrune_(
          cleanReplayF = Future.successful(CantonTimestamp.MinValue),
          commitmentsPruningBound = CommitmentsPruningBound.Outstanding(_ =>
            Future.successful(Some(CantonTimestamp.MinValue))
          ),
          earliestInFlightSubmissionF = Future.successful(None),
          sortedReconciliationIntervalsProvider =
            constantSortedReconciliationIntervalsProvider(longInterval),
          domainId,
        )
      } yield res shouldBe Some(CantonTimestampSecond.MinValue)
    }

    "take checkForOutstandingCommitments flag into account" in {
      val longInterval = PositiveSeconds.tryOfDays(100)
      val now = CantonTimestamp.now()

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(longInterval)

      def safeToPrune(
          checkForOutstandingCommitments: Boolean
      ): Future[Option[CantonTimestampSecond]] = {
        val noOutstandingCommitmentsF: CantonTimestamp => Future[Some[CantonTimestamp]] =
          _ => Future.successful(Some(CantonTimestamp.MinValue))
        val lastComputedAndSentF = Future.successful(Some(now))

        AcsCommitmentProcessor.safeToPrune_(
          cleanReplayF = Future.successful(now),
          commitmentsPruningBound =
            if (checkForOutstandingCommitments)
              CommitmentsPruningBound.Outstanding(noOutstandingCommitmentsF)
            else CommitmentsPruningBound.LastComputedAndSent(lastComputedAndSentF),
          earliestInFlightSubmissionF = Future.successful(None),
          sortedReconciliationIntervalsProvider =
            constantSortedReconciliationIntervalsProvider(longInterval),
          domainId,
        )
      }

      for {
        res1 <- safeToPrune(true)
        res2 <- safeToPrune(false)
        sortedReconciliationIntervals <- sortedReconciliationIntervalsProvider
          .reconciliationIntervals(now)
        tick = sortedReconciliationIntervals.tickBeforeOrAt(now).value
      } yield {
        res1 shouldBe Some(CantonTimestampSecond.MinValue)
        res2 shouldBe Some(tick)
      }
    }
  }

  private val parallelism = PositiveNumeric.tryCreate(2)

  "AcsCommitmentProcessor" must {
    "computes commitments for the correct counter-participants, basic but incomplete sanity checks on commitments" in {
      // an ACS with contracts that have never been reassigned
      val contractSetup = Map(
        (
          coid(0, 0),
          (
            Set(alice, bob),
            NonEmpty.mk(
              Seq,
              Lifespan(ts(2).forgetRefinement, ts(4).forgetRefinement),
            ),
          ),
        ),
        (
          coid(1, 0),
          (
            Set(alice, bob),
            NonEmpty.mk(
              Seq,
              Lifespan(ts(2).forgetRefinement, ts(5).forgetRefinement),
            ),
          ),
        ),
        (
          coid(2, 0),
          (
            Set(alice, bob, carol),
            NonEmpty.mk(
              Seq,
              Lifespan(ts(7).forgetRefinement, ts(8).forgetRefinement),
            ),
          ),
        ),
        (
          coid(3, 0),
          (
            Set(alice, bob, carol),
            NonEmpty.mk(
              Seq,
              Lifespan(ts(9).forgetRefinement, ts(9).forgetRefinement),
            ),
          ),
        ),
        (
          coid(4, 0),
          (
            Set(bob, carol),
            NonEmpty.mk(
              Seq,
              Lifespan(
                ts(11).forgetRefinement,
                ts(13).forgetRefinement,
              ),
            ),
          ),
        ),
      )

      val crypto = cryptoSetup(localId, topology)

      val acsF = acsSetup(contractSetup.fmap { case (_, lifespan) =>
        lifespan
      })

      for {
        acs <- acsF
        commitments1 <- commitmentsFromSnapshot(acs, ts(1), contractSetup, crypto)
        commitments3 <- commitmentsFromSnapshot(acs, ts(3), contractSetup, crypto)
        commitments4 <- commitmentsFromSnapshot(acs, ts(4), contractSetup, crypto)
        commitments5 <- commitmentsFromSnapshot(acs, ts(5), contractSetup, crypto)
        commitments7 <- commitmentsFromSnapshot(acs, ts(7), contractSetup, crypto)
        commitments9 <- commitmentsFromSnapshot(acs, ts(9), contractSetup, crypto)
        commitments10 <- commitmentsFromSnapshot(acs, ts(10), contractSetup, crypto)
        commitments12 <- commitmentsFromSnapshot(acs, ts(12), contractSetup, crypto)
      } yield {
        assert(commitments1.isEmpty)
        assert(commitments3.contains(remoteId1))
        assert(!commitments3.contains(remoteId2))
        assert(!commitments3.contains(remoteId3))

        assert(commitments4.contains(remoteId1))
        assert(commitments4.get(remoteId1) != commitments3.get(remoteId1))

        assert(commitments5.isEmpty)

        assert(commitments7.contains(remoteId1))
        assert(commitments7.contains(remoteId2))
        assert(!commitments7.contains(remoteId3))
        assert(commitments7.get(remoteId1) == commitments7.get(remoteId2))
        assert(commitments7.get(remoteId1) != commitments4.get(remoteId1))
        assert(commitments7.get(remoteId1) != commitments5.get(remoteId1))

        assert(commitments9.isEmpty)
        assert(commitments10.isEmpty)

        // the participant localId does not host any stakeholder of the active contract coid(4,0)
        assert(commitments12.isEmpty)
      }
    }

    // Covers the case where a previously hosted stakeholder got disabled
    "ignore contracts in the ACS snapshot where the participant doesn't host a stakeholder" in {
      val snapshot1 = Map(
        SortedSet(bob, carol) -> LtHash16().getByteString()
      )
      val snapshot2 = Map(
        // does the participant localId, which does not host neither bob nor carol, have this commitment
        // because the scenario is that localId used to host at least one of them?
        SortedSet(bob, carol) -> LtHash16().getByteString(),
        SortedSet(alice, bob) -> LtHash16().getByteString(),
      )
      val snapshot3 = Map(
        SortedSet(alice, bob) -> LtHash16().getByteString()
      )
      val crypto = cryptoSetup(localId, topology)

      for {
        res1 <- AcsCommitmentProcessor.commitments(
          localId,
          snapshot1,
          crypto,
          ts(0),
          None,
          parallelism,
          new CachedCommitments(),
        )
        res2 <- AcsCommitmentProcessor.commitments(
          localId,
          snapshot2,
          crypto,
          ts(0),
          None,
          parallelism,
          new CachedCommitments(),
        )
        res3 <- AcsCommitmentProcessor.commitments(
          localId,
          snapshot3,
          crypto,
          ts(0),
          None,
          parallelism,
          new CachedCommitments(),
        )
      } yield {
        res1 shouldBe Map.empty
        res2.keySet shouldBe Set(remoteId1)
        res2 shouldEqual res3
      }
    }

    "correctly issue local and process buffered remote commitments" in {

      val timeProofs = List(3L, 6, 10, 16).map(CantonTimestamp.ofEpochSecond)
      val contractSetup = Map(
        // contract ID to stakeholders, creation and archival time
        (
          coid(0, 0),
          (Set(alice, bob), toc(1), toc(9)),
        ),
        (
          coid(0, 1),
          (Set(alice, carol), toc(9), toc(12)),
        ),
        (
          coid(1, 0),
          (Set(alice, carol), toc(1), toc(3)),
        ),
      )

      val topology = Map(
        localId -> Set(alice),
        remoteId1 -> Set(bob),
        remoteId2 -> Set(carol),
      )

      val (processor, store, sequencerClient, changes) =
        testSetupDontPublish(timeProofs, contractSetup, topology)

      val remoteCommitments = List(
        (remoteId1, Seq(coid(0, 0)), ts(0), ts(5)),
        (remoteId2, Seq(coid(0, 1)), ts(5), ts(10)),
      )

      for {
        remote <- remoteCommitments.parTraverse(commitmentMsg)
        delivered = remote.map(cmt =>
          (
            cmt.message.period.toInclusive.plusSeconds(1),
            List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
          )
        )
        // First ask for the remote commitments to be processed, and then compute locally
        _ <- delivered
          .parTraverse_ { case (ts, batch) =>
            processor.processBatchInternal(ts.forgetRefinement, batch)
          }
          .onShutdown(fail())
        _ <- processChanges(processor, store, changes)

        computed <- store.searchComputedBetween(CantonTimestamp.Epoch, timeProofs.lastOption.value)
        received <- store.searchReceivedBetween(CantonTimestamp.Epoch, timeProofs.lastOption.value)
      } yield {
        verify(sequencerClient, times(2)).sendAsync(
          any[Batch[DefaultOpenEnvelope]],
          any[SendType],
          any[Option[CantonTimestamp]],
          any[CantonTimestamp],
          any[MessageId],
          any[SendCallback],
        )(anyTraceContext)

        assert(computed.size === 2)
        assert(received.size === 2)
      }
    }

    /*
     This test is disabled for protocol versions for which the reconciliation interval is
     static because the described setting cannot occur.

     Important note! The test duplicates the logic of computing (remote) commitments via the val `commitmentMsg`.
     If one changes the logic of computing commitments, one *also* needs to change the commitment computation
     in `commitmentMsg`, otherwise the test will fail.
     */

    "work when commitment tick falls between two participants connection to the domain" in {
      /*
        The goal here is to check that ACS commitment processing works even when
        a commitment tick falls between two participants' connection timepoints to the domain.
        The reason this scenario is important is because the reconciliation interval (and
        thus ticks) is defined only from the connection time.

        We test the following scenario (timestamps are considered as seconds since epoch):
        - Reconciliation interval = 5s
        - Remote participant (RP) connects to the domain at t=0
        - Local participant (LP) connects to the domain at t=6
        - A shared contract lives between t=8 and t=12
        - RP sends a commitment with period (5, 10]
          Note: t=5 is not on a tick for LP
        - LP sends a commitment with period (0, 10]

        At t=13, we check that:
        - Nothing is outstanding at LP
        - Computed and received commitments are correct
       */

      interval shouldBe PositiveSeconds.tryOfSeconds(5)

      val timeProofs = List[Long](9, 13).map(CantonTimestamp.ofEpochSecond)
      val contractSetup = Map(
        // contract ID to stakeholders, creation and archival time
        (
          coid(0, 0),
          (Set(alice, bob), toc(8), toc(12)),
        )
      )

      val topology = Map(
        localId -> Set(alice),
        remoteId1 -> Set(bob),
      )

      val sortedReconciliationIntervalsProvider = constantSortedReconciliationIntervalsProvider(
        interval,
        domainBootstrappingTime = CantonTimestamp.ofEpochSecond(6),
      )

      val (processor, store, _) = testSetup(
        timeProofs,
        contractSetup,
        topology,
        overrideDefaultSortedReconciliationIntervalsProvider =
          Some(sortedReconciliationIntervalsProvider),
      )

      val remoteCommitments =
        List((remoteId1, Seq(coid(0, 0)), ts(5), ts(10)))

      for {
        remote <- remoteCommitments.parTraverse(commitmentMsg)
        delivered = remote.map(cmt =>
          (
            cmt.message.period.toInclusive.plusSeconds(1),
            List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
          )
        )
        // First ask for the remote commitments to be processed, and then compute locally
        _ <- delivered
          .parTraverse_ { case (ts, batch) =>
            processor.processBatchInternal(ts.forgetRefinement, batch)
          }
          .onShutdown(fail())

        _ <- processor.flush()

        computed <- store.searchComputedBetween(
          CantonTimestamp.Epoch,
          timeProofs.lastOption.value,
        )
        received <- store.searchReceivedBetween(
          CantonTimestamp.Epoch,
          timeProofs.lastOption.value,
        )
        outstanding <- store.outstanding(
          CantonTimestamp.MinValue,
          timeProofs.lastOption.value,
          None,
        )

      } yield {
        computed.size shouldBe 1
        inside(computed.headOption.value) { case (commitmentPeriod, participantId, _) =>
          commitmentPeriod shouldBe CommitmentPeriod
            .create(CantonTimestampSecond.MinValue, ts(10))
            .value
          participantId shouldBe remoteId1
        }

        received.size shouldBe 1

        inside(received.headOption.value) {
          case SignedProtocolMessage(
                TypedSignedProtocolMessageContent(
                  AcsCommitment(_, sender, counterParticipant, period, _)
                ),
                _,
              ) =>
            sender shouldBe remoteId1
            counterParticipant shouldBe localId
            period shouldBe CommitmentPeriod.create(ts(5), ts(10)).value
        }

        outstanding shouldBe empty
      }
    }

    "prevent pruning when there is no timestamp such that no commitments are outstanding" in {
      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenReturn(Future.successful(None))
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(0), CantonTimestamp.Epoch, CantonTimestamp.Epoch, None)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), CantonTimestamp.Epoch)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(0), CantonTimestamp.Epoch)
        )
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          constantSortedReconciliationIntervalsProvider(defaultReconciliationInterval),
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        res shouldEqual None
      }
    }

    "prevent pruning when there is no clean head in the request journal" in {
      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(Some(ts.min(CantonTimestamp.Epoch)))
        }
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      for {
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          constantSortedReconciliationIntervalsProvider(defaultReconciliationInterval),
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        res shouldEqual Some(CantonTimestampSecond.MinValue)
      }
    }

    def assertInIntervalBefore(
        before: CantonTimestamp,
        reconciliationInterval: PositiveSeconds,
    ): Option[CantonTimestampSecond] => Assertion = {
      case None => fail()
      case Some(ts) =>
        val delta = JDuration.between(ts.toInstant, before.toInstant)
        delta should be > JDuration.ofSeconds(0)
        delta should be <= reconciliationInterval.unwrap
    }

    "prevent pruning of requests needed for crash recovery" in {
      val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)
      val requestTsDelta = 20.seconds

      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)
      val ts0 = CantonTimestamp.Epoch
      val ts1 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis)
      val ts2 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      val ts3 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 3)
      val ts4 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 5)
      for {
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(0), ts0, ts0))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), ts0)
        )
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(1), ts1, ts3.plusMillis(1))
        ) // RC1 commits after RC3
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(1), ts1)
        )
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(2), ts2, ts2))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(2), ts2)
        )
        _ <- requestJournalStore.insert(
          RequestData(RequestCounter(3), RequestState.Pending, ts3, None)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(3), ts3)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(CursorPrehead(RequestCounter(2), ts2))
        res1 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
        _ <- requestJournalStore.insert(
          RequestData(RequestCounter(4), RequestState.Pending, ts4, None)
        ) // Replay starts at ts4
        _ <- requestJournalStore
          .replace(RequestCounter(3), ts3, RequestState.Clean, Some(ts3))
          .valueOrFail("advance RC 3 to clean")
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(4), ts4)
        )
        res2 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        withClue("request 1:") {
          assertInIntervalBefore(ts1, reconciliationInterval)(res1)
        } // Do not prune request 1
        // Do not prune request 1 as crash recovery may delete the dirty request 4 and then we're back in the same situation as for res1
        withClue("request 3:") {
          assertInIntervalBefore(ts1, reconciliationInterval)(res2)
        }
      }
    }

    "prevent pruning of the last request known to be clean" in {
      val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)
      val requestTsDelta = 20.seconds

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      val ts0 = CantonTimestamp.Epoch
      val tsCleanRequest = CantonTimestamp.Epoch.plusMillis(requestTsDelta.toMillis * 1)
      val ts3 = CantonTimestamp.Epoch.plusMillis(requestTsDelta.toMillis * 3)
      for {
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(0), ts0, ts0))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), ts0)
        )
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(2), tsCleanRequest, tsCleanRequest)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(2), tsCleanRequest)
        )
        _ <- requestJournalStore.insert(RequestData(RequestCounter(3), RequestState.Pending, ts3))
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(4), ts3)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(2), tsCleanRequest)
        )
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield assertInIntervalBefore(tsCleanRequest, reconciliationInterval)(res)
    }

    "prevent pruning of dirty sequencer counters" in {
      val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)
      val requestTsDelta = 20.seconds

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      // Clean sequencer counter is behind clean request counter
      val ts1 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis)
      val tsCleanRequest = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(2), tsCleanRequest, tsCleanRequest)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(2), tsCleanRequest)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(0), ts1)
        )
        res <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        assertInIntervalBefore(ts1, reconciliationInterval)(res)
      }
    }

    "prevent pruning of events corresponding to in-flight requests" in {
      val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)
      val requestTsDelta = 20.seconds

      val changeId1 = mkChangeIdHash(1)
      val changeId2 = mkChangeIdHash(2)

      val submissionId = LedgerSubmissionId.assertFromString("submission-id").some

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
      val sequencerCounterTrackerStore =
        new InMemorySequencerCounterTrackerStore(loggerFactory, timeouts)
      val acsCommitmentStore = mock[AcsCommitmentStore]
      when(acsCommitmentStore.noOutstandingCommitments(any[CantonTimestamp])(any[TraceContext]))
        .thenAnswer { (ts: CantonTimestamp, _: TraceContext) =>
          Future.successful(
            Some(ts.min(CantonTimestamp.Epoch.plusSeconds(JDuration.ofDays(200).getSeconds)))
          )
        }
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      val sortedReconciliationIntervalsProvider =
        constantSortedReconciliationIntervalsProvider(reconciliationInterval)

      // In-flight submission 2 and 3 are clean
      val tsCleanRequest = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      val tsCleanRequest2 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 3)
      // In-flight submission 1 has timed out
      val ts1 = CantonTimestamp.ofEpochSecond(-100)
      val submission1 = InFlightSubmission(
        changeId1,
        submissionId,
        domainId,
        new UUID(0, 1),
        None,
        UnsequencedSubmission(ts1, TestSubmissionTrackingData.default),
        traceContext,
      )
      val submission2 = InFlightSubmission(
        changeId2,
        submissionId,
        domainId,
        new UUID(0, 2),
        None,
        UnsequencedSubmission(CantonTimestamp.MaxValue, TestSubmissionTrackingData.default),
        traceContext,
      )
      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(2), tsCleanRequest, tsCleanRequest)
        )
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(3), tsCleanRequest2, tsCleanRequest2)
        )
        _ <- requestJournalStore.advancePreheadCleanTo(
          CursorPrehead(RequestCounter(3), tsCleanRequest2)
        )
        _ <- sequencerCounterTrackerStore.advancePreheadSequencerCounterTo(
          CursorPrehead(SequencerCounter(1), tsCleanRequest2)
        )
        () <- inFlightSubmissionStore
          .register(submission1)
          .valueOrFailShutdown("register message ID 1")
        () <- inFlightSubmissionStore
          .register(submission2)
          .valueOrFailShutdown("register message ID 2")
        () <- inFlightSubmissionStore.observeSequencing(
          submission2.submissionDomain,
          Map(submission2.messageId -> SequencedSubmission(SequencerCounter(2), tsCleanRequest)),
        )
        res1 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
        // Now remove the timed-out submission 1 and compute the pruning point again
        () <- inFlightSubmissionStore.delete(Seq(submission1.referenceByMessageId))
        res2 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
        // Now remove the clean request and compute the pruning point again
        () <- inFlightSubmissionStore.delete(Seq(submission2.referenceByMessageId))
        res3 <- AcsCommitmentProcessor.safeToPrune(
          requestJournalStore,
          sequencerCounterTrackerStore,
          sortedReconciliationIntervalsProvider,
          acsCommitmentStore,
          inFlightSubmissionStore,
          domainId,
          checkForOutstandingCommitments = true,
        )
      } yield {
        assertInIntervalBefore(submission1.associatedTimestamp, reconciliationInterval)(res1)
        assertInIntervalBefore(tsCleanRequest, reconciliationInterval)(res2)
        assertInIntervalBefore(tsCleanRequest2, reconciliationInterval)(res3)
      }
    }

    "running commitments work as expected" in {
      val rc =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)

      rc.watermark shouldBe RecordTime.MinValue
      rc.snapshot() shouldBe CommitmentSnapshot(
        RecordTime.MinValue,
        Map.empty,
        Map.empty,
        Set.empty,
      )
      val ch1 = AcsChange(
        activations = Map(
          coid(0, 0) -> withTestHash(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None)
          ),
          coid(0, 1) -> withTestHash(
            ContractMetadata.tryCreate(Set.empty, Set(bob, carol), None)
          ),
        ),
        deactivations = Map.empty,
      )
      rc.update(rt(1, 0), ch1)
      rc.watermark shouldBe rt(1, 0)
      val snap1 = rc.snapshot()
      snap1.recordTime shouldBe rt(1, 0)
      snap1.active.keySet shouldBe Set(SortedSet(alice, bob), SortedSet(bob, carol))
      snap1.delta.keySet shouldBe Set(SortedSet(alice, bob), SortedSet(bob, carol))
      snap1.deleted shouldBe Set.empty

      val ch2 = AcsChange(
        deactivations = Map(
          coid(0, 0) -> withTestHash(
            ContractStakeholders(Set(alice, bob))
          )
        ),
        activations = Map(
          coid(1, 1) -> withTestHash(
            ContractMetadata.tryCreate(Set.empty, Set(alice, carol), None)
          )
        ),
      )
      rc.update(rt(1, 1), ch2)
      rc.watermark shouldBe rt(1, 1)
      val snap2 = rc.snapshot()
      snap2.recordTime shouldBe rt(1, 1)
      snap2.active.keySet shouldBe Set(SortedSet(alice, carol), SortedSet(bob, carol))
      snap2.delta.keySet shouldBe Set(SortedSet(alice, carol))
      snap2.deleted shouldBe Set(SortedSet(alice, bob))

      val ch3 = AcsChange(
        deactivations = Map.empty,
        activations = Map(
          coid(2, 1) -> withTestHash(
            ContractMetadata.tryCreate(Set.empty, Set(alice, carol), None)
          )
        ),
      )
      rc.update(rt(3, 0), ch3)
      val snap3 = rc.snapshot()
      snap3.recordTime shouldBe rt(3, 0)
      snap3.active.keySet shouldBe Set(SortedSet(alice, carol), SortedSet(bob, carol))
      snap3.delta.keySet shouldBe Set(SortedSet(alice, carol))
      snap3.deleted shouldBe Set.empty
    }

    "contracts differing by contract hash only result in different commitments" in {
      val rc1 =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)
      val rc2 =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)
      val hash1 = ExampleTransactionFactory.lfHash(1)
      val hash2 = ExampleTransactionFactory.lfHash(2)
      hash1 should not be hash2

      val (activeCommitment1, deltaAddedCommitment1) =
        addCommonContractId(rc1, hash1)
      val (activeCommitment2, deltaAddedCommitment2) =
        addCommonContractId(rc2, hash2)
      activeCommitment1 should not be activeCommitment2
      deltaAddedCommitment1 should not be deltaAddedCommitment2
    }

    // Test timeline of contract creations, reassignments and archivals
    // cid 0       c   a
    // cid 1       c   to   ti   a
    // cid 2                c    to        ti   to
    // cid 3                         ca
    // timestamp   2   4    7    8    9    10   12
    "ensure that computed commitments are consistent with the ACS snapshot" in {
      // setup
      val (contractSetup, acsChanges) = setupContractsAndAcsChanges()
      val crypto = cryptoSetup(localId, topology)

      // 1. compute stakeholder commitments by repeatedly applying acs changes (obtained from a commit set)
      // to an empty snapshot using AcsCommitmentProcessor.update
      // and then compute counter-participant commitments by adding together stakeholder commitments
      val rc =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)

      rc.update(rt(2, 0), acsChanges(ts(2)))
      rc.watermark shouldBe rt(2, 0)
      val rcBasedCommitments2 =
        commitmentsForCounterParticipants(rc.snapshot().active, localId, topology)

      rc.update(rt(4, 0), acsChanges(ts(4)))
      rc.watermark shouldBe rt(4, 0)
      val rcBasedCommitments4 =
        commitmentsForCounterParticipants(rc.snapshot().active, localId, topology)

      rc.update(rt(7, 0), acsChanges(ts(7)))
      rc.watermark shouldBe rt(7, 0)
      val rcBasedCommitments7 =
        commitmentsForCounterParticipants(rc.snapshot().active, localId, topology)

      rc.update(rt(8, 0), acsChanges(ts(8)))
      rc.watermark shouldBe rt(8, 0)
      val rcBasedCommitments8 =
        commitmentsForCounterParticipants(rc.snapshot().active, localId, topology)

      rc.update(rt(9, 0), acsChanges(ts(9)))
      rc.watermark shouldBe rt(9, 0)
      val rcBasedCommitments9 =
        commitmentsForCounterParticipants(rc.snapshot().active, localId, topology)

      rc.update(rt(10, 0), acsChanges(ts(10)))
      rc.watermark shouldBe rt(10, 0)
      val rcBasedCommitments10 =
        commitmentsForCounterParticipants(rc.snapshot().active, localId, topology)

      rc.update(rt(12, 0), acsChanges(ts(12)))
      rc.watermark shouldBe rt(12, 0)
      val rcBasedCommitments12 =
        commitmentsForCounterParticipants(rc.snapshot().active, localId, topology)

      // 2. compute commitments by building the acs, recompute the stakeholder commitments based on
      // the acs snapshot, and then combine them into a per-participant commitment using AcsCommitmentProcessor.commitments
      val acsF = acsSetup(contractSetup.fmap { case (_, lifespan) =>
        lifespan
      })

      for {
        acs <- acsF
        commitments2 <- commitmentsFromSnapshot(acs, ts(2), contractSetup, crypto)
        commitments4 <- commitmentsFromSnapshot(acs, ts(4), contractSetup, crypto)
        commitments7 <- commitmentsFromSnapshot(acs, ts(7), contractSetup, crypto)
        commitments8 <- commitmentsFromSnapshot(acs, ts(8), contractSetup, crypto)
        commitments9 <- commitmentsFromSnapshot(acs, ts(9), contractSetup, crypto)
        commitments10 <- commitmentsFromSnapshot(acs, ts(10), contractSetup, crypto)
        commitments12 <- commitmentsFromSnapshot(acs, ts(12), contractSetup, crypto)
      } yield {
        assert(commitments2 equals rcBasedCommitments2)
        assert(commitments4 equals rcBasedCommitments4)
        assert(commitments7 equals rcBasedCommitments7)
        assert(commitments8 equals rcBasedCommitments8)
        assert(commitments9 equals rcBasedCommitments9)
        assert(commitments10 equals rcBasedCommitments10)
        assert(commitments12 equals rcBasedCommitments12)
      }
    }

    "use catch-up logic correctly:" must {

      def checkCatchUpModeCfgCorrect(
          processor: pruning.AcsCommitmentProcessor,
          cantonTimestamp: CantonTimestamp,
          nrIntervalsToTriggerCatchUp: PositiveInt = PositiveInt.tryCreate(1),
          catchUpIntervalSkip: PositiveInt = PositiveInt.tryCreate(2),
      ): Future[Assertion] = {
        for {
          config <- processor.catchUpConfig(cantonTimestamp)
        } yield {
          config match {
            case Some(cfg) =>
              assert(cfg.nrIntervalsToTriggerCatchUp == nrIntervalsToTriggerCatchUp)
              assert(cfg.catchUpIntervalSkip == catchUpIntervalSkip)
            case None => fail("catch up mode needs to be enabled")
          }
        }
      }

      def checkCatchUpModeCfgDisabled(
          processor: pruning.AcsCommitmentProcessor,
          cantonTimestamp: CantonTimestamp,
      ): Future[Assertion] = {
        for {
          config <- processor.catchUpConfig(cantonTimestamp)
        } yield {
          config match {
            case Some(cfg) => fail(s"Canton config is defined ($cfg) at $cantonTimestamp")
            case None => succeed
          }
        }
      }

      "enter catch up mode when processing falls behind" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val timeProofs = List(3L, 8, 20, 35, 59).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(9)),
          ),
          (
            coid(0, 1),
            (Set(alice, carol), toc(11), toc(21)),
          ),
          (
            coid(1, 0),
            (Set(alice, carol), toc(18), toc(33)),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(timeProofs, contractSetup, topology, catchUpModeEnabled = true)

        val remoteCommitments = List(
          (remoteId1, Seq(coid(0, 0)), ts(0), ts(5)),
          (remoteId2, Seq(coid(0, 1)), ts(10), ts(15)),
          (
            remoteId2,
            Seq(coid(1, 0), coid(0, 1)),
            ts(15),
            ts(20),
          ),
          (remoteId2, Seq(coid(1, 0)), ts(20), ts(25)),
          (remoteId2, Seq(coid(1, 0)), ts(25), ts(30)),
        )

        for {
          _ <- checkCatchUpModeCfgCorrect(processor, timeProofs.head)
          remote <- remoteCommitments.parTraverse(commitmentMsg)
          delivered = remote.map(cmt =>
            (
              cmt.message.period.toInclusive.plusSeconds(1),
              List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
            )
          )
          // First ask for the remote commitments to be processed, and then compute locally
          // This triggers catch-up mode
          _ <- delivered
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())
          _ <- processChanges(processor, store, changes)

          outstanding <- store.noOutstandingCommitments(timeProofs.lastOption.value)
          computed <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          received <- store.searchReceivedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
        } yield {
          // the participant catches up to ticks 10, 20, 30
          // the only ticks with non-empty commitments are at 20 and 30, and they match the remote ones,
          // therefore there are 2 sends of commitments
          verify(sequencerClient, times(2)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
          assert(computed.size === 4)
          assert(received.size === 5)
          // all local commitments were matched and can be pruned
          assert(outstanding == Some(toc(55).timestamp))
        }
      }

      "catch up in correct skip steps scenario1" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val reconciliationInterval = 5
        val testSequences =
          (1L to 14)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(20000)),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig = new CatchUpConfig(PositiveInt.tryCreate(2), PositiveInt.tryCreate(3))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(CantonTimestamp.MaxValue),
          parameter = defaultParameters.tryUpdate(catchUpConfig = Some(startConfig)),
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            catchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity),
          )

        for {
          _ <- checkCatchUpModeCfgCorrect(
            processor,
            testSequences.head,
            startConfig.nrIntervalsToTriggerCatchUp,
            startConfig.catchUpIntervalSkip,
          )

          // we apply any changes (contract deployment) that happens before our windows
          _ = changes
            .filter(a => a._1 < testSequences.head)
            .foreach { case (ts, tb, change) =>
              processor.publish(RecordTime(ts, tb.v), change)
            }
          _ <- processor.flush()
          _ <- testSequence(
            testSequences,
            processor,
            changes,
            store,
            reconciliationInterval,
          )
          // first catch-up step to 30, in steps of 2*5 => 3 sends at [10,20,30]
          // second catch-up step to 60, in steps of 2*5 => 2 sends at [40,50,60]
          // third catch-up would be up to 90, in steps of 2*5, but only if we are 3*5 = 15 seconds behind
          // as the last tick is 70, we don't trigger catch-up mode, and instead send commitments every 5 seconds
          // so the timestamps are [5,10,20,30,40,50,60,65,70]
          _ = verify(sequencerClient, times(9)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
        } yield {
          succeed
        }
      }

      "catch up in correct skip steps scenario2" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val reconciliationInterval = 5
        val testSequences =
          (1L to 45)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(20000)),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig = new CatchUpConfig(PositiveInt.tryCreate(10), PositiveInt.tryCreate(2))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(CantonTimestamp.MaxValue),
          parameter = defaultParameters.tryUpdate(catchUpConfig = Some(startConfig)),
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            catchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity),
          )

        for {
          _ <- checkCatchUpModeCfgCorrect(
            processor,
            testSequences.head,
            startConfig.nrIntervalsToTriggerCatchUp,
            startConfig.catchUpIntervalSkip,
          )

          // we apply any changes (contract deployment) that happens before our windows
          _ = changes
            .filter(a => a._1 < testSequences.head)
            .foreach { case (ts, tb, change) =>
              processor.publish(RecordTime(ts, tb.v), change)
            }
          _ <- processor.flush()
          _ <- testSequence(
            testSequences,
            processor,
            changes,
            store,
            reconciliationInterval,
          )
          // we get an initial send at 5
          // first catch-up step to 100, in steps of 10*5 => 2 sends at [50,100]
          // second catch-up to 200, in steps of 10*5 => 2 sends at [150,200]
          // at time 200 we end catch-up mode, and send 5 more commitments at 205,210,215,220,225
          _ = verify(sequencerClient, times(10)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
        } yield {
          succeed
        }
      }

      "pruning works correctly for a participant ahead of a counter-participant that catches up" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val timeProofs = List(5L, 10, 15, 20, 25, 30).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(9)),
          ),
          (
            coid(0, 1),
            (Set(alice, carol), toc(11), toc(21)),
          ),
          (
            coid(1, 0),
            (Set(alice, carol), toc(18), toc(33)),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(timeProofs, contractSetup, topology, catchUpModeEnabled = true)

        val remoteCommitments = List(
          (remoteId1, Seq(coid(0, 0)), ts(0), ts(5)),
          (remoteId2, Seq(coid(0, 1)), ts(10), ts(15)),
          (
            remoteId2,
            Seq(coid(1, 0), coid(0, 1)),
            ts(15),
            ts(20),
          ),
          (remoteId2, Seq(coid(1, 0)), ts(20), ts(30)),
        )

        for {
          _ <- checkCatchUpModeCfgCorrect(processor, timeProofs.head)
          remote <- remoteCommitments.parTraverse(commitmentMsg)
          delivered = remote.map(cmt =>
            (
              cmt.message.period.toInclusive.plusSeconds(1),
              List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
            )
          )
          // First ask for the local commitments to be processed, and then receive the remote ones,
          // because the remote participants are catching up
          _ <- processChanges(processor, store, changes)
          _ <- delivered
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())
          _ <- processor.flush()
          outstanding <- store.noOutstandingCommitments(timeProofs.lastOption.value)
          computed <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          received <- store.searchReceivedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
        } yield {
          // regular sends (no catch-up) at ticks 5, 15, 20, 25, 30 (tick 10 has an empty commitment)
          verify(sequencerClient, times(5)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
          assert(computed.size === 5)
          assert(received.size === 4)
          // all local commitments were matched and can be pruned
          assert(outstanding == Some(toc(30).timestamp))
        }
      }

      "send skipped commitments on mismatch during catch-up" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {

        val timeProofs = List(3L, 8, 20, 35, 59).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(9)),
          ),
          (
            coid(0, 1),
            (Set(alice, carol), toc(11), toc(21)),
          ),
          (
            coid(1, 0),
            (Set(alice, carol), toc(18), toc(33)),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(timeProofs, contractSetup, topology, catchUpModeEnabled = true)

        val remoteCommitments = List(
          (remoteId1, Seq(coid(0, 0)), ts(0), ts(5)),
          (remoteId2, Seq(coid(0, 1)), ts(10), ts(15)),
          // wrong contract, causes mismatch
          (
            remoteId2,
            Seq(coid(1, 1), coid(2, 1)),
            ts(15),
            ts(20),
          ),
          (remoteId2, Seq(coid(1, 0)), ts(20), ts(25)),
          (remoteId2, Seq(coid(1, 0)), ts(25), ts(30)),
        )

        for {
          _ <- checkCatchUpModeCfgCorrect(processor, timeProofs.head)
          remote <- remoteCommitments.parTraverse(commitmentMsg)
          delivered = remote.map(cmt =>
            (
              cmt.message.period.toInclusive.plusSeconds(1),
              List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
            )
          )
          // First ask for the remote commitments to be processed, and then compute locally
          _ <- delivered
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())

          _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              changes.foreach { case (ts, tb, change) =>
                processor.publish(RecordTime(ts, tb.v), change)
              }
              processor.flush()
            },
            // there should be one mismatch
            // however, since buffered remote commitments are deleted asynchronously, it can happen that they
            // are processed (checked for matches) several times before deleted
            forAtLeast(1, _) {
              _.warningMessage
                .sliding("ACS_COMMITMENT_MISMATCH(5,0): The local commitment does not match".length)
                .count(substr =>
                  substr == "ACS_COMMITMENT_MISMATCH(5,0): The local commitment does not match"
                ) shouldEqual 1
            },
          )

          outstanding <- store.noOutstandingCommitments(toc(30).timestamp)
          computed <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          received <- store.searchReceivedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
        } yield {
          // there are three sends, at the end of each coarse-grained interval 10, 20, 30
          // the send at the end of interval 10 is empty, so that is not performed
          // therefore, there should be 2 async sends
          // there should be one mismatch, with carol, for the interval 15-20
          // which means we send the fine-grained commitment 10-15
          // therefore, there should be 3 async sends in total
          verify(sequencerClient, times(3)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
          assert(computed.size === 4)
          assert(received.size === 5)
          // cannot prune past the mismatch
          assert(outstanding == Some(toc(30).timestamp))
        }
      }

      "prune correctly on mismatch during catch-up" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {

        val timeProofs = List(3L, 8, 20, 35, 59).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(9)),
          ),
          (
            coid(0, 1),
            (Set(alice, carol), toc(11), toc(21)),
          ),
          (
            coid(1, 0),
            (Set(alice, carol), toc(18), toc(33)),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(timeProofs, contractSetup, topology, catchUpModeEnabled = true)

        val remoteCommitments = List(
          (remoteId1, Seq(coid(0, 0)), ts(0), ts(5)),
          (remoteId2, Seq(coid(0, 1)), ts(10), ts(15)),
          // wrong contract, causes mismatch
          (
            remoteId2,
            Seq(coid(1, 1), coid(2, 1)),
            ts(15),
            ts(20),
          ),
          (remoteId2, Seq(coid(1, 0)), ts(20), ts(25)),
          // wrong contract, causes mismatch
          (remoteId2, Seq(coid(1, 1)), ts(25), ts(30)),
        )

        for {
          _ <- checkCatchUpModeCfgCorrect(processor, timeProofs.head)
          remote <- remoteCommitments.parTraverse(commitmentMsg)
          delivered = remote.map(cmt =>
            (
              cmt.message.period.toInclusive.plusSeconds(1),
              List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
            )
          )
          // First ask for the remote commitments to be processed, and then compute locally
          _ <- delivered
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())

          _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              changes.foreach { case (ts, tb, change) =>
                processor.publish(RecordTime(ts, tb.v), change)
              }
              processor.flush()
            },
            // there should be two mismatches
            // however, since buffered remote commitments are deleted asynchronously, it can happen that they
            // are processed (checked for matches) several times before deleted
            forAtLeast(2, _) {
              _.warningMessage
                .sliding("ACS_COMMITMENT_MISMATCH(5,0): The local commitment does not match".length)
                .count(substr =>
                  substr == "ACS_COMMITMENT_MISMATCH(5,0): The local commitment does not match"
                ) shouldEqual 1
            },
          )

          outstanding <- store.noOutstandingCommitments(toc(30).timestamp)
          computed <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          received <- store.searchReceivedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
        } yield {
          // there are three sends, at the end of each coarse-grained interval 10, 20, 30
          // the send at the end of interval 10 is empty, so that is not performed
          // therefore, there should be 2 async sends
          // there should be two mismatch, with carol, for the intervals 15-20 and 20-30.
          // which means we send the fine-grained commitment 10-15
          // however, there is no commitment to send for the interval 20-25, because we never observed this interval;
          // we only observed the interval 20-30, for which we already sent a commitment.
          // therefore, there should be 3 async sends in total
          verify(sequencerClient, times(3)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
          assert(computed.size === 4)
          assert(received.size === 5)
          // cannot prune past the mismatch 25-30, because there are no commitments that match past this point
          assert(outstanding == Some(toc(25).timestamp))
        }
      }

      "dynamically change, disable & re-enable catch-up config during a catch-up" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val reconciliationInterval = 5
        val testSequences =
          List(
            // we split them up by large amounts to avoid potential overlaps
            (1L to 5)
              .map(i => i * reconciliationInterval)
              .map(CantonTimestamp.ofEpochSecond)
              .toList,
            (101L to 105)
              .map(i => i * reconciliationInterval)
              .map(CantonTimestamp.ofEpochSecond)
              .toList,
            (201L to 205)
              .map(i => i * reconciliationInterval)
              .map(CantonTimestamp.ofEpochSecond)
              .toList,
          )

        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(20000)),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val midConfig = new CatchUpConfig(PositiveInt.tryCreate(1), PositiveInt.tryCreate(2))
        val changedConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.last.head,
          validUntil = None,
          parameter = defaultParameters.tryUpdate(catchUpConfig = Some(midConfig)),
        )

        val disabledConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.apply(1).head,
          validUntil = Some(testSequences.apply(1).last),
          parameter = defaultParameters,
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences.flatten,
            contractSetup,
            topology,
            catchUpModeEnabled = true,
            domainParametersUpdates = List(disabledConfigWithValidity, changedConfigWithValidity),
          )

        for {
          _ <- checkCatchUpModeCfgCorrect(processor, testSequences.head.head)
          _ <- checkCatchUpModeCfgDisabled(processor, testSequences.apply(1).last)
          _ <- checkCatchUpModeCfgCorrect(
            processor,
            testSequences.last.last,
            nrIntervalsToTriggerCatchUp = midConfig.nrIntervalsToTriggerCatchUp,
            catchUpIntervalSkip = midConfig.catchUpIntervalSkip,
          )

          // we apply any changes (contract deployment) that happens before our windows
          _ = changes
            .filter(a => a._1 <= testSequences.head.head)
            .foreach { case (ts, tb, change) =>
              processor.publish(RecordTime(ts, tb.v), change)
            }
          _ <- processor.flush()
          _ <- testSequence(
            testSequences.head,
            processor,
            changes,
            store,
            reconciliationInterval,
            expectDegradation = true,
          )
          // catchup is enabled so we send only 3 commitments
          _ = verify(sequencerClient, times(3)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
          _ <- testSequence(
            testSequences.apply(1),
            processor,
            changes,
            store,
            reconciliationInterval,
          )
          // catchup is disabled so we send all 5 commitments (plus 3 previous)
          _ = verify(sequencerClient, times(3 + 5)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
          _ <- testSequence(
            testSequences.last,
            processor,
            changes,
            store,
            reconciliationInterval,
            expectDegradation = true,
          )
          // catchup is re-enabled but with a step of 1 so we send 5 commitments (plus 5 & 3 previous)
          _ = verify(sequencerClient, times(3 + 5 + 5)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
        } yield {
          succeed
        }
      }

      "disable catch-up config during catch-up mode" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val reconciliationInterval = 5
        val testSequences =
          (1L to 10)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val changeConfigTimestamp = CantonTimestamp.ofEpochSecond(36L)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(20000)),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig = new CatchUpConfig(PositiveInt.tryCreate(3), PositiveInt.tryCreate(1))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(changeConfigTimestamp),
          parameter = defaultParameters.tryUpdate(catchUpConfig = Some(startConfig)),
        )

        val disabledConfigWithValidity = DomainParameters.WithValidity(
          validFrom = changeConfigTimestamp,
          validUntil = None,
          parameter = defaultParameters,
        )
        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            catchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity, disabledConfigWithValidity),
          )

        for {
          _ <- checkCatchUpModeCfgCorrect(
            processor,
            testSequences.head,
            startConfig.nrIntervalsToTriggerCatchUp,
            startConfig.catchUpIntervalSkip,
          )
          _ <- checkCatchUpModeCfgDisabled(processor, testSequences.last)

          // we apply any changes (contract deployment) that happens before our windows
          _ =
            changes
              .filter(a => a._1 < testSequences.head)
              .foreach { case (ts, tb, change) =>
                processor.publish(RecordTime(ts, tb.v), change)
              }
          _ <- processor.flush()

          _ <- testSequence(
            testSequences,
            processor,
            changes,
            store,
            reconciliationInterval,
          )
          // here we get the times: [5,10,15,20,25,30,35,40,45,50]
          // we disable the config at 36.
          // expected send timestamps are: [5,15,30,45,50,55]
          _ = verify(sequencerClient, times(6)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
        } yield {
          succeed
        }
      }

      "change catch-up config during catch-up mode" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val reconciliationInterval = 5
        val testSequences =
          (1L to 11)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val changeConfigTimestamp = CantonTimestamp.ofEpochSecond(36L)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(20000)),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig = new CatchUpConfig(PositiveInt.tryCreate(3), PositiveInt.tryCreate(1))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(changeConfigTimestamp),
          parameter = defaultParameters.tryUpdate(catchUpConfig = Some(startConfig)),
        )

        val changeConfig = new CatchUpConfig(PositiveInt.tryCreate(2), PositiveInt.tryCreate(1))
        val changeConfigWithValidity = DomainParameters.WithValidity(
          validFrom = changeConfigTimestamp,
          validUntil = None,
          parameter = defaultParameters.tryUpdate(catchUpConfig = Some(changeConfig)),
        )
        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            catchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity, changeConfigWithValidity),
          )

        for {
          _ <- checkCatchUpModeCfgCorrect(
            processor,
            testSequences.head,
            startConfig.nrIntervalsToTriggerCatchUp,
            startConfig.catchUpIntervalSkip,
          )
          _ <- checkCatchUpModeCfgCorrect(
            processor,
            testSequences.last,
            changeConfig.nrIntervalsToTriggerCatchUp,
            changeConfig.catchUpIntervalSkip,
          )

          // we apply any changes (contract deployment) that happens before our windows
          _ = changes
            .filter(a => a._1 < testSequences.head)
            .foreach { case (ts, tb, change) =>
              processor.publish(RecordTime(ts, tb.v), change)

            }
          _ <- processor.flush()

          _ <- testSequence(
            testSequences,
            processor,
            changes,
            store,
            reconciliationInterval,
            expectDegradation = true,
          )
          // here we get the times: [5,10,15,20,25,30,35,40,45,50,55]
          // the sends with a catch-up of (3,1) are [5,15,30,45]
          // we change the config at 36 to (2,1), while we were catching up to 45
          // at the next tick, 40, we realize we are at a boundary according to the new catch-up parameters and we send
          // the catch-up is extended to 50, and we also send at 50
          // expected send timestamps are: [5,15,30,40,50]
          // 55 is not expected since at 45 we are still behind (so next catchup boundary would be 60)
          _ = verify(sequencerClient, times(5)).sendAsync(
            any[Batch[DefaultOpenEnvelope]],
            any[SendType],
            any[Option[CantonTimestamp]],
            any[CantonTimestamp],
            any[MessageId],
            any[SendCallback],
          )(anyTraceContext)
        } yield {
          succeed
        }
      }

      "should mark as unhealthy when not caught up" onlyRunWithOrGreaterThan ProtocolVersion.v6 in {
        val reconciliationInterval = 5
        val testSequences =
          (1L to 10)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList

        val timeProofs =
          (1L to 5)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList

        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (Set(alice, bob), toc(1), toc(20000)),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val (processor, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            catchUpModeEnabled = true,
          )

        for {
          // we apply any changes (contract deployment) that happens before our windows
          _ <- Future.successful(
            changes
              .filter(a => a._1 < testSequences.head)
              .foreach { case (ts, tb, change) =>
                processor.publish(RecordTime(ts, tb.v), change)

              }
          )
          _ <- processor.flush()

          _ <- testSequence(
            testSequences,
            processor,
            changes,
            store,
            reconciliationInterval,
            expectDegradation = true,
          )
          _ = assert(processor.healthComponent.isDegrading)
        } yield {
          succeed
        }
      }

      def testSequence(
          sequence: List[CantonTimestamp],
          processor: AcsCommitmentProcessor,
          changes: List[(CantonTimestamp, RequestCounter, AcsChange)],
          store: AcsCommitmentStore,
          reconciliationInterval: Int,
          expectDegradation: Boolean = false,
      ): Future[Assertion] = {
        val remoteCommitments = sequence
          .map(i =>
            (
              remoteId1,
              Seq(coid(0, 0)),
              ts(i),
              ts(i.plusSeconds(reconciliationInterval.toLong)),
            )
          )
        for {
          remote <- remoteCommitments.parTraverse(commitmentMsg)
          delivered = remote.map(cmt =>
            (
              cmt.message.period.toInclusive,
              List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
            )
          )
          endOfRemoteCommitsPeriod = sequence.last.plusSeconds(
            reconciliationInterval.toLong
          )

          changesApplied = changes
            .filter(a => a._1 >= sequence.head && a._1 <= endOfRemoteCommitsPeriod)

          // First ask for the remote commitments to be processed, and then compute locally
          // This triggers catch-up mode
          _ <- delivered
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())
          _ <- processChanges(
            processor,
            store,
            changesApplied,
          )

          received <- store.searchReceivedBetween(
            sequence.head,
            endOfRemoteCommitsPeriod,
          )
          computed <- store.searchComputedBetween(
            sequence.head,
            endOfRemoteCommitsPeriod,
          )
        } yield {
          if (expectDegradation)
            assert(processor.healthComponent.isDegrading)
          else {
            assert(processor.healthComponent.isOk)
          }
          if (changesApplied.last._1 >= sequence.last)
            assert(computed.size === sequence.length)
          assert(received.size === sequence.length)
        }
      }
    }

    def processChanges(
        processor: AcsCommitmentProcessor,
        store: AcsCommitmentStore,
        changes: List[(CantonTimestamp, RequestCounter, AcsChange)],
    ): Future[Unit] = {
      lazy val fut = {
        changes.foreach { case (ts, tb, change) =>
          processor.publish(RecordTime(ts, tb.v), change)
        }
        processor.flush()
      }
      for {
        config <- processor.catchUpConfig(changes.head._1)
        remote <- store.searchReceivedBetween(changes.head._1, changes.last._1)
        _ <- config match {
          case _ if remote.isEmpty => fut
          case None => fut
          case Some(cfg) if cfg.catchUpIntervalSkip.value == 1 =>
            loggerFactory.assertLogs(
              fut,
              entry => {
                entry.message should include(AcsCommitmentDegradationWithIneffectiveConfig.id)
              },
            )
          case _ =>
            loggerFactory.assertLogs(
              fut,
              entry => {
                entry.message should include(AcsCommitmentDegradation.id)
              },
            )
        }
      } yield ()

    }
    "caching commitments" should {

      "caches and computes correctly" in {
        val (_, acsChanges) = setupContractsAndAcsChanges2()
        val crypto = cryptoSetup(localId, topology)

        val inMemoryCommitmentStore = new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          // init phase
          rc <- runningCommitments
          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          byParticipant2 <- AcsCommitmentProcessor.stakeholderCommitmentsPerParticipant(
            localId,
            rc.snapshot().active,
            crypto,
            ts(2),
            parallelism,
          )
          normalCommitments2 = computeCommitmentsPerParticipant(byParticipant2, cachedCommitments)
          _ = cachedCommitments.setCachedCommitments(
            normalCommitments2,
            rc.snapshot().active,
            byParticipant2.map { case (pid, set) =>
              (pid, set.map { case (stkhd, _cmt) => stkhd }.toSet)
            },
          )

          // update: at time 4, a contract of (alice, bob), and a contract of (alice, bob, charlie) gets archived
          // these are all the stakeholder groups participant "localId" has in common with participant "remoteId1"
          // in contrast, (alice, bob, charlie) is one of three stakeholder groups participant "localId" has in common
          // with participant "remoteId2"
          _ = rc.update(rt(4, 0), acsChanges(ts(4)))

          byParticipant <- AcsCommitmentProcessor.stakeholderCommitmentsPerParticipant(
            localId,
            rc.snapshot().active,
            crypto,
            ts(4),
            parallelism,
          )

          computeFromCachedRemoteId1 = cachedCommitments.computeCmtFromCached(
            remoteId1,
            byParticipant(remoteId1).toMap,
          )

          computeFromCachedRemoteId2 = cachedCommitments.computeCmtFromCached(
            remoteId2,
            byParticipant(remoteId2).toMap,
          )
        } yield {
          // because more than 1/2 of the stakeholder commitments for participant "remoteId1" change, we shouldn't
          // use cached commitments for the computation of remoteId1's commitment
          assert(computeFromCachedRemoteId1.isEmpty)
          // because less than 1/2 of the stakeholder commitments for participant "remoteId2" change, we should
          // use cached commitments for the computation of remoteId2's commitment
          assert(computeFromCachedRemoteId2.isDefined)
        }
      }

      "yields the same commitments as without caching" in {

        // setup
        // participant "local" and participant "remoteId1" have two stakeholder groups in common (alice, bob), (alice, bob, charlie)
        // participant  "local" and participant "remoteId2" have three stakeholder groups in common (alice, bob, charlie), (alice, donna), (alice, ed)
        // all contracts are created at time 2
        // at time 4, a contract of (alice, bob), and a contract of (alice, bob, charlie) gets archived
        // we cache commitments for participant "remoteId2"
        // the commitments participant "local" computes for participant "remoteId1" should use the normal computation method
        // the commitments participant "local" computes for participant "remoteId2" should use the caching computation method
        val (_, acsChanges) = setupContractsAndAcsChanges2()
        val crypto = cryptoSetup(localId, topology)

        val inMemoryCommitmentStore = new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          rc <- runningCommitments

          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          normalCommitments2 <- AcsCommitmentProcessor.commitments(
            localId,
            rc.snapshot().active,
            crypto,
            ts(2),
            None,
            parallelism,
            // behaves as if we don't use caching, because we don't reuse this object for further computation
            new CachedCommitments(),
          )
          cachedCommitments2 <- AcsCommitmentProcessor.commitments(
            localId,
            rc.snapshot().active,
            crypto,
            ts(2),
            None,
            parallelism,
            cachedCommitments,
          )

          _ = rc.update(rt(4, 0), acsChanges(ts(4)))
          normalCommitments4 <- AcsCommitmentProcessor.commitments(
            localId,
            rc.snapshot().active,
            crypto,
            ts(4),
            None,
            parallelism,
            // behaves as if we don't use caching, because we don't reuse this object for further computation
            new CachedCommitments(),
          )
          cachedCommitments4 <- AcsCommitmentProcessor.commitments(
            localId,
            rc.snapshot().active,
            crypto,
            ts(4),
            None,
            parallelism,
            cachedCommitments,
          )

        } yield {
          assert(normalCommitments2 equals cachedCommitments2)
          assert(normalCommitments4 equals cachedCommitments4)
        }
      }

      "handles stakeholder group removal correctly" in {
        val (_, acsChanges) = setupContractsAndAcsChanges2()
        val crypto = cryptoSetup(remoteId2, topology)

        val inMemoryCommitmentStore = new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          // init phase
          // participant "remoteId2" has one stakeholder group in common with "remote1": (alice, bob, charlie) with one contract
          // participant "remoteId2" has three stakeholder group in common with "local": (alice, bob, charlie) with one contract,
          // (alice, danna) with one contract, and (alice, ed) with one contract
          rc <- runningCommitments
          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          byParticipant2 <- AcsCommitmentProcessor.stakeholderCommitmentsPerParticipant(
            remoteId2,
            rc.snapshot().active,
            crypto,
            ts(2),
            parallelism,
          )
          normalCommitments2 = computeCommitmentsPerParticipant(byParticipant2, cachedCommitments)
          _ = cachedCommitments.setCachedCommitments(
            normalCommitments2,
            rc.snapshot().active,
            byParticipant2.map { case (pid, set) =>
              (pid, set.map { case (stkhd, _cmt) => stkhd }.toSet)
            },
          )

          byParticipant <- AcsCommitmentProcessor.stakeholderCommitmentsPerParticipant(
            remoteId2,
            rc.snapshot().active,
            crypto,
            ts(2),
            parallelism,
          )

          // simulate offboarding party ed from remoteId2 by replacing the commitment for (alice,ed) with an empty commitment
          byParticipantWithOffboard = byParticipant.updatedWith(localId) {
            case Some(stakeholdersCmts) =>
              Some(stakeholdersCmts.updated(SortedSet(alice, ed), emptyCommitment))
            case None => None
          }

          computeFromCachedLocalId1 = cachedCommitments.computeCmtFromCached(
            localId,
            byParticipantWithOffboard(localId),
          )

          // the correct commitment for local should not include any commitment for (alice, ed)
          correctCmts = commitmentsFromStkhdCmts(
            byParticipantWithOffboard(localId)
              .map { case (stakeholders, cmt) => cmt }
              .filter(_ != AcsCommitmentProcessor.emptyCommitment)
              .toSeq
          )
        } yield {
          assert(computeFromCachedLocalId1.contains(correctCmts))
        }
      }

      "handles stakeholder group addition correctly" in {
        val (_, acsChanges) = setupContractsAndAcsChanges2()
        val crypto = cryptoSetup(remoteId2, topology)

        val inMemoryCommitmentStore = new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          // init phase
          // participant "remoteId2" has one stakeholder group in common with "remote1": (alice, bob, charlie) with one contract
          // participant "remoteId2" has three stakeholder group in common with "local": (alice, bob, charlie) with one contract,
          // (alice, danna) with one contract, and (alice, ed) with one contract
          rc <- runningCommitments
          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          byParticipant2 <- AcsCommitmentProcessor.stakeholderCommitmentsPerParticipant(
            remoteId2,
            rc.snapshot().active,
            crypto,
            ts(2),
            parallelism,
          )
          normalCommitments2 = computeCommitmentsPerParticipant(byParticipant2, cachedCommitments)
          _ = cachedCommitments.setCachedCommitments(
            normalCommitments2,
            rc.snapshot().active,
            byParticipant2.map { case (pid, set) =>
              (pid, set.map { case (stkhd, _cmt) => stkhd }.toSet)
            },
          )

          byParticipant <- AcsCommitmentProcessor.stakeholderCommitmentsPerParticipant(
            remoteId2,
            rc.snapshot().active,
            crypto,
            ts(2),
            parallelism,
          )

          // simulate onboarding party ed to remoteId1 by adding remoteId1 as a participant for group (alice, ed)
          // the commitment for (alice,ed) existed previously, but was not used for remoteId1's commitment
          // also simulate creating a new, previously inexisting stakeholder group (danna, ed) with commitment dummyCmt,
          // which the commitment for remoteId1 needs to use now that ed is onboarded on remoteId1
          newCmts = byParticipant(localId).filter(x => x._1 == SortedSet(alice, ed)) ++ Map(
            SortedSet(danna, ed) -> dummyCmt
          )
          byParticipantWithOnboard = byParticipant.updatedWith(remoteId1) {
            case Some(stakeholdersCmts) => Some(stakeholdersCmts ++ newCmts)
            case None => Some(newCmts)
          }

          computeFromCachedRemoteId1 = cachedCommitments.computeCmtFromCached(
            remoteId1,
            byParticipantWithOnboard(remoteId1).toMap,
          )

          // the correct commitment for local should include the commitments for (alice, bob, charlie), (alice, ed)
          // and (dana, ed)
          correctCmts = commitmentsFromStkhdCmts(
            byParticipantWithOnboard(remoteId1).values.toSeq
          )
        } yield {
          assert(computeFromCachedRemoteId1.contains(correctCmts))
        }
      }

    }
  }
}

final case class Lifespan(
    createdTs: CantonTimestamp,
    archivedTs: CantonTimestamp,
)

class AcsCommitmentProcessorSyncTest
    extends AnyWordSpec
    with AcsCommitmentProcessorBaseTest
    with HasExecutionContext
    with RepeatableTestSuiteTest {

  "retry on DB exceptions" in {
    val timeProofs = List(0L, 1).map(CantonTimestamp.ofEpochSecond)
    val contractSetup = Map(
      (
        coid(0, 0),
        (Set(alice, bob), toc(1), toc(9)),
      )
    )

    val topology = Map(
      localId -> Set(alice),
      remoteId1 -> Set(bob),
    )

    val badStore = new ThrowOnWriteCommitmentStore()
    loggerFactory.assertLoggedWarningsAndErrorsSeq(
      {
        val (processor, _, _sequencerClient) = testSetup(
          timeProofs,
          contractSetup,
          topology,
          optCommitmentStore = Some(badStore),
        )
        eventually(timeUntilSuccess = FiniteDuration(40, TimeUnit.SECONDS)) {
          badStore.writeCounter.get() should be > 100
        }
        logger.info("Close the processor to stop retrying")
        processor.close()
      },
      forAll(_) {
        _.warningMessage should (include(
          s"Disconnect and reconnect to the domain ${domainId.toString} if this error persists."
        ) or include regex "Timeout .* expired, but tasks still running. Shutting down forcibly")
      },
    )
  }
}
