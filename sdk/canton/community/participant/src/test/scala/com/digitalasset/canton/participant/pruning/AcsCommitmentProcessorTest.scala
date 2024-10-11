// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.syntax.functor.*
import cats.syntax.option.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt, PositiveNumeric}
import com.digitalasset.canton.config.{
  DefaultProcessingTimeouts,
  NonNegativeDuration,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.ledger.participant.state.{DomainIndex, RequestIndex, SequencerIndex}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.event.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
  RecordTime,
}
import com.digitalasset.canton.participant.metrics.ParticipantTestMetrics
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet.{
  ArchivalCommit,
  AssignmentCommit,
  CreationCommit,
  UnassignmentCommit,
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
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.{PositiveSeconds, SimClock}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.daml.lf.data.Ref
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

  protected lazy val crypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  protected lazy val interval = PositiveSeconds.tryOfSeconds(5)
  protected lazy val domainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("domain::da"))
  protected lazy val localId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::domain")
  )
  protected lazy val remoteId1 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant1::domain")
  )
  protected lazy val remoteId2 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant2::domain")
  )
  protected lazy val remoteId3 = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("remoteParticipant3::domain")
  )

  protected lazy val List(alice, bob, carol, danna, ed) =
    List("Alice::1", "Bob::2", "Carol::3", "Danna::4", "Ed::5").map(LfPartyId.assertFromString)

  protected lazy val topology = Map(
    localId -> Set(alice),
    remoteId1 -> Set(bob),
    remoteId2 -> Set(carol, danna, ed),
  )

  protected lazy val dummyCmt = {
    val h = LtHash16()
    h.add("123".getBytes())
    h.getByteString()
  }

  lazy val initialReassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis

  protected def ts(i: CantonTimestamp): CantonTimestampSecond =
    CantonTimestampSecond.ofEpochSecond(i.getEpochSecond)

  protected def ts(i: Long): CantonTimestampSecond =
    CantonTimestampSecond.ofEpochSecond(i.longValue)

  protected def toc(timestamp: Long, requestCounter: Int = 0): TimeOfChange =
    TimeOfChange(RequestCounter(requestCounter), ts(timestamp).forgetRefinement)

  protected def mkChangeIdHash(index: Int) = ChangeIdHash(DefaultDamlValues.lfhash(index))

  private lazy val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)

  protected def acsSetup(
      contracts: Map[LfContractId, NonEmpty[Seq[Lifespan]]]
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[ActiveContractSnapshot] = {
    val acs =
      new InMemoryActiveContractStore(indexedStringStore, loggerFactory)
    contracts.toList
      .flatMap { case (cid, seq) => seq.forgetNE.map(lifespan => (cid, lifespan)) }
      .parTraverse_ { case (cid, lifespan) =>
        for {
          _ <- {
            if (lifespan.reassignmentCounterAtActivation == ReassignmentCounter.Genesis)
              acs
                .markContractCreated(
                  cid -> initialReassignmentCounter,
                  TimeOfChange(RequestCounter(0), lifespan.activatedTs),
                )
                .value
            else
              acs
                .assignContract(
                  cid,
                  TimeOfChange(RequestCounter(0), lifespan.activatedTs),
                  Source(domainId),
                  lifespan.reassignmentCounterAtActivation,
                )
                .value
          }
          _ <- lifespan match {
            case Lifespan.ArchiveOnDeactivate(_, deactivatedTs, _) =>
              acs
                .archiveContract(
                  cid,
                  TimeOfChange(RequestCounter(0), deactivatedTs),
                )
                .value
            case Lifespan.UnassignmentDeactivate(
                  _,
                  deactivatedTs,
                  _,
                  reassignmentCounterAtUnassignment,
                ) =>
              acs
                .unassignContracts(
                  cid,
                  TimeOfChange(RequestCounter(0), lifespan.deactivatedTs),
                  Target(domainId),
                  reassignmentCounterAtUnassignment,
                )
                .value
          }
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
      .build(crypto, loggerFactory)
      .forOwnerAndDomain(owner)
  }

  protected def changesAtToc(
      contractSetup: Map[
        LfContractId,
        (Set[LfPartyId], TimeOfChange, TimeOfChange, ReassignmentCounter, ReassignmentCounter),
      ]
  )(toc: TimeOfChange): (CantonTimestamp, RequestCounter, AcsChange) =
    (
      toc.timestamp,
      toc.rc,
      contractSetup.foldLeft(AcsChange.empty) {
        case (
              acsChange,
              (
                cid,
                (
                  stkhs,
                  creationToc,
                  archivalToc,
                  assignReassignmentCounter,
                  unassignReassignmentCounter,
                ),
              ),
            ) =>
          AcsChange(
            deactivations =
              acsChange.deactivations ++ (if (archivalToc == toc)
                                            Map(
                                              cid ->
                                                ContractStakeholdersAndReassignmentCounter(
                                                  stkhs,
                                                  unassignReassignmentCounter,
                                                )
                                            )
                                          else Map.empty),
            activations =
              acsChange.activations ++ (if (creationToc == toc)
                                          Map(
                                            cid ->
                                              ContractStakeholdersAndReassignmentCounter(
                                                stkhs,
                                                assignReassignmentCounter,
                                              )
                                          )
                                        else Map.empty),
          )
      },
    )

  // Create the processor, but return the changes instead of publishing them, such that the user can decide when
  // to publish
  protected def testSetupDontPublish(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[
        LfContractId,
        (Set[LfPartyId], TimeOfChange, TimeOfChange, ReassignmentCounter, ReassignmentCounter),
      ],
      topology: Map[ParticipantId, Set[LfPartyId]],
      optCommitmentStore: Option[AcsCommitmentStore] = None,
      overrideDefaultSortedReconciliationIntervalsProvider: Option[
        SortedReconciliationIntervalsProvider
      ] = None,
      acsCommitmentsCatchUpModeEnabled: Boolean = false,
      domainParametersUpdates: List[DomainParameters.WithValidity[DynamicDomainParameters]] =
        List.empty,
      reconciliationIntervalsUpdates: List[DynamicDomainParametersWithValidity] = List.empty,
  )(implicit ec: ExecutionContext): (
      FutureUnlessShutdown[AcsCommitmentProcessor],
      AcsCommitmentStore,
      TestSequencerClientSend,
      List[(CantonTimestamp, RequestCounter, AcsChange)],
  ) = {

    val acsCommitmentsCatchUpConfig =
      if (acsCommitmentsCatchUpModeEnabled)
        Some(AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(2), PositiveInt.tryCreate(1)))
      else None

    val domainCrypto = cryptoSetup(
      localId,
      topology,
      domainParametersUpdates.appended(
        DomainParameters.WithValidity(
          validFrom = CantonTimestamp.MinValue,
          validUntil = domainParametersUpdates
            .sortBy(_.validFrom)
            .headOption
            .fold(Some(CantonTimestamp.MaxValue))(param => Some(param.validFrom)),
          parameter = defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter =
            acsCommitmentsCatchUpConfig
          ),
        )
      ),
    )

    val sequencerClient = new TestSequencerClientSend

    val changeTimes =
      (timeProofs
        .map(time => time.plusSeconds(1))
        .map(ts => TimeOfChange(RequestCounter(0), ts)) ++ contractSetup.values.toList
        .flatMap { case (_, creationTs, archivalTs, _, _) =>
          List(creationTs, archivalTs)
        }).distinct.sorted
    val changes = changeTimes.map(changesAtToc(contractSetup))
    val store =
      optCommitmentStore.getOrElse(
        new InMemoryAcsCommitmentStore(loggerFactory)
      )

    val sortedReconciliationIntervalsProvider =
      overrideDefaultSortedReconciliationIntervalsProvider.getOrElse {
        constantSortedReconciliationIntervalsProvider(interval)
      }

    val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)

    val acsCommitmentProcessor = AcsCommitmentProcessor(
      domainId,
      localId,
      sequencerClient,
      domainCrypto,
      sortedReconciliationIntervalsProvider,
      store,
      _ => (),
      ParticipantTestMetrics.domain.commitments,
      testedProtocolVersion,
      DefaultProcessingTimeouts.testing
        .copy(storageMaxRetryInterval = NonNegativeDuration.tryFromDuration(1.millisecond)),
      futureSupervisor,
      new InMemoryActiveContractStore(indexedStringStore, loggerFactory),
      new InMemoryContractStore(loggerFactory),
      // no additional consistency checks; if enabled, one needs to populate the above ACS and contract stores
      // correctly, otherwise the test will fail
      false,
      loggerFactory,
      TestingConfigInternal(),
      new SimClock(loggerFactory = loggerFactory),
      exitOnFatalFailures = true,
      // do not delay sending commitments for testing, because tests often expect to see commitments after an interval
      Some(NonNegativeInt.zero),
    )
    (acsCommitmentProcessor, store, sequencerClient, changes)
  }

  protected def testSetup(
      timeProofs: List[CantonTimestamp],
      contractSetup: Map[
        LfContractId,
        (Set[LfPartyId], TimeOfChange, TimeOfChange, ReassignmentCounter, ReassignmentCounter),
      ],
      topology: Map[ParticipantId, Set[LfPartyId]],
      optCommitmentStore: Option[AcsCommitmentStore] = None,
      overrideDefaultSortedReconciliationIntervalsProvider: Option[
        SortedReconciliationIntervalsProvider
      ] = None,
  )(implicit
      ec: ExecutionContext
  ): (FutureUnlessShutdown[AcsCommitmentProcessor], AcsCommitmentStore, TestSequencerClientSend) = {

    val (acsCommitmentProcessor, store, sequencerClient, changes) =
      testSetupDontPublish(
        timeProofs,
        contractSetup,
        topology,
        optCommitmentStore,
        overrideDefaultSortedReconciliationIntervalsProvider,
      )

    val proc = for {
      processor <- acsCommitmentProcessor
      _ = changes.foreach { case (ts, rc, acsChange) =>
        processor.publish(RecordTime(ts, rc.v), acsChange, Future.unit)
      }
    } yield processor
    (proc, store, sequencerClient)
  }

  protected def setupContractsAndAcsChanges(): (
      Map[LfContractId, (Set[Ref.IdString.Party], NonEmpty[Seq[Lifespan]])],
      Map[CantonTimestampSecond, AcsChange],
  ) = {
    val reassignmentCounter2 = initialReassignmentCounter + 1
    val reassignmentCounter3 = initialReassignmentCounter + 2
    val contracts = Map(
      (
        coid(0, 0),
        (
          Set(alice, bob),
          NonEmpty.mk(
            Seq,
            Lifespan.ArchiveOnDeactivate(
              ts(2).forgetRefinement,
              ts(4).forgetRefinement,
              initialReassignmentCounter,
            ): Lifespan,
          ),
        ),
      ),
      (
        coid(1, 0),
        (
          Set(alice, bob),
          NonEmpty.mk(
            Seq,
            Lifespan.UnassignmentDeactivate(
              ts(2).forgetRefinement,
              ts(4).forgetRefinement,
              initialReassignmentCounter,
              reassignmentCounter2,
            ): Lifespan,
            Lifespan
              .UnassignmentDeactivate(
                ts(7).forgetRefinement,
                ts(8).forgetRefinement,
                reassignmentCounter2,
                reassignmentCounter2,
              ): Lifespan,
          ),
        ),
      ),
      (
        coid(2, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan.UnassignmentDeactivate(
              ts(7).forgetRefinement,
              ts(8).forgetRefinement,
              initialReassignmentCounter,
              reassignmentCounter2,
            ): Lifespan,
            Lifespan.UnassignmentDeactivate(
              ts(10).forgetRefinement,
              ts(12).forgetRefinement,
              reassignmentCounter2,
              reassignmentCounter3,
            ): Lifespan,
          ),
        ),
      ),
      (
        coid(3, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan.ArchiveOnDeactivate(
              ts(9).forgetRefinement,
              ts(9).forgetRefinement,
              initialReassignmentCounter,
            ): Lifespan,
          ),
        ),
      ),
    )

    val cs2 = CommitSet(
      creations = Map[LfContractId, CreationCommit](
        coid(0, 0) ->
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
            initialReassignmentCounter,
          ),
        coid(1, 0) ->
          CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
            initialReassignmentCounter,
          ),
      ),
      archivals = Map.empty[LfContractId, ArchivalCommit],
      unassignments = Map.empty[LfContractId, UnassignmentCommit],
      assignments = Map.empty[LfContractId, AssignmentCommit],
    )
    val acs2 = AcsChange.tryFromCommitSet(
      cs2,
      Map.empty[LfContractId, ReassignmentCounter],
      Map.empty[LfContractId, ReassignmentCounter],
    )

    val cs4 = CommitSet(
      creations = Map.empty[LfContractId, CreationCommit],
      archivals = Map[LfContractId, ArchivalCommit](
        coid(0, 0) -> ArchivalCommit(Set(alice, bob))
      ),
      unassignments = Map[LfContractId, UnassignmentCommit](
        coid(1, 0) -> UnassignmentCommit(
          Target(domainId),
          Set(alice, bob),
          reassignmentCounter2,
        )
      ),
      assignments = Map.empty[LfContractId, AssignmentCommit],
    )
    val acs4 = AcsChange.tryFromCommitSet(
      cs4,
      Map[LfContractId, ReassignmentCounter](coid(0, 0) -> initialReassignmentCounter),
      Map.empty[LfContractId, ReassignmentCounter],
    )

    val cs7 = CommitSet(
      creations = Map[LfContractId, CreationCommit](
        coid(2, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None),
          initialReassignmentCounter,
        )
      ),
      archivals = Map.empty[LfContractId, ArchivalCommit],
      unassignments = Map.empty[LfContractId, UnassignmentCommit],
      assignments = Map[LfContractId, AssignmentCommit](
        coid(1, 0) -> AssignmentCommit(
          ReassignmentId(Source(domainId), ts(4).forgetRefinement),
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
          reassignmentCounter2,
        )
      ),
    )
    val acs7 = AcsChange.tryFromCommitSet(
      cs7,
      Map.empty[LfContractId, ReassignmentCounter],
      Map.empty[LfContractId, ReassignmentCounter],
    )

    val cs8 = CommitSet(
      creations = Map.empty[LfContractId, CreationCommit],
      archivals = Map[LfContractId, ArchivalCommit](
        coid(1, 0) -> ArchivalCommit(
          Set(alice, bob)
        )
      ),
      unassignments = Map[LfContractId, UnassignmentCommit](
        coid(2, 0) -> UnassignmentCommit(
          Target(domainId),
          Set(alice, bob, carol),
          reassignmentCounter2,
        )
      ),
      assignments = Map.empty[LfContractId, AssignmentCommit],
    )
    val acs8 =
      AcsChange.tryFromCommitSet(
        cs8,
        Map[LfContractId, ReassignmentCounter](coid(1, 0) -> reassignmentCounter2),
        Map.empty[LfContractId, ReassignmentCounter],
      )

    val cs9 = CommitSet(
      creations = Map[LfContractId, CreationCommit](
        coid(3, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None),
          initialReassignmentCounter,
        )
      ),
      archivals = Map[LfContractId, ArchivalCommit](
        coid(3, 0) -> ArchivalCommit(
          Set(alice, bob, carol)
        )
      ),
      unassignments = Map.empty[LfContractId, UnassignmentCommit],
      assignments = Map.empty[LfContractId, AssignmentCommit],
    )
    val acs9 = AcsChange.tryFromCommitSet(
      cs9,
      Map.empty[LfContractId, ReassignmentCounter],
      Map[LfContractId, ReassignmentCounter](coid(3, 0) -> initialReassignmentCounter),
    )

    val cs10 = CommitSet(
      creations = Map.empty[LfContractId, CreationCommit],
      archivals = Map.empty[LfContractId, ArchivalCommit],
      unassignments = Map.empty[LfContractId, UnassignmentCommit],
      assignments = Map[LfContractId, AssignmentCommit](
        coid(2, 0) -> AssignmentCommit(
          ReassignmentId(Source(domainId), ts(8).forgetRefinement),
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None),
          reassignmentCounter2,
        )
      ),
    )
    val acs10 = AcsChange.tryFromCommitSet(
      cs10,
      Map.empty[LfContractId, ReassignmentCounter],
      Map.empty[LfContractId, ReassignmentCounter],
    )

    val cs12 = CommitSet(
      creations = Map.empty[LfContractId, CreationCommit],
      archivals = Map.empty[LfContractId, ArchivalCommit],
      unassignments = Map[LfContractId, UnassignmentCommit](
        coid(2, 0) -> UnassignmentCommit(
          Target(domainId),
          Set(alice, bob, carol),
          reassignmentCounter3,
        )
      ),
      assignments = Map.empty[LfContractId, AssignmentCommit],
    )
    val acs12 = AcsChange.tryFromCommitSet(
      cs12,
      Map.empty[LfContractId, ReassignmentCounter],
      Map.empty[LfContractId, ReassignmentCounter],
    )

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
            Lifespan.ArchiveOnDeactivate(
              ts(2).forgetRefinement,
              ts(4).forgetRefinement,
              initialReassignmentCounter,
            ),
          ),
        ),
      ),
      (
        coid(1, 0),
        (
          Set(alice, bob),
          NonEmpty.mk(
            Seq,
            Lifespan.ArchiveOnDeactivate(
              ts(2).forgetRefinement,
              ts(6).forgetRefinement,
              initialReassignmentCounter,
            ),
          ),
        ),
      ),
      (
        coid(2, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan.ArchiveOnDeactivate(
              ts(2).forgetRefinement,
              ts(10).forgetRefinement,
              initialReassignmentCounter,
            ),
          ),
        ),
      ),
      (
        coid(3, 0),
        (
          Set(alice, bob, carol),
          NonEmpty.mk(
            Seq,
            Lifespan.ArchiveOnDeactivate(
              ts(2).forgetRefinement,
              ts(4).forgetRefinement,
              initialReassignmentCounter,
            ),
          ),
        ),
      ),
      (
        coid(4, 0),
        (
          Set(alice, danna),
          NonEmpty.mk(
            Seq,
            Lifespan.ArchiveOnDeactivate(
              ts(2).forgetRefinement,
              ts(14).forgetRefinement,
              initialReassignmentCounter,
            ),
          ),
        ),
      ),
      (
        coid(5, 0),
        (
          Set(alice, ed),
          NonEmpty.mk(
            Seq,
            Lifespan.ArchiveOnDeactivate(
              ts(2).forgetRefinement,
              ts(18).forgetRefinement,
              initialReassignmentCounter,
            ),
          ),
        ),
      ),
    )

    val cs2 = CommitSet(
      creations = Map[LfContractId, CreationCommit](
        coid(0, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
          initialReassignmentCounter,
        ),
        coid(1, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
          initialReassignmentCounter,
        ),
        coid(2, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None),
          initialReassignmentCounter,
        ),
        coid(3, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, bob, carol), None),
          initialReassignmentCounter,
        ),
        coid(4, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, danna), None),
          initialReassignmentCounter,
        ),
        coid(5, 0) -> CreationCommit(
          ContractMetadata.tryCreate(Set.empty, Set(alice, ed), None),
          initialReassignmentCounter,
        ),
      ),
      archivals = Map.empty[LfContractId, ArchivalCommit],
      unassignments = Map.empty[LfContractId, UnassignmentCommit],
      assignments = Map.empty[LfContractId, AssignmentCommit],
    )
    val acs2 = AcsChange.tryFromCommitSet(
      cs2,
      Map.empty[LfContractId, ReassignmentCounter],
      Map.empty[LfContractId, ReassignmentCounter],
    )

    val cs4 = CommitSet(
      creations = Map.empty[LfContractId, CreationCommit],
      archivals = Map[LfContractId, ArchivalCommit](
        coid(0, 0) -> ArchivalCommit(
          Set(alice, bob)
        ),
        coid(3, 0) -> ArchivalCommit(
          Set(alice, bob, carol)
        ),
      ),
      unassignments = Map.empty[LfContractId, UnassignmentCommit],
      assignments = Map.empty[LfContractId, AssignmentCommit],
    )
    val acs4 = AcsChange.tryFromCommitSet(
      cs4,
      Map[LfContractId, ReassignmentCounter](
        coid(0, 0) -> initialReassignmentCounter,
        coid(3, 0) -> initialReassignmentCounter,
      ),
      Map.empty[LfContractId, ReassignmentCounter],
    )

    val acsChanges = Map(ts(2) -> acs2, ts(4) -> acs4)
    (contracts, acsChanges)
  }

  protected def rt(timestamp: Long, tieBreaker: Int): RecordTime =
    RecordTime(ts(timestamp).forgetRefinement, tieBreaker.toLong)

  protected val coid = (txId, discriminator) =>
    ExampleTransactionFactory.suffixedId(txId, discriminator)
}

class AcsCommitmentProcessorTest
    extends AsyncWordSpec
    with AcsCommitmentProcessorBaseTest
    with ProtocolVersionChecksAsyncWordSpec {
  // This is duplicating the internal logic of the commitment computation, but I don't have a better solution at the moment
  // if we want to test whether commitment buffering works
  // Also assumes that all the contracts in the map have the same stakeholders
  private def stakeholderCommitment(
      contracts: Map[LfContractId, ReassignmentCounter]
  ): AcsCommitment.CommitmentType = {
    val h = LtHash16()
    contracts.keySet.foreach { cid =>
      h.add(
        (cid.encodeDeterministically
          concat ReassignmentCounter.encodeDeterministically(contracts(cid))).toByteArray
      )
    }
    h.getByteString()
  }

  private def participantCommitment(
      stakeholderCommitments: List[AcsCommitment.CommitmentType]
  ): AcsCommitment.CommitmentType = {
    val unionHash = LtHash16()
    stakeholderCommitments.foreach(h => unionHash.add(h.toByteArray))
    unionHash.getByteString()
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
      stkhd.intersect(localParties).nonEmpty && (stkhd.intersect(remoteParticipantParties).nonEmpty)

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
      .filter { case (p, comm) => comm != LtHash16().getByteString() }
  }

  private def commitmentMsg(
      params: (
          ParticipantId,
          Map[LfContractId, ReassignmentCounter],
          CantonTimestampSecond,
          CantonTimestampSecond,
          Option[PositiveSeconds],
      )
  ): Future[SignedProtocolMessage[AcsCommitment]] = {
    val (remote, contracts, fromExclusive, toInclusive, reconciliationInterval) = params

    val syncCrypto =
      TestingTopology()
        .withSimpleParticipants(remote)
        .build(crypto, loggerFactory)
        .forOwnerAndDomain(remote)
    // we assume that the participant has a single stakeholder group
    val cmt = commitmentsFromStkhdCmts(Seq(stakeholderCommitment(contracts)))
    val snapshotF = syncCrypto.snapshot(CantonTimestamp.Epoch)
    val period =
      CommitmentPeriod
        .create(
          fromExclusive.forgetRefinement,
          toInclusive.forgetRefinement,
          reconciliationInterval.getOrElse(interval),
        )
        .value
    val payload =
      AcsCommitment.create(domainId, remote, localId, period, cmt, testedProtocolVersion)

    snapshotF.flatMap { snapshot =>
      SignedProtocolMessage
        .trySignAndCreate(payload, snapshot, testedProtocolVersion)
        .failOnShutdown
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
        .map { case (cid, (ts, reassignmentCounter)) =>
          cid -> (stakeholderLookup(cid), reassignmentCounter)
        }
        .groupBy { case (_, (stakeholder, _)) => stakeholder }
        .map {
          case (stkhs, m) => {
            logger.debug(
              s"adding to commitment for stakeholders $stkhs the parts cid and reassignment counter in $m"
            )
            SortedSet(stkhs.toList*) -> stakeholderCommitment(m.map {
              case (cid, (_, reassignmentCounter)) => (cid, reassignmentCounter)
            })
          }
        }
      res <- AcsCommitmentProcessor
        .commitments(
          localId,
          byStkhSet,
          crypto,
          at,
          None,
          parallelism,
          new CachedCommitments(),
        )
        .failOnShutdown
    } yield res
  }

  // add a fixed contract id and custom reassignment counter
  // and return active and delta-added commitments (byte strings)
  private def addCommonContractId(
      rc: RunningCommitments,
      reassignmentCounter: ReassignmentCounter,
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
        commonContractId ->
          ContractStakeholdersAndReassignmentCounter(
            Set(alice, bob),
            reassignmentCounter,
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
        res <- PruningProcessor
          .safeToPrune_(
            cleanReplayF = Future.successful(CantonTimestamp.MinValue),
            commitmentsPruningBound =
              CommitmentsPruningBound.Outstanding(_ => Future.successful(None)),
            earliestInFlightSubmissionF = Future.successful(None),
            sortedReconciliationIntervalsProvider =
              constantSortedReconciliationIntervalsProvider(longInterval),
            domainId,
          )
          .failOnShutdown
      } yield res shouldBe None
    }

    "compute safeToPrune timestamp with no clean replay timestamp" in {
      val longInterval = PositiveSeconds.tryOfDays(100)
      for {
        res <- PruningProcessor
          .safeToPrune_(
            cleanReplayF = Future.successful(CantonTimestamp.MinValue),
            commitmentsPruningBound = CommitmentsPruningBound.Outstanding(_ =>
              Future.successful(Some(CantonTimestamp.MinValue))
            ),
            earliestInFlightSubmissionF = Future.successful(None),
            sortedReconciliationIntervalsProvider =
              constantSortedReconciliationIntervalsProvider(longInterval),
            domainId,
          )
          .failOnShutdown
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

        PruningProcessor
          .safeToPrune_(
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
          .failOnShutdown
      }

      for {
        res1 <- safeToPrune(true)
        res2 <- safeToPrune(false)
        sortedReconciliationIntervals <- sortedReconciliationIntervalsProvider
          .reconciliationIntervals(now)
          .failOnShutdown
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
              Lifespan.ArchiveOnDeactivate(
                ts(2).forgetRefinement,
                ts(4).forgetRefinement,
                initialReassignmentCounter,
              ),
            ),
          ),
        ),
        (
          coid(1, 0),
          (
            Set(alice, bob),
            NonEmpty.mk(
              Seq,
              Lifespan.ArchiveOnDeactivate(
                ts(2).forgetRefinement,
                ts(5).forgetRefinement,
                initialReassignmentCounter,
              ),
            ),
          ),
        ),
        (
          coid(2, 0),
          (
            Set(alice, bob, carol),
            NonEmpty.mk(
              Seq,
              Lifespan.ArchiveOnDeactivate(
                ts(7).forgetRefinement,
                ts(8).forgetRefinement,
                initialReassignmentCounter,
              ),
            ),
          ),
        ),
        (
          coid(3, 0),
          (
            Set(alice, bob, carol),
            NonEmpty.mk(
              Seq,
              Lifespan.ArchiveOnDeactivate(
                ts(9).forgetRefinement,
                ts(9).forgetRefinement,
                initialReassignmentCounter,
              ),
            ),
          ),
        ),
        (
          coid(4, 0),
          (
            Set(bob, carol),
            NonEmpty.mk(
              Seq,
              Lifespan.ArchiveOnDeactivate(
                ts(11).forgetRefinement,
                ts(13).forgetRefinement,
                initialReassignmentCounter,
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
        res1 <- AcsCommitmentProcessor
          .commitments(
            localId,
            snapshot1,
            crypto,
            ts(0),
            None,
            parallelism,
            new CachedCommitments(),
          )
          .failOnShutdown
        res2 <- AcsCommitmentProcessor
          .commitments(
            localId,
            snapshot2,
            crypto,
            ts(0),
            None,
            parallelism,
            new CachedCommitments(),
          )
          .failOnShutdown
        res3 <- AcsCommitmentProcessor
          .commitments(
            localId,
            snapshot3,
            crypto,
            ts(0),
            None,
            parallelism,
            new CachedCommitments(),
          )
          .failOnShutdown
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
          (Set(alice, bob), toc(1), toc(9), initialReassignmentCounter, initialReassignmentCounter),
        ),
        (
          coid(0, 1),
          (
            Set(alice, carol),
            toc(9),
            toc(12),
            initialReassignmentCounter,
            initialReassignmentCounter,
          ),
        ),
        (
          coid(1, 0),
          (
            Set(alice, carol),
            toc(1),
            toc(3),
            initialReassignmentCounter,
            initialReassignmentCounter,
          ),
        ),
      )

      val topology = Map(
        localId -> Set(alice),
        remoteId1 -> Set(bob),
        remoteId2 -> Set(carol),
      )

      val (proc, store, sequencerClient, changes) =
        testSetupDontPublish(timeProofs, contractSetup, topology)

      val remoteCommitments = List(
        (remoteId1, Map((coid(0, 0), initialReassignmentCounter)), ts(0), ts(5), None),
        (remoteId2, Map((coid(0, 1), initialReassignmentCounter)), ts(5), ts(10), None),
      )

      for {
        processor <- proc.onShutdown(fail())
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
        sequencerClient.requests.size shouldBe 2
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
          (Set(alice, bob), toc(8), toc(12), initialReassignmentCounter, initialReassignmentCounter),
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

      val (proc, store, _) = testSetup(
        timeProofs,
        contractSetup,
        topology,
        overrideDefaultSortedReconciliationIntervalsProvider =
          Some(sortedReconciliationIntervalsProvider),
      )

      val remoteCommitments =
        List((remoteId1, Map((coid(0, 0), initialReassignmentCounter)), ts(5), ts(10), None))

      for {
        processor <- proc.onShutdown(fail())
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
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      for {
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(0), CantonTimestamp.Epoch, CantonTimestamp.Epoch, None)
        )
        res <- PruningProcessor
          .latestSafeToPruneTick(
            requestJournalStore,
            DomainIndex.of(
              RequestIndex(
                counter = RequestCounter(0L),
                sequencerCounter = Some(SequencerCounter(0L)),
                timestamp = CantonTimestamp.Epoch,
              )
            ),
            constantSortedReconciliationIntervalsProvider(defaultReconciliationInterval),
            acsCommitmentStore,
            inFlightSubmissionStore,
            domainId,
            checkForOutstandingCommitments = true,
          )
          .failOnShutdown
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
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      for {
        res <- PruningProcessor
          .latestSafeToPruneTick(
            requestJournalStore,
            DomainIndex.empty,
            constantSortedReconciliationIntervalsProvider(defaultReconciliationInterval),
            acsCommitmentStore,
            inFlightSubmissionStore,
            domainId,
            checkForOutstandingCommitments = true,
          )
          .failOnShutdown
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
      val inFlightSubmissionStore = new InMemoryInFlightSubmissionStore(loggerFactory)

      val ts0 = CantonTimestamp.Epoch
      val ts1 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis)
      val ts2 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 2)
      val ts3 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 3)
      val ts4 = CantonTimestamp.ofEpochMilli(requestTsDelta.toMillis * 5)
      for {
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(0), ts0, ts0))
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(1), ts1, ts3.plusMillis(1))
        ) // RC1 commits after RC3
        _ <- requestJournalStore.insert(RequestData.clean(RequestCounter(2), ts2, ts2))
        _ <- requestJournalStore.insert(
          RequestData(RequestCounter(3), RequestState.Pending, ts3, None)
        )
        res1 <- PruningProcessor
          .latestSafeToPruneTick(
            requestJournalStore,
            DomainIndex(
              Some(
                RequestIndex(
                  counter = RequestCounter(2L),
                  sequencerCounter = None,
                  timestamp = ts2,
                )
              ),
              Some(
                SequencerIndex(
                  counter = SequencerCounter(3L),
                  timestamp = ts3,
                )
              ),
            ),
            sortedReconciliationIntervalsProvider,
            acsCommitmentStore,
            inFlightSubmissionStore,
            domainId,
            checkForOutstandingCommitments = true,
          )
          .failOnShutdown
        _ <- requestJournalStore.insert(
          RequestData(RequestCounter(4), RequestState.Pending, ts4, None)
        ) // Replay starts at ts4
        _ <- requestJournalStore
          .replace(RequestCounter(3), ts3, RequestState.Clean, Some(ts3))
          .valueOrFail("advance RC 3 to clean")
        res2 <- PruningProcessor
          .latestSafeToPruneTick(
            requestJournalStore,
            DomainIndex(
              Some(
                RequestIndex(
                  counter = RequestCounter(3L),
                  sequencerCounter = None,
                  timestamp = ts3,
                )
              ),
              Some(
                SequencerIndex(
                  counter = SequencerCounter(4L),
                  timestamp = ts4,
                )
              ),
            ),
            sortedReconciliationIntervalsProvider,
            acsCommitmentStore,
            inFlightSubmissionStore,
            domainId,
            checkForOutstandingCommitments = true,
          )
          .failOnShutdown
      } yield {
        withClue("request 1:") {
          assertInIntervalBefore(ts1, reconciliationInterval)(res1)
        } // Do not prune request 1
        // Do not prune request 1 as crash recovery may delete the inflight validation request 4 and then we're back in the same situation as for res1
        withClue("request 3:") {
          assertInIntervalBefore(ts1, reconciliationInterval)(res2)
        }
      }
    }

    "prevent pruning of the last request known to be clean" in {
      val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)
      val requestTsDelta = 20.seconds

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
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
        _ <- requestJournalStore.insert(
          RequestData.clean(RequestCounter(2), tsCleanRequest, tsCleanRequest)
        )
        _ <- requestJournalStore.insert(RequestData(RequestCounter(3), RequestState.Pending, ts3))
        res <- PruningProcessor
          .latestSafeToPruneTick(
            requestJournalStore,
            DomainIndex(
              Some(
                RequestIndex(
                  counter = RequestCounter(2L),
                  sequencerCounter = None,
                  timestamp = tsCleanRequest,
                )
              ),
              Some(
                SequencerIndex(
                  counter = SequencerCounter(4L),
                  timestamp = ts3,
                )
              ),
            ),
            sortedReconciliationIntervalsProvider,
            acsCommitmentStore,
            inFlightSubmissionStore,
            domainId,
            checkForOutstandingCommitments = true,
          )
          .failOnShutdown
      } yield assertInIntervalBefore(tsCleanRequest, reconciliationInterval)(res)
    }

    "prevent pruning of events corresponding to in-flight requests" in {
      val reconciliationInterval = PositiveSeconds.tryOfSeconds(1)
      val requestTsDelta = 20.seconds

      val changeId1 = mkChangeIdHash(1)
      val changeId2 = mkChangeIdHash(2)

      val submissionId = LedgerSubmissionId.assertFromString("submission-id").some

      val requestJournalStore = new InMemoryRequestJournalStore(loggerFactory)
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
        testeeSafeToPrune = () =>
          PruningProcessor
            .latestSafeToPruneTick(
              requestJournalStore,
              DomainIndex(
                Some(
                  RequestIndex(
                    counter = RequestCounter(3L),
                    sequencerCounter = None,
                    timestamp = tsCleanRequest2,
                  )
                ),
                Some(
                  SequencerIndex(
                    counter = SequencerCounter(1L),
                    timestamp = tsCleanRequest2,
                  )
                ),
              ),
              sortedReconciliationIntervalsProvider,
              acsCommitmentStore,
              inFlightSubmissionStore,
              domainId,
              checkForOutstandingCommitments = true,
            )
            .failOnShutdown
        res1 <- testeeSafeToPrune()
        // Now remove the timed-out submission 1 and compute the pruning point again
        () <- inFlightSubmissionStore.delete(Seq(submission1.referenceByMessageId))
        res2 <- testeeSafeToPrune()
        // Now remove the clean request and compute the pruning point again
        () <- inFlightSubmissionStore.delete(Seq(submission2.referenceByMessageId))
        res3 <- testeeSafeToPrune()
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
          coid(0, 0) ->
            ContractStakeholdersAndReassignmentCounter(
              Set(alice, bob),
              initialReassignmentCounter,
            ),
          coid(0, 1) ->
            ContractStakeholdersAndReassignmentCounter(
              Set(bob, carol),
              initialReassignmentCounter,
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
          coid(0, 0) -> ContractStakeholdersAndReassignmentCounter(
            Set(alice, bob),
            initialReassignmentCounter,
          )
        ),
        activations = Map(
          coid(1, 1) ->
            ContractStakeholdersAndReassignmentCounter(
              Set(alice, carol),
              initialReassignmentCounter,
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
          coid(2, 1) ->
            ContractStakeholdersAndReassignmentCounter(
              Set(alice, carol),
              initialReassignmentCounter,
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

    "contracts differing by reassignment counter result in different commitments if the PV support reassignment counters" in {
      val rc1 =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)
      val rc2 =
        new pruning.AcsCommitmentProcessor.RunningCommitments(RecordTime.MinValue, TrieMap.empty)
      val reassignmentCounter2 = initialReassignmentCounter + 1

      val (activeCommitment1, deltaAddedCommitment1) =
        addCommonContractId(rc1, initialReassignmentCounter)
      val (activeCommitment2, deltaAddedCommitment2) =
        addCommonContractId(rc2, reassignmentCounter2)

      activeCommitment1 should not be activeCommitment2
      deltaAddedCommitment1 should not be deltaAddedCommitment2
    }

    "transient contracts in a commit set obtain the correct reassignment counter for archivals, hence do not appear in the ACS change" in {
      val cid1 = coid(0, 1)
      val cid2 = coid(0, 2)
      val cid3 = coid(0, 3)
      val cid4 = coid(0, 4)
      val reassignmentCounter1 = initialReassignmentCounter + 1
      val reassignmentCounter2 = initialReassignmentCounter + 2
      val reassignmentCounter3 = initialReassignmentCounter + 3

      val cs = CommitSet(
        creations = Map[LfContractId, CreationCommit](
          cid1.leftSide -> CommitSet.CreationCommit(
            ContractMetadata.tryCreate(Set.empty, Set(alice, bob), None),
            initialReassignmentCounter,
          )
        ),
        archivals = Map[LfContractId, ArchivalCommit](
          cid1.leftSide -> CommitSet.ArchivalCommit(Set(alice, bob)),
          cid3.leftSide -> CommitSet.ArchivalCommit(Set(bob)),
          cid4.leftSide -> CommitSet.ArchivalCommit(Set(alice)),
        ),
        unassignments = Map[LfContractId, UnassignmentCommit](
          cid2.leftSide -> CommitSet
            .UnassignmentCommit(Target(domainId), Set(alice), reassignmentCounter2)
        ),
        assignments = Map[LfContractId, AssignmentCommit](
          cid3.leftSide -> CommitSet.AssignmentCommit(
            ReassignmentId(Source(domainId), CantonTimestamp.Epoch),
            ContractMetadata.tryCreate(Set.empty, Set(bob), None),
            reassignmentCounter1,
          )
        ),
      )

      val reassignmentCounterOfArchival =
        Map[LfContractId, ReassignmentCounter](cid4 -> reassignmentCounter3)

      val reassignmentCountersForArchivedTransient =
        AcsChange.reassignmentCountersForArchivedTransient(cs)
      val acs1 =
        AcsChange.tryFromCommitSet(
          cs,
          reassignmentCounterOfArchival,
          reassignmentCountersForArchivedTransient,
        )

      // cid1 is a transient creation with reassignment counter initialReassignmentCounter and should not appear in the ACS change
      reassignmentCountersForArchivedTransient
        .get(cid1)
        .fold(
          fail(
            s"$cid1 should be transient, but is not in $reassignmentCountersForArchivedTransient"
          )
        )(_ shouldBe initialReassignmentCounter)
      acs1.activations.get(cid1) shouldBe None
      acs1.deactivations.get(cid1) shouldBe None
      // cid3 is a transient assignment and should not appear in the ACS change
      reassignmentCountersForArchivedTransient
        .get(cid3)
        .fold(
          fail(
            s"$cid3 should be transient, but is not in $reassignmentCountersForArchivedTransient"
          )
        )(_ shouldBe reassignmentCounter1)
      acs1.activations.get(cid3) shouldBe None
      acs1.deactivations.get(cid3) shouldBe None
      // unassignment cid2 is a deactivation with reassignment counter reassignmentCounter2
      acs1.deactivations(cid2.leftSide).reassignmentCounter shouldBe reassignmentCounter1
      // archival of cid4 is a deactivation with reassignment counter reassignmentCounter3
      acs1.deactivations(cid4.leftSide).reassignmentCounter shouldBe reassignmentCounter3
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
      val acsF = acsSetup(contractSetup.fmap { case (stkhd, lifespan) =>
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
      ): Future[Assertion] =
        for {
          config <- processor.catchUpConfig(cantonTimestamp).failOnShutdown
        } yield {
          config match {
            case Some(cfg) =>
              assert(cfg.nrIntervalsToTriggerCatchUp == nrIntervalsToTriggerCatchUp)
              assert(cfg.catchUpIntervalSkip == catchUpIntervalSkip)
            case None => fail("catch up mode needs to be enabled")
          }
        }

      def checkCatchUpModeCfgDisabled(
          processor: pruning.AcsCommitmentProcessor,
          cantonTimestamp: CantonTimestamp,
      ): Future[Assertion] =
        for {
          config <- processor.catchUpConfig(cantonTimestamp).failOnShutdown
        } yield {
          config match {
            case Some(cfg)
                if cfg.catchUpIntervalSkip == AcsCommitmentsCatchUpConfig
                  .disabledCatchUp()
                  .catchUpIntervalSkip && cfg.nrIntervalsToTriggerCatchUp == AcsCommitmentsCatchUpConfig
                  .disabledCatchUp()
                  .nrIntervalsToTriggerCatchUp =>
              succeed
            case Some(cfg) => fail(s"Canton config is defined ($cfg) at $cantonTimestamp")
            case None => succeed
          }
        }

      "enter catch up mode when processing falls behind" in {
        val timeProofs = List(3L, 8, 20, 35, 59).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob),
              toc(1),
              toc(9),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(0, 1),
            (
              Set(alice, carol),
              toc(11),
              toc(21),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(1, 0),
            (
              Set(alice, carol),
              toc(18),
              toc(33),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
          )

        val remoteCommitments = List(
          (remoteId1, Map((coid(0, 0), initialReassignmentCounter)), ts(0), ts(5), None),
          (remoteId2, Map((coid(0, 1), initialReassignmentCounter)), ts(10), ts(15), None),
          (
            remoteId2,
            Map((coid(1, 0), initialReassignmentCounter), (coid(0, 1), initialReassignmentCounter)),
            ts(15),
            ts(20),
            None,
          ),
          (remoteId2, Map((coid(1, 0), initialReassignmentCounter)), ts(20), ts(25), None),
          (remoteId2, Map((coid(1, 0), initialReassignmentCounter)), ts(25), ts(30), None),
        )

        for {
          processor <- proc.onShutdown(fail())
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
          computedAll <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          computed = computedAll.filter(_._2 != localId)
          received <- store.searchReceivedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
        } yield {
          // the participant catches up to ticks 10, 20, 30
          // the only ticks with non-empty commitments are at 20 and 30, and they match the remote ones,
          // therefore there are 2 sends of commitments
          sequencerClient.requests.size shouldBe 2
          // compute commitments only for interval ends 20 and 30
          assert(computed.size === 2)
          assert(received.size === 5)
          // all local commitments were matched and can be pruned
          assert(outstanding == Some(toc(55).timestamp))
        }
      }

      "catch up parameters overflow causes exception" in {
        assertThrows[IllegalArgumentException]({
          new AcsCommitmentsCatchUpConfig(
            PositiveInt.tryCreate(Int.MaxValue / 2),
            PositiveInt.tryCreate(Int.MaxValue / 2),
          )
        })
      }

      "catch up parameters (1,1) throws exception" in {
        assertThrows[IllegalArgumentException]({
          new AcsCommitmentsCatchUpConfig(
            PositiveInt.tryCreate(1),
            PositiveInt.tryCreate(1),
          )
        })
      }

      "catch up with maximum reconciliation interval and catch-up parameters logs error" in {
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          {
            // maximum reconciliation interval in seconds allowed by the CantonTimestamp
            // the last division by 11 is to have valid CantonTimestamps for all the time bounds in the test sequences
            val reconciliationInterval = CantonTimestamp.MaxValue.getEpochSecond / 11 - 1
            val testSequences =
              (1 to 10)
                .map(i => i * reconciliationInterval)
                .map(CantonTimestamp.ofEpochSecond)
                .toList

            val contractSetup = Map(
              // contract ID to stakeholders, creation and archival time
              (
                coid(0, 0),
                (
                  Set(alice, bob),
                  toc(1),
                  toc(CantonTimestamp.MaxValue.getEpochSecond),
                  initialReassignmentCounter,
                  initialReassignmentCounter,
                ),
              )
            )

            val topology = Map(
              localId -> Set(alice),
              remoteId1 -> Set(bob),
            )

            // maximum catch-up config parameters so that their multiplication is allowed
            val startConfig =
              new AcsCommitmentsCatchUpConfig(
                PositiveInt.tryCreate(Int.MaxValue / 8),
                PositiveInt.tryCreate(8),
              )
            val startConfigWithValidity = DomainParameters.WithValidity(
              validFrom = CantonTimestamp.MinValue,
              validUntil = Some(CantonTimestamp.MaxValue),
              parameter = defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter =
                Some(startConfig)
              ),
            )

            val (proc, store, sequencerClient, changes) =
              testSetupDontPublish(
                testSequences,
                contractSetup,
                topology,
                acsCommitmentsCatchUpModeEnabled = true,
                domainParametersUpdates = List(startConfigWithValidity),
                overrideDefaultSortedReconciliationIntervalsProvider = Some(
                  constantSortedReconciliationIntervalsProvider(
                    PositiveSeconds.tryOfSeconds(reconciliationInterval.toLong)
                  )
                ),
              )

            for {
              processor <- proc.onShutdown(fail())
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
                  processor.publish(RecordTime(ts, tb.v), change, Future.unit)
                }
              _ <- processor.flush()
              _ <- testSequence(
                testSequences,
                processor,
                changes,
                store,
                reconciliationInterval.longValue,
                noLogSuppression = true,
                justProcessingNoChecks = true,
              )
            } yield {
              succeed
            }
          },
          // the computed timestamp to catch up to represents an out of bound CantonTimestamp, therefore we log an error
          LogEntry.assertLogSeq(
            Seq(
              (
                _.message should (include("Error when computing the catch up timestamp")),
                "invalid catchUpTo timestamp did not cause an error",
              )
            )
          ),
        )
      }

      "catch up in correct skip steps scenario1" in {
        val reconciliationInterval = 5L
        val testSequences =
          (1L to 14)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob),
              toc(1),
              toc(20000),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig =
          new AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(2), PositiveInt.tryCreate(3))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(CantonTimestamp.MaxValue),
          parameter =
            defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = Some(startConfig)),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity),
          )

        for {
          processor <- proc.onShutdown(fail())
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
              processor.publish(RecordTime(ts, tb.v), change, Future.unit)
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
          _ = sequencerClient.requests.size shouldBe 9
        } yield {
          succeed
        }
      }

      "catch up in correct skip steps scenario2" in {
        val reconciliationInterval = 5L
        val testSequences =
          (1L to 45)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob),
              toc(1),
              toc(20000),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig =
          new AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(10), PositiveInt.tryCreate(2))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(CantonTimestamp.MaxValue),
          parameter =
            defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = Some(startConfig)),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity),
          )

        for {
          processor <- proc.onShutdown(fail())
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
              processor.publish(RecordTime(ts, tb.v), change, Future.unit)
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
          _ = sequencerClient.requests.size shouldBe 10
        } yield {
          succeed
        }
      }

      "pruning works correctly for a participant ahead of a counter-participant that catches up" in {
        val timeProofs = List(5L, 10, 15, 20, 25, 30).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob),
              toc(1),
              toc(9),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(0, 1),
            (
              Set(alice, carol),
              toc(11),
              toc(21),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(1, 0),
            (
              Set(alice, carol),
              toc(18),
              toc(33),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
          )

        val remoteCommitments = List(
          (remoteId1, Map((coid(0, 0), initialReassignmentCounter)), ts(0), ts(5), None),
          (remoteId2, Map((coid(0, 1), initialReassignmentCounter)), ts(10), ts(15), None),
          (
            remoteId2,
            Map((coid(1, 0), initialReassignmentCounter), (coid(0, 1), initialReassignmentCounter)),
            ts(15),
            ts(20),
            None,
          ),
          (remoteId2, Map((coid(1, 0), initialReassignmentCounter)), ts(20), ts(30), None),
        )

        for {
          processor <- proc.onShutdown(fail())
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
          sequencerClient.requests.size shouldBe 5
          assert(computed.size === 5)
          assert(received.size === 4)
          // all local commitments were matched and can be pruned
          assert(outstanding == Some(toc(30).timestamp))
        }
      }

      "send skipped commitments on mismatch during catch-up" in {

        val timeProofs = List(3L, 8, 20, 35, 59).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob),
              toc(1),
              toc(9),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(0, 1),
            (
              Set(alice, carol),
              toc(11),
              toc(21),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(1, 0),
            (
              Set(alice, carol),
              toc(18),
              toc(33),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
          )

        val remoteCommitments = List(
          (remoteId1, Map((coid(0, 0), initialReassignmentCounter)), ts(0), ts(5), None),
          (remoteId2, Map((coid(0, 1), initialReassignmentCounter)), ts(10), ts(15), None),
          // wrong contract, causes mismatch
          (
            remoteId2,
            Map(
              (coid(1, 1), initialReassignmentCounter + 1),
              (coid(2, 1), initialReassignmentCounter + 2),
            ),
            ts(15),
            ts(20),
            None,
          ),
          (remoteId2, Map((coid(1, 0), initialReassignmentCounter)), ts(20), ts(25), None),
          (remoteId2, Map((coid(1, 0), initialReassignmentCounter)), ts(25), ts(30), None),
        )

        for {
          processor <- proc.onShutdown(fail())
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
                processor.publish(RecordTime(ts, tb.v), change, Future.unit)
              }
              for {
                _ <- processor.flush()
              } yield ()
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
          computedAll <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          computed = computedAll.filter(_._2 != localId)
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
          sequencerClient.requests.size shouldBe 3
          // compute commitments for interval ends 10, 20, and one for the mismatch at interval end 15
          assert(computed.size === 3)
          assert(received.size === 5)
          // cannot prune past the mismatch
          assert(outstanding == Some(toc(30).timestamp))
        }
      }

      "dynamically change, disable & re-enable catch-up config during a catch-up" in {
        val reconciliationInterval = 5L
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
            (
              Set(alice, bob),
              toc(1),
              toc(20000),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val midConfig =
          new AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(1), PositiveInt.tryCreate(2))
        val disabledConfig = AcsCommitmentsCatchUpConfig.disabledCatchUp()
        val changedConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.last.head,
          validUntil = None,
          parameter =
            defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = Some(midConfig)),
        )

        val disabledConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.apply(1).head,
          validUntil = Some(testSequences.apply(1).last),
          parameter =
            defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = Some(disabledConfig)),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences.flatten,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
            domainParametersUpdates = List(disabledConfigWithValidity, changedConfigWithValidity),
          )

        for {
          processor <- proc.onShutdown(fail())
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
              processor.publish(RecordTime(ts, tb.v), change, Future.unit)
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
          _ = sequencerClient.requests.size shouldBe 3
          _ <- testSequence(
            testSequences.apply(1),
            processor,
            changes,
            store,
            reconciliationInterval,
          )
          // catchup is disabled so we send all 5 commitments (plus 3 previous)
          _ = sequencerClient.requests.size shouldBe (3 + 5)
          _ <- testSequence(
            testSequences.last,
            processor,
            changes,
            store,
            reconciliationInterval,
            expectDegradation = true,
          )
          // catchup is re-enabled but with a step of 1 so we send 5 commitments (plus 5 & 3 previous)
          _ = sequencerClient.requests.size shouldBe (3 + 5 + 5)
        } yield {
          succeed
        }
      }

      "disable catch-up config during catch-up mode" in {
        val reconciliationInterval = 5L
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
            (
              Set(alice, bob),
              toc(1),
              toc(20000),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig =
          new AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(3), PositiveInt.tryCreate(1))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(changeConfigTimestamp),
          parameter =
            defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = Some(startConfig)),
        )

        val disabledConfigWithValidity = DomainParameters.WithValidity(
          validFrom = changeConfigTimestamp,
          validUntil = None,
          parameter = defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = None),
        )
        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity, disabledConfigWithValidity),
          )

        for {
          processor <- proc.onShutdown(fail())
          _ <- checkCatchUpModeCfgCorrect(
            processor,
            testSequences.head,
            startConfig.nrIntervalsToTriggerCatchUp,
            startConfig.catchUpIntervalSkip,
          )
          _ <- checkCatchUpModeCfgDisabled(processor, testSequences.last)

          // we apply any changes (contract deployment) that happens before our windows
          _ = changes
            .filter(a => a._1 <= testSequences.head)
            .foreach { case (ts, tb, change) =>
              processor.publish(RecordTime(ts, tb.v), change, Future.unit)
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
          _ = sequencerClient.requests.size shouldBe 6
        } yield {
          succeed
        }
      }

      "change catch-up config during catch-up mode" in {
        val reconciliationInterval = 5L
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
            (
              Set(alice, bob),
              toc(1),
              toc(20000),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val startConfig =
          new AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(3), PositiveInt.tryCreate(1))
        val startConfigWithValidity = DomainParameters.WithValidity(
          validFrom = testSequences.head.addMicros(-1),
          validUntil = Some(changeConfigTimestamp),
          parameter =
            defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = Some(startConfig)),
        )

        val changeConfig =
          new AcsCommitmentsCatchUpConfig(PositiveInt.tryCreate(2), PositiveInt.tryCreate(1))
        val changeConfigWithValidity = DomainParameters.WithValidity(
          validFrom = changeConfigTimestamp,
          validUntil = None,
          parameter =
            defaultParameters.tryUpdate(acsCommitmentsCatchUpConfigParameter = Some(changeConfig)),
        )
        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            testSequences,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
            domainParametersUpdates = List(startConfigWithValidity, changeConfigWithValidity),
          )

        for {
          processor <- proc.onShutdown(fail())
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
            .filter(a => a._1 <= testSequences.head)
            .foreach { case (ts, tb, change) =>
              processor.publish(RecordTime(ts, tb.v), change, Future.unit)

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
          _ = sequencerClient.requests.size shouldBe 5
        } yield {
          succeed
        }
      }

      "should mark as unhealthy when not caught up" in {
        val reconciliationInterval = 5L
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
            (
              Set(alice, bob),
              toc(1),
              toc(20000),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
          )

        for {
          processor <- proc.onShutdown(fail())
          // we apply any changes (contract deployment) that happens before our windows
          _ <- Future.successful(
            changes
              .filter(a => a._1 < testSequences.head)
              .foreach { case (ts, tb, change) =>
                processor.publish(RecordTime(ts, tb.v), change, Future.unit)

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
          _ = assert(processor.healthComponent.isDegraded)
        } yield {
          succeed
        }
      }

      def testSequence(
          sequence: List[CantonTimestamp],
          processor: AcsCommitmentProcessor,
          changes: List[(CantonTimestamp, RequestCounter, AcsChange)],
          store: AcsCommitmentStore,
          reconciliationInterval: Long,
          expectDegradation: Boolean = false,
          noLogSuppression: Boolean = false,
          justProcessingNoChecks: Boolean = false,
      ): Future[Assertion] = {
        val remoteCommitments = sequence
          .map(i =>
            (
              remoteId1,
              Map((coid(0, 0), initialReassignmentCounter)),
              ts(i),
              ts(i.plusSeconds(reconciliationInterval)),
              Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
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
            noLogSuppression,
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
            assert(processor.healthComponent.isDegraded)
          else {
            assert(processor.healthComponent.isOk)
          }

          if (!justProcessingNoChecks) {
            if (changesApplied.last._1 >= sequence.last)
              assert(computed.size === sequence.length)
            assert(received.size === sequence.length)
          } else succeed
        }
      }

      "prune correctly on mismatch during catch-up" in {

        val timeProofs = List(3L, 8, 20, 35, 59).map(CantonTimestamp.ofEpochSecond)
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob),
              toc(1),
              toc(9),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(0, 1),
            (
              Set(alice, carol),
              toc(11),
              toc(21),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
          (
            coid(1, 0),
            (
              Set(alice, carol),
              toc(18),
              toc(33),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          ),
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
          )

        val remoteCommitments = List(
          (remoteId1, Map((coid(0, 0), initialReassignmentCounter)), ts(0), ts(5), None),
          (remoteId2, Map((coid(0, 1), initialReassignmentCounter)), ts(10), ts(15), None),
          // wrong contract, causes mismatch
          (
            remoteId2,
            Map(
              (coid(1, 1), initialReassignmentCounter + 1),
              (coid(2, 1), initialReassignmentCounter + 2),
            ),
            ts(15),
            ts(20),
            None,
          ),
          (remoteId2, Map((coid(1, 0), initialReassignmentCounter)), ts(20), ts(25), None),
          // wrong contract, causes mismatch
          (remoteId2, Map((coid(1, 1), initialReassignmentCounter)), ts(25), ts(30), None),
        )

        for {
          processor <- proc.onShutdown(fail())
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

          _ <- loggerFactory.assertLoggedWarningsAndErrorsSeq(
            {
              changes.foreach { case (ts, tb, change) =>
                processor.publish(RecordTime(ts, tb.v), change, Future.unit)
              }
              for {
                _ <- processor.flush()
              } yield ()
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
          computedAll <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          computed = computedAll.filter(_._2 != localId)
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
          sequencerClient.requests.size shouldBe 3
          // compute commitments for interval ends 10, 20, and one for the mismatch at interval end 15
          assert(computed.size === 3)
          assert(received.size === 5)
          // cannot prune past the mismatch 25-30, because there are no commitments that match past this point
          assert(outstanding == Some(toc(25).timestamp))
        }
      }

      "not report errors about skipped commitments due to catch-up mode" in {
        val reconciliationInterval = 5L
        val timeProofs =
          (1L to 7)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob, carol),
              toc(1),
              toc(36),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
          )

        val remoteCommitmentsFast = List(
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(0),
            ts(5),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(5),
            ts(10),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(10),
            ts(15),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(15),
            ts(20),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(20),
            ts(25),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(25),
            ts(30),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
        )

        val remoteCommitmentsNormal = List(
          (
            remoteId2,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(10),
            ts(15),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId2,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(20),
            ts(25),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
        )

        for {
          processor <- proc.onShutdown(fail())
          _ <- checkCatchUpModeCfgCorrect(processor, timeProofs.head)
          remoteFast <- remoteCommitmentsFast.parTraverse(commitmentMsg)
          deliveredFast = remoteFast.map(cmt =>
            (
              cmt.message.period.toInclusive.plusSeconds(1),
              List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
            )
          )
          // First ask for the remote commitments from remoteId1 to be processed,
          // which are up to timestamp 30
          // This causes the local participant to enter catch-up mode
          _ <- deliveredFast
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())

          _ = loggerFactory.assertLogs(
            {
              changes.foreach { case (ts, tb, change) =>
                processor.publish(RecordTime(ts, tb.v), change, Future.unit)
              }
              processor.flush()
            },
            entry => {
              entry.message should include(AcsCommitmentDegradationWithIneffectiveConfig.id)
            },
          )

          // Receive and process remote commitments from remoteId2, for skipped timestamps 10-15 and 20-25
          // The local participant did not compute those commitments because of the catch-up mode, but should
          // not error (in particular, not report NO_SHARED_CONTRACTS)
          remoteNormal <- remoteCommitmentsNormal.parTraverse(commitmentMsg)
          deliveredNormal = remoteNormal.map(cmt =>
            (
              cmt.message.period.toInclusive.plusSeconds(1),
              List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
            )
          )

          _ <- deliveredNormal
            .parTraverse_ { case (ts, batch) =>
              processor.processBatchInternal(ts.forgetRefinement, batch)
            }
            .onShutdown(fail())
          _ <- processor.flush()

          outstanding <- store.noOutstandingCommitments(toc(30).timestamp)
          computed <- store.searchComputedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
          computedCatchUp = computed.filter(_._2 == localId)
          received <- store.searchReceivedBetween(
            CantonTimestamp.Epoch,
            timeProofs.lastOption.value,
          )
        } yield {
          // there are four sends, at the end of each coarse-grained interval 10, 20, 30, and normal 35
          sequencerClient.requests.size shouldBe 4
          // compute commitments for interval ends (10, 20, 30 and 35) x 2, and 3 empty ones for 5,15,25 as catch-up
          assert(computed.size === 11)
          assert(computedCatchUp.forall(_._3 == emptyCommitment) && computedCatchUp.size == 3)
          assert(received.size === 8)
          // cannot prune past the mismatch
          assert(outstanding == Some(toc(25).timestamp))
        }
      }

      "perform match for fine-grained commitments in case of mismatch at catch-up boundary" in {

        val reconciliationInterval = 5L
        val timeProofs =
          (1L to 7)
            .map(i => i * reconciliationInterval)
            .map(CantonTimestamp.ofEpochSecond)
            .toList
        val contractSetup = Map(
          // contract ID to stakeholders, creation and archival time
          (
            coid(0, 0),
            (
              Set(alice, bob, carol),
              toc(1),
              toc(36),
              initialReassignmentCounter,
              initialReassignmentCounter,
            ),
          )
        )

        val topology = Map(
          localId -> Set(alice),
          remoteId1 -> Set(bob),
          remoteId2 -> Set(carol),
        )

        val (proc, store, sequencerClient, changes) =
          testSetupDontPublish(
            timeProofs,
            contractSetup,
            topology,
            acsCommitmentsCatchUpModeEnabled = true,
          )

        val remoteCommitmentsFast = List(
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(0),
            ts(5),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(5),
            ts(10),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          // coid (0,1) is not shared: the mismatch appears here, but is skipped initially during catch-up
          // this commitment is buffered and checked later
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter), (coid(0, 1), initialReassignmentCounter)),
            ts(10),
            ts(15),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          // coid (0,1) is not shared, should cause a mismatch at catch-up boundary and fine-grained sending
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter), (coid(0, 1), initialReassignmentCounter)),
            ts(15),
            ts(20),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(20),
            ts(25),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId1,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(25),
            ts(30),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
        )

        val remoteCommitmentsNormal = List(
          (
            remoteId2,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(15),
            ts(20),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          // coid (0,2) is not shared, but does not cause a mismatch because we hadn't computed fine-grained
          // commitments for remoteId2
          (
            remoteId2,
            Map((coid(0, 0), initialReassignmentCounter), (coid(0, 2), initialReassignmentCounter)),
            ts(10),
            ts(15),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
          (
            remoteId2,
            Map((coid(0, 0), initialReassignmentCounter)),
            ts(20),
            ts(25),
            Some(PositiveSeconds.tryOfSeconds(reconciliationInterval)),
          ),
        )

        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          for {
            processor <- proc.onShutdown(fail())
            _ <- checkCatchUpModeCfgCorrect(processor, timeProofs.head)
            remoteFast <- remoteCommitmentsFast.parTraverse(commitmentMsg)
            deliveredFast = remoteFast.map(cmt =>
              (
                cmt.message.period.toInclusive.plusSeconds(1),
                List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
              )
            )
            // First ask for the remote commitments from remoteId1 to be processed,
            // which are up to timestamp 30
            // This causes the local participant to enter catch-up mode, and observe mismatch for timestamp 20,
            // and fine-grained compute and send commitment 10-15
            _ <- deliveredFast
              .parTraverse_ { case (ts, batch) =>
                processor.processBatchInternal(ts.forgetRefinement, batch)
              }
              .onShutdown(fail())

            _ = changes.foreach { case (ts, tb, change) =>
              processor.publish(RecordTime(ts, tb.v), change, Future.unit)
            }
            _ <- processor.flush()

            // Receive and process remote commitments from remoteId2, for skipped timestamps 10-15 and 20-25
            // The local participant did not compute commitment 20-25 because of the catch-up mode, but should
            // not error (in particular, not report NO_SHARED_CONTRACTS)
            // The local participant computed commitment 10-15 because of the mismatch at 20, and should issue
            // a mismatch for that period
            remoteNormal <- remoteCommitmentsNormal.parTraverse(commitmentMsg)
            deliveredNormal = remoteNormal.map(cmt =>
              (
                cmt.message.period.toInclusive.plusSeconds(1),
                List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
              )
            )

            _ <- deliveredNormal
              .parTraverse_ { case (ts, batch) =>
                processor.processBatchInternal(ts.forgetRefinement, batch)
              }
              .onShutdown(fail())
            _ <- processor.flush()

            outstanding <- store.noOutstandingCommitments(toc(30).timestamp)
            computed <- store.searchComputedBetween(
              CantonTimestamp.Epoch,
              timeProofs.lastOption.value,
            )
            computedCatchUp = computed.filter(_._2 == localId)
            received <- store.searchReceivedBetween(
              CantonTimestamp.Epoch,
              timeProofs.lastOption.value,
            )
          } yield {
            // there are five sends, at the end of each coarse-grained interval 10, 15, 20, 30, and normal 35
            sequencerClient.requests.size shouldBe 5
            // compute commitments for interval ends (10, 15, 20, 30 and 35) x 2, and 3 empty ones for 5,15,25 as catch-up
            assert(computed.size === 13)
            assert(computedCatchUp.forall(_._3 == emptyCommitment) && computedCatchUp.size == 3)
            assert(received.size === 9)
            // cannot prune past the mismatch
            assert(outstanding == Some(toc(25).timestamp))
          },
          LogEntry.assertLogSeq(
            Seq(
              (
                _.shouldBeCantonErrorCode(
                  AcsCommitmentProcessor.Errors.DegradationError.AcsCommitmentDegradation
                ),
                "",
              ),
              (
                _.shouldBeCantonError(
                  AcsCommitmentProcessor.Errors.MismatchError.CommitmentsMismatch,
                  _ => succeed,
                  _("remote") should (include(s"sender = $remoteId1") and include(
                    "period = CommitmentPeriod(fromExclusive = 1970-01-01T00:00:15Z, toInclusive = 1970-01-01T00:00:20Z)"
                  )),
                ),
                s"mismatch at interval 15-20 with $remoteId1",
              ),
              (
                _.shouldBeCantonError(
                  AcsCommitmentProcessor.Errors.MismatchError.CommitmentsMismatch,
                  _ => succeed,
                  _("remote") should (include(s"sender = $remoteId1") and include(
                    "period = CommitmentPeriod(fromExclusive = 1970-01-01T00:00:10Z, toInclusive = 1970-01-01T00:00:15Z)"
                  )),
                ),
                s"mismatch at interval 10-15 with buffered commitment from $remoteId1",
              ),
              (
                _.shouldBeCantonError(
                  AcsCommitmentProcessor.Errors.MismatchError.CommitmentsMismatch,
                  _ => succeed,
                  _("remote") should (include(s"sender = $remoteId2") and include(
                    "period = CommitmentPeriod(fromExclusive = 1970-01-01T00:00:10Z, toInclusive = 1970-01-01T00:00:15Z)"
                  )),
                ),
                s"mismatch at interval 10-15 with incoming commitment from $remoteId2",
              ),
            )
          ),
        )
      }
    }
    def processChanges(
        processor: AcsCommitmentProcessor,
        store: AcsCommitmentStore,
        changes: List[(CantonTimestamp, RequestCounter, AcsChange)],
        noLogSuppression: Boolean = false,
    ): Future[Unit] = {
      lazy val fut = {
        changes.foreach { case (ts, tb, change) =>
          processor.publish(RecordTime(ts, tb.v), change, Future.unit)
        }
        processor.flush()
      }
      for {
        config <- processor.catchUpConfig(changes.head._1).failOnShutdown
        remote <- store.searchReceivedBetween(changes.head._1, changes.last._1)
        _ <- config match {
          case _ if (remote.isEmpty || noLogSuppression) => fut
          case None => fut
          case Some(cfg) if !cfg.isCatchUpEnabled() => fut
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

        val inMemoryCommitmentStore =
          new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          // init phase
          rc <- runningCommitments
          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          byParticipant2 <- AcsCommitmentProcessor
            .stakeholderCommitmentsPerParticipant(
              localId,
              rc.snapshot().active,
              crypto,
              ts(2),
              parallelism,
            )
            .failOnShutdown
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

          byParticipant <- AcsCommitmentProcessor
            .stakeholderCommitmentsPerParticipant(
              localId,
              rc.snapshot().active,
              crypto,
              ts(4),
              parallelism,
            )
            .failOnShutdown

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

        val inMemoryCommitmentStore =
          new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          rc <- runningCommitments

          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          normalCommitments2 <- AcsCommitmentProcessor
            .commitments(
              localId,
              rc.snapshot().active,
              crypto,
              ts(2),
              None,
              parallelism,
              // behaves as if we don't use caching, because we don't reuse this object for further computation
              new CachedCommitments(),
            )
            .failOnShutdown
          cachedCommitments2 <- AcsCommitmentProcessor
            .commitments(
              localId,
              rc.snapshot().active,
              crypto,
              ts(2),
              None,
              parallelism,
              cachedCommitments,
            )
            .failOnShutdown

          _ = rc.update(rt(4, 0), acsChanges(ts(4)))
          normalCommitments4 <- AcsCommitmentProcessor
            .commitments(
              localId,
              rc.snapshot().active,
              crypto,
              ts(4),
              None,
              parallelism,
              // behaves as if we don't use caching, because we don't reuse this object for further computation
              new CachedCommitments(),
            )
            .failOnShutdown
          cachedCommitments4 <- AcsCommitmentProcessor
            .commitments(
              localId,
              rc.snapshot().active,
              crypto,
              ts(4),
              None,
              parallelism,
              cachedCommitments,
            )
            .failOnShutdown

        } yield {
          assert(normalCommitments2 equals cachedCommitments2)
          assert(normalCommitments4 equals cachedCommitments4)
        }
      }

      "handles stakeholder group removal correctly" in {
        val (_, acsChanges) = setupContractsAndAcsChanges2()
        val crypto = cryptoSetup(remoteId2, topology)

        val inMemoryCommitmentStore =
          new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          // init phase
          // participant "remoteId2" has one stakeholder group in common with "remote1": (alice, bob, charlie) with one contract
          // participant "remoteId2" has three stakeholder group in common with "local": (alice, bob, charlie) with one contract,
          // (alice, danna) with one contract, and (alice, ed) with one contract
          rc <- runningCommitments
          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          byParticipant2 <- AcsCommitmentProcessor
            .stakeholderCommitmentsPerParticipant(
              remoteId2,
              rc.snapshot().active,
              crypto,
              ts(2),
              parallelism,
            )
            .failOnShutdown
          normalCommitments2 = computeCommitmentsPerParticipant(byParticipant2, cachedCommitments)
          _ = cachedCommitments.setCachedCommitments(
            normalCommitments2,
            rc.snapshot().active,
            byParticipant2.map { case (pid, set) =>
              (pid, set.map { case (stkhd, _cmt) => stkhd }.toSet)
            },
          )

          byParticipant <- AcsCommitmentProcessor
            .stakeholderCommitmentsPerParticipant(
              remoteId2,
              rc.snapshot().active,
              crypto,
              ts(2),
              parallelism,
            )
            .failOnShutdown

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

        val inMemoryCommitmentStore =
          new InMemoryAcsCommitmentStore(loggerFactory)
        val runningCommitments = initRunningCommitments(inMemoryCommitmentStore)
        val cachedCommitments = new CachedCommitments()

        for {
          // init phase
          // participant "remoteId2" has one stakeholder group in common with "remote1": (alice, bob, charlie) with one contract
          // participant "remoteId2" has three stakeholder group in common with "local": (alice, bob, charlie) with one contract,
          // (alice, danna) with one contract, and (alice, ed) with one contract
          rc <- runningCommitments
          _ = rc.update(rt(2, 0), acsChanges(ts(2)))
          byParticipant2 <- AcsCommitmentProcessor
            .stakeholderCommitmentsPerParticipant(
              remoteId2,
              rc.snapshot().active,
              crypto,
              ts(2),
              parallelism,
            )
            .failOnShutdown
          normalCommitments2 = computeCommitmentsPerParticipant(byParticipant2, cachedCommitments)
          _ = cachedCommitments.setCachedCommitments(
            normalCommitments2,
            rc.snapshot().active,
            byParticipant2.map { case (pid, set) =>
              (pid, set.map { case (stkhd, _cmt) => stkhd }.toSet)
            },
          )

          byParticipant <- AcsCommitmentProcessor
            .stakeholderCommitmentsPerParticipant(
              remoteId2,
              rc.snapshot().active,
              crypto,
              ts(2),
              parallelism,
            )
            .failOnShutdown

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

sealed trait Lifespan {
  def activatedTs: CantonTimestamp
  def deactivatedTs: CantonTimestamp
  def reassignmentCounterAtActivation: ReassignmentCounter
}

object Lifespan {
  final case class ArchiveOnDeactivate(
      activatedTs: CantonTimestamp,
      deactivatedTs: CantonTimestamp,
      reassignmentCounterAtActivation: ReassignmentCounter,
  ) extends Lifespan

  final case class UnassignmentDeactivate(
      activatedTs: CantonTimestamp,
      deactivatedTs: CantonTimestamp,
      reassignmentCounterAtActivation: ReassignmentCounter,
      reassignmentCounterAtUnassignment: ReassignmentCounter,
  ) extends Lifespan
}

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
        (Set(alice, bob), toc(1), toc(9), initialReassignmentCounter, initialReassignmentCounter),
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
        processor.map(_.close())
      },
      forAll(_) {
        _.warningMessage should (include(
          s"Disconnect and reconnect to the domain ${domainId.toString} if this error persists."
        ) or include regex "Timeout .* expired, but tasks still running. Shutting down forcibly")
      },
    )
  }
}
