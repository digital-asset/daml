// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.data.EitherT
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.BaseTest.*
import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeProportion, PositiveInt}
import com.digitalasset.canton.config.{
  BatchingConfig,
  CommitmentSendDelay,
  DefaultProcessingTimeouts,
  NonNegativeDuration,
  PositiveDurationSeconds,
  TestingConfigInternal,
}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  LtHash16,
  SyncCryptoClient,
  SynchronizerSnapshotSyncCryptoApi,
}
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChange,
  ContractStakeholdersAndReassignmentCounter,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.metrics.{
  ConnectedSynchronizerMetrics,
  ParticipantHistograms,
  ParticipantMetrics,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.commitmentsFromStkhdCmts
import com.digitalasset.canton.participant.store.memory.{
  InMemoryAcsCommitmentConfigStore,
  InMemoryAcsCommitmentStore,
  InMemoryActiveContractStore,
  InMemoryContractStore,
}
import com.digitalasset.canton.participant.store.{
  AcsCommitmentStore,
  AcsCounterParticipantConfigStore,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.platform.store.interning.MockStringInterning
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  DefaultOpenEnvelope,
  SignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.client.{SendCallback, SequencerClientSend}
import com.digitalasset.canton.sequencing.protocol.{
  AggregationRule,
  Batch,
  MessageId,
  OpenEnvelope,
  Recipients,
}
import com.digitalasset.canton.store.memory.InMemoryIndexedStringStore
import com.digitalasset.canton.time.{PositiveSeconds, SimClock}
import com.digitalasset.canton.topology.client.{SynchronizerTopologyClient, TopologySnapshot}
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{
  ParticipantId,
  SynchronizerId,
  TestingTopology,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{
  HasExecutorServiceGeneric,
  LfPartyId,
  ReassignmentCounter,
  RepairCounter,
  SynchronizerAlias,
  TestEssentials,
}
import com.digitalasset.daml.lf.data.Bytes
import com.google.protobuf.ByteString
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, DurationInt}

@SuppressWarnings(
  Array("org.wartremover.warts.Var", "org.wartremover.warts.Null")
)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
@Measurement(iterations = 10)
@Fork(value = 2)
class AcsCommitmentBenchmark
    extends HasExecutorServiceGeneric
    with Matchers
    with Inspectors
    with TestEssentials
    with FlagCloseable
    with HasCloseContext {

  implicit val ec: scala.concurrent.ExecutionContext = Threading.newExecutionContext(
    loggerFactory.threadName + "-env-ec",
    noTracingLogger,
  )

  // these are the parameters that can be changed during a run to benchmark different scenarios.
  protected var counterParticipants: Int = 2
  protected var partiesPerCounterParticipant: Int = 1

  /** see [[com.digitalasset.canton.participant.pruning.StakeholderGroupType]] for options and
    * definitions. *
    */
  protected var stakeholderGroups: String = "simple"
  // these contracts oscillate each interval between activation and deactivation. Mimicking a contract with lots of interactions.
  protected var activeContracts: Int = 1
  // these contracts are activated in the beginning and remain active for the duration of the benchmark Iteration.
  // passive contracts are expected to have an almost zero impact on performance (they only add latency to the first iteration, which is warmup); this is used to capture code changes that breaks that.
  protected var passiveContracts: Int = 1
  protected var configCatchUpIntervalSkip: Int = 1
  protected var configNrIntervalsToTriggerCatchUp: Int = Int.MaxValue
  protected var enableCommitmentCaching: Boolean = false
  protected var activeContractsOscillationFrequence: Int = 2
  private var changes: Seq[(CantonTimestamp, RepairCounter, AcsChange)] = _
  private var contractDeployment: Seq[(CantonTimestamp, RepairCounter, AcsChange)] = _
  private var contractArchive: Seq[(CantonTimestamp, RepairCounter, AcsChange)] = _
  private var remoteCommitments: Seq[(ContractDefinition, CantonTimestamp, CantonTimestamp)] = _
  private var counterPartiesTopology: IndexedSeq[(ParticipantId, Set[LfPartyId])] = _
  private var processorF: FutureUnlessShutdown[AcsCommitmentProcessor] = _
  private var store: AcsCommitmentStore = _
  private var configStore: AcsCounterParticipantConfigStore = _
  private var lastRunEndTime: CantonTimestamp = ts(0).forgetRefinement

  private val runtime: Long = 5000L
  private val reconciliationInterval: Long = 5L
  private val reconciliationIntervalAsMicros: Long =
    Duration(reconciliationInterval, TimeUnit.SECONDS).toMicros
  private val breakTimeInterval: Long =
    reconciliationInterval * activeContractsOscillationFrequence * 10
  private val contractCreationTimestamp = CantonTimestamp.Epoch
  private val contractArchiveTimestamp =
    CantonTimestamp.Epoch.plusSeconds(runtime)
  private val synchronizerId = SynchronizerId(
    UniqueIdentifier.tryFromProtoPrimitive("synchronizer::da")
  ).toPhysical
  private val localId = ParticipantId(
    UniqueIdentifier.tryFromProtoPrimitive("localParticipant::synchronizer")
  )
  private val localParty = LfPartyId.assertFromString("User::0")
  private val symbolicCrypto =
    SymbolicCrypto.create(testedReleaseProtocolVersion, timeouts, loggerFactory)

  @Setup(Level.Trial)
  // builds the counterParticipant Topology map and AcsChanges needed for the run.
  def setupBenchmarkRun(): Unit = {
    val stakeholderGroupType = StakeholderGroupType.parse(stakeholderGroups)
    counterPartiesTopology = (0 until counterParticipants).map(i =>
      ParticipantId(
        UniqueIdentifier.tryFromProtoPrimitive(s"remoteParticipant${i + 1}::domain")
      ) -> (0 until partiesPerCounterParticipant)
        .map(j => LfPartyId.assertFromString(s"User::${(i * 1000) + j + 1}"))
        .toSet
    )

    val proofs = (1 to (runtime / reconciliationInterval).toInt)
      .map(i => ts(i * reconciliationInterval.toInt).forgetRefinement)

    val contractSetup = buildContractSetup(counterPartiesTopology, stakeholderGroupType)
    val changeTimes =
      (proofs.map(ts => TimeOfChange(ts)) ++
        List(
          TimeOfChange(contractCreationTimestamp),
          TimeOfChange(contractArchiveTimestamp),
        )).distinct.sorted

    changes = changeTimes.map(changesAtToc(contractSetup))
    contractDeployment = changes.filter { case (time, _, _) => time == contractCreationTimestamp }
    contractArchive = changes.filter { case (time, _, _) => time == contractArchiveTimestamp }
    changes = changes.filter { case (time, _, _) =>
      time != contractCreationTimestamp && time != contractArchiveTimestamp
    }

    remoteCommitments = proofs
      .flatMap(t => contractSetup.map(cs => (cs, t.plusSeconds(-reconciliationInterval), t)))
      .dropRight(1)

  }

  @Setup(Level.Iteration)
  def iteration(): Unit =
    testSetup(Map(localId -> Set(localParty)) ++ counterPartiesTopology)

  @Setup(Level.Invocation)
  def prepareRun(): Unit = {

    val prepare = for {
      processor <- processorF
      // we deploy contracts
      _ = contractDeployment.foreach { case (ts, tb, change) =>
        processor.publish(RecordTime(ts, tb.v), change)
      }
      remote <- remoteCommitments.parTraverse(commitmentMsg)
      delivered = remote.flatten.map(cmt =>
        (
          cmt.message.period.toInclusive,
          List(OpenEnvelope(cmt, Recipients.cc(localId))(testedProtocolVersion)),
        )
      )

      - <- processor.flush()
      // First ask for the remote commitments to be processed, and then compute locally
      // This triggers catch-up mode
      _ <- delivered
        .parTraverse_ { case (ts, batch) =>
          processor.processBatchInternal(ts.forgetRefinement, batch)
        }
    } yield {}
    Await.result(prepare.failOnShutdownToAbortException("prepareRun"), Duration.Inf)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def localParticipantCatchUp(blackHole: Blackhole): Unit = {
    val publish = for {
      processor <- processorF
      _ =
        changes.foreach { case (ts, tb, change) =>
          processor.publish(RecordTime(ts, tb.v), change)
        }
      wait <- processor.flush()
    } yield wait

    val p: Unit =
      Await.result(publish.failOnShutdownToAbortException("localParticipantCatchup"), Duration.Inf)
    blackHole.consume(p)
  }

  @TearDown(Level.Invocation)
  def updateTimes(): Unit = {
    val updateTimes = for {
      processor <- processorF
      // we archive contracts
      _ = contractArchive.foreach { case (ts, tb, change) =>
        processor.publish(RecordTime(ts, tb.v), change)
      }
    } yield {
      val incrementSeconds = runtime + breakTimeInterval
      lastRunEndTime = lastRunEndTime.plusSeconds(incrementSeconds)
      updateRunWithNewMinimumTimestamp(incrementSeconds)
      ()
    }
    Await.result(updateTimes.failOnShutdownToAbortException("updateTimes"), Duration.Inf)
  }

  @TearDown(Level.Iteration)
  def iterationTeardown(): Unit = {
    val tearDown = for {
      processor <- processorF

    } yield {
      processor.close()
      store.close()
    }
    Await.result(tearDown.failOnShutdownToAbortException("iterationTeardown"), Duration.Inf)
  }

  @TearDown(Level.Trial)
  def tearDown(): Unit =
    closeExecutor()

  private def buildContractSetup(
      counterPartiesTopology: IndexedSeq[(ParticipantId, Set[LfPartyId])],
      stakeholder: StakeholderGroupType,
  ): Seq[ContractDefinition] =
    stakeholder match {
      case StakeholderGroupType.Simple =>
        counterPartiesTopology.zipWithIndex.map { case ((participant, parties), index) =>
          buildCounterParticipantsAndContractSetup(
            Set(participant),
            index,
            Map(parties -> true),
          )
        }
      case StakeholderGroupType.X3 =>
        counterPartiesTopology
          .grouped(3)
          .zipWithIndex
          .map { case (participantsSequence, index) =>
            buildCounterParticipantsAndContractSetup(
              participantsSequence.map { case (participant, _) => participant }.toSet,
              index,
              Map(participantsSequence.flatMap { case (_, parties) => parties }.toSet -> true),
            )
          }
          .toSeq
      case StakeholderGroupType.X3withManyStakeholders =>
        counterPartiesTopology
          .grouped(3)
          .zipWithIndex
          .map { case (participantsSequence, index) =>
            buildCounterParticipantsAndContractSetup(
              participantsSequence.map { case (participant, _) => participant }.toSet,
              index,
              // only the first stakeholder set for every group of participants will have active contracts.
              participantsSequence.flatMap { case (_, lfPartyIdSet) =>
                lfPartyIdSet.zipWithIndex.map { case (party, index) =>
                  (Set(party) -> (index == 1))
                }
              }.toMap,
            )
          }
          .toSeq
      case StakeholderGroupType.Massive =>
        Seq(
          buildCounterParticipantsAndContractSetup(
            Set(counterPartiesTopology.apply(0)._1),
            0,
            Map(counterPartiesTopology.flatMap(p => p._2).toSet -> true),
          )
        )
      case _ => throw new Exception("Unsupported stakeholderGroupType")
    }

  private def buildCounterParticipantsAndContractSetup(
      participants: Set[ParticipantId],
      startContractId: Int,
      stakeholderGroup: Map[Set[LfPartyId], Boolean],
  ): ContractDefinition = {
    var count = 0
    ContractDefinition(
      participants,
      stakeholderGroup.map { case (parties, hasActiveContracts) =>
        val activeC =
          if (!hasActiveContracts) IndexedSeq.empty
          else
            (0 until activeContracts).map { _ =>
              count = count + 1
              coid(startContractId, count)
            }
        val passiveC = (0 until passiveContracts).map { _ =>
          count = count + 1
          coid(startContractId, count)
        }

        StakeholderGroup(parties incl localParty, activeC, passiveC)
      }.toSet,
      ReassignmentCounter.Genesis,
    )
  }

  private def updateRunWithNewMinimumTimestamp(seconds: Long): Unit = {
    remoteCommitments = remoteCommitments.map {
      case (contractDefinition, fromExclusive, toInclusive) =>
        (contractDefinition, fromExclusive.plusSeconds(seconds), toInclusive.plusSeconds(seconds))
    }
    changes = changes.map { case (timestamp, requestCounter, acsChanges) =>
      (timestamp.plusSeconds(seconds), requestCounter, acsChanges)
    }
    contractDeployment = contractDeployment.map { case (timestamp, requestCounter, acsChanges) =>
      (timestamp.plusSeconds(seconds), requestCounter, acsChanges)
    }
    contractArchive = contractArchive.map { case (timestamp, requestCounter, acsChanges) =>
      (timestamp.plusSeconds(seconds), requestCounter, acsChanges)
    }
  }
  def handleFailure(message: String): Nothing = throw new RuntimeException(message)

  private def cryptoSetup(
      owner: ParticipantId,
      topology: Map[ParticipantId, Set[LfPartyId]],
      dynamicSynchronizerParametersWithValidity: List[
        SynchronizerParameters.WithValidity[DynamicSynchronizerParameters]
      ],
  ): SyncCryptoClient[SynchronizerSnapshotSyncCryptoApi] = {

    val topologyWithPermissions =
      topology.fmap(_.map(p => (p, ParticipantPermission.Submission)).toMap)

    val testingTopology = dynamicSynchronizerParametersWithValidity match {
      // this way we get default values for an empty List
      case Nil => TestingTopology()
      case _ => TestingTopology(synchronizerParameters = dynamicSynchronizerParametersWithValidity)
    }

    testingTopology
      .withReversedTopology(topologyWithPermissions)
      .build(symbolicCrypto, loggerFactory)
      .forOwnerAndSynchronizer(owner)
  }

  private def testSetup(topology: Map[ParticipantId, Set[LfPartyId]]): Unit = {

    val acsCommitmentsCatchUpConfig =
      Some(
        AcsCommitmentsCatchUpParameters(
          PositiveInt.tryCreate(configCatchUpIntervalSkip),
          PositiveInt.tryCreate(configNrIntervalsToTriggerCatchUp),
        )
      )

    val synchronizerCrypto = cryptoSetup(
      localId,
      topology,
      List(
        SynchronizerParameters.WithValidity(
          validFrom = CantonTimestamp.MinValue,
          validUntil = None,
          parameter = TestSynchronizerParameters.defaultDynamic
            .tryUpdate(acsCommitmentsCatchUp = acsCommitmentsCatchUpConfig),
        )
      ),
    )

    val sequencerClient = mock[SequencerClientSend]

    when(
      sequencerClient.send(
        any[Batch[DefaultOpenEnvelope]],
        any[Option[CantonTimestamp]],
        any[CantonTimestamp],
        any[MessageId],
        any[Option[AggregationRule]],
        any[SendCallback],
        any[Boolean],
      )(any[TraceContext], any[MetricsContext])
    ).thenReturn(EitherT.pure(())): Unit

    configStore = new InMemoryAcsCommitmentConfigStore()

    store = new InMemoryAcsCommitmentStore(synchronizerId.logical, configStore, loggerFactory)

    val sortedReconciliationIntervalsProvider = constantSortedReconciliationIntervalsProvider(
      PositiveSeconds.tryOfSeconds(reconciliationInterval)
    )

    val indexedStringStore = new InMemoryIndexedStringStore(minIndex = 1, maxIndex = 1)

    processorF = AcsCommitmentProcessor(
      localId,
      sequencerClient,
      synchronizerCrypto,
      sortedReconciliationIntervalsProvider,
      store,
      _ => (),
      ParticipantTestMetrics.synchronizer.commitments,
      DefaultProcessingTimeouts.testing
        .copy(storageMaxRetryInterval = NonNegativeDuration.tryFromDuration(1.millisecond)),
      futureSupervisor,
      new InMemoryActiveContractStore(indexedStringStore, loggerFactory),
      new InMemoryAcsCommitmentConfigStore(),
      new InMemoryContractStore(timeouts, loggerFactory),
      // no additional consistency checks; if enabled, one needs to populate the above ACS and contract stores
      // correctly, otherwise the test will fail
      enableAdditionalConsistencyChecks = false,
      loggerFactory,
      if (enableCommitmentCaching)
        TestingConfigInternal()
      else
        TestingConfigInternal(doNotUseCommitmentCachingFor = topology.map { case (participant, _) =>
          participant.uid.identifier.str
        }.toSet),
      new SimClock(loggerFactory = loggerFactory),
      exitOnFatalFailures = true,
      BatchingConfig(),
      // do not delay sending commitments for testing, because tests often expect to see commitments after an interval
      Some(CommitmentSendDelay(Some(NonNegativeProportion.zero), Some(NonNegativeProportion.zero))),
      doNotAwaitOnCheckingIncomingCommitments = false,
      commitmentCheckpointInterval = PositiveDurationSeconds.ofSeconds(reconciliationInterval),
      commitmentMismatchDebugging = false,
      commitmentProcessorNrAcsChangesBehindToTriggerCatchUp = None,
      stringInterning = new MockStringInterning,
    )
  }

  private object ParticipantTestMetrics
      extends ParticipantMetrics(
        new ParticipantHistograms(MetricName("test"))(new HistogramInventory),
        new NoOpMetricsFactory,
      ) {

    val synchronizer: ConnectedSynchronizerMetrics =
      this.connectedSynchronizerMetrics(SynchronizerAlias.tryCreate("test"))
  }

  @nowarn("cat=deprecation")
  private def constantSortedReconciliationIntervalsProvider(
      reconciliationInterval: PositiveSeconds,
      domainBootstrappingTime: CantonTimestamp = CantonTimestamp.MinValue,
  ): SortedReconciliationIntervalsProvider = {

    val topologyClient = mock[SynchronizerTopologyClient]
    val topologySnapshot = mock[TopologySnapshot]

    when(topologyClient.approximateTimestamp).thenReturn(CantonTimestamp.MaxValue): Unit
    when(topologyClient.awaitSnapshot(any[CantonTimestamp])(any[TraceContext])).thenReturn(
      FutureUnlessShutdown.pure(topologySnapshot)
    ): Unit

    when(topologySnapshot.listDynamicSynchronizerParametersChanges()).thenReturn {
      FutureUnlessShutdown.pure(
        Seq(
          mkDynamicSynchronizerParameters(domainBootstrappingTime, reconciliationInterval)
        )
      )
    }: Unit

    new SortedReconciliationIntervalsProvider(
      topologyClient = topologyClient,
      futureSupervisor = FutureSupervisor.Noop,
      loggerFactory = loggerFactory,
    )
  }

  private def changesAtToc(
      contractSetup: Seq[ContractDefinition]
  )(toc: TimeOfChange): (CantonTimestamp, RepairCounter, AcsChange) = {
    def hashedDeactivation(lfContractId: LfContractId, contract: ContractDefinition) =
      lfContractId ->
        ContractStakeholdersAndReassignmentCounter(
          contract.parties(),
          contract.reassignmentCounter,
        )
    def hashedActivation(
        lfContractId: LfContractId,
        contract: ContractDefinition,
    ) = lfContractId ->
      ContractStakeholdersAndReassignmentCounter(
        contract.parties(),
        contract.reassignmentCounter,
      )
    (
      toc.timestamp,
      toc.counterO.getOrElse(RepairCounter.One),
      contractSetup.foldLeft(AcsChange.empty) { case (acsChange, contract) =>
        AcsChange(
          deactivations =
            acsChange.deactivations ++ (if (toc.timestamp == contractArchiveTimestamp)
                                          contract
                                            .passiveContractIds()
                                            .map(c => hashedDeactivation(c, contract)) ++
                                            contract
                                              .activeContractIds()
                                              .map(c => hashedDeactivation(c, contract))
                                        else if (
                                          toc.timestamp.toMicros % (activeContractsOscillationFrequence * reconciliationIntervalAsMicros) ==
                                            (activeContractsOscillationFrequence * reconciliationIntervalAsMicros) / 2
                                        )
                                          contract
                                            .activeContractIds()
                                            .map(c => hashedDeactivation(c, contract))
                                        else Map.empty),
          activations =
            acsChange.activations ++ (if (toc.timestamp == contractCreationTimestamp)
                                        contract
                                          .activeContractIds()
                                          .map(c => hashedActivation(c, contract))
                                          ++ contract
                                            .passiveContractIds()
                                            .map(c => hashedActivation(c, contract))
                                      else if (
                                        toc.timestamp.toMicros % (activeContractsOscillationFrequence * reconciliationIntervalAsMicros) == 0
                                      )
                                        contract
                                          .activeContractIds()
                                          .map(c => hashedActivation(c, contract))
                                      else Map.empty),
        )
      },
    )
  }

  private def mkDynamicSynchronizerParameters(
      validFrom: CantonTimestamp,
      reconciliationInterval: PositiveSeconds,
  ): DynamicSynchronizerParametersWithValidity =
    DynamicSynchronizerParametersWithValidity(
      DynamicSynchronizerParameters
        .initialValues(testedProtocolVersion)
        .tryUpdate(reconciliationInterval = reconciliationInterval),
      validFrom,
      None,
    )

  private def lfHash(index: Int): LfHash =
    LfHash.assertFromBytes(
      Bytes.assertFromString(f"$index%04x".padTo(LfHash.underlyingHashLength * 2, '0'))
    )

  private def commitmentMsg(
      params: (
          ContractDefinition,
          CantonTimestamp,
          CantonTimestamp,
      )
  ): FutureUnlessShutdown[Set[SignedProtocolMessage[AcsCommitment]]] = {
    val (contractDefinition, fromExclusive, toInclusive) = params
    FutureUnlessShutdown.sequence(
      contractDefinition.counterParticipants.map { participant =>
        val crypto =
          TestingTopology()
            .withSimpleParticipants(contractDefinition.counterParticipants.toSeq*)
            .build(symbolicCrypto, loggerFactory)
            .forOwnerAndSynchronizer(participant)
        val remoteCommitmentsByParticipant =
          remoteCommitments.filter { case (contractDefinition, _, _) =>
            contractDefinition.counterParticipants.contains(participant)
          }
        val stakeholderCommitments =
          if (
            toInclusive.toMicros % (activeContractsOscillationFrequence * reconciliationIntervalAsMicros) <
              (activeContractsOscillationFrequence * reconciliationIntervalAsMicros) / 2
          ) {
            remoteCommitmentsByParticipant.flatMap { case (contractDefinition, _, _) =>
              contractDefinition
                .activeContractIds()
                .toSeq
                .appendedAll(contractDefinition.passiveContractIds())
                .map(contractId => contractId -> contractDefinition.reassignmentCounter)
            }.toMap
          } else
            remoteCommitmentsByParticipant.flatMap { case (contractDefinition, _, _) =>
              contractDefinition
                .passiveContractIds()
                .map(contractId => contractId -> contractDefinition.reassignmentCounter)
            }.toMap
        val cmt = commitmentsFromStkhdCmts(Seq(stakeholderCommitment(stakeholderCommitments)))
        val snapshotF = crypto.snapshot(CantonTimestamp.Epoch)
        val period =
          CommitmentPeriod
            .create(
              fromExclusive,
              toInclusive,
              PositiveSeconds.tryOfSeconds(reconciliationInterval),
            )
            .getOrElse(throw new Exception("missing commitment period"))
        val payload =
          AcsCommitment
            .create(synchronizerId, participant, localId, period, cmt, testedProtocolVersion)
        snapshotF.flatMap { snapshot =>
          SignedProtocolMessage
            .trySignAndCreate(payload, snapshot, None)
        }
      }
    )
  }
  private def stakeholderCommitment(
      contracts: Map[LfContractId, ReassignmentCounter]
  ): AcsCommitment.CommitmentType = {
    val h = LtHash16()
    contracts.keySet.foreach { cid =>
      h.add(
        (ByteString.copyFromUtf8(cid.coid)
          concat ReassignmentCounter.encodeDeterministically(contracts(cid))).toByteArray
      )
    }
    h.getByteString()
  }
  private def ts(i: Int): CantonTimestampSecond = CantonTimestampSecond.ofEpochSecond(i.longValue)

  private def coid(discriminator: Int, txId: Int): LfContractId =
    LfContractId.V1(lfHash(discriminator), Bytes.assertFromString(f"$txId%04x"))
}

final case class ContractDefinition(
    counterParticipants: Set[ParticipantId],
    stakeholderGroups: Set[StakeholderGroup],
    reassignmentCounter: ReassignmentCounter = ReassignmentCounter.Genesis,
) {

  def passiveContractIds(): Set[LfContractId] = stakeholderGroups.flatMap(_.passiveContractIds)
  def activeContractIds(): Set[LfContractId] = stakeholderGroups.flatMap(_.activeContractIds)
  def parties(): Set[LfPartyId] = stakeholderGroups.flatMap(_.parties)

}

final case class StakeholderGroup(
    parties: Set[LfPartyId],
    activeContractIds: Seq[LfContractId],
    passiveContractIds: Seq[LfContractId],
)

sealed class StakeholderGroupType {}
object StakeholderGroupType {

  def parse(string: String): StakeholderGroupType = string match {
    case "simple" => Simple
    case "x3" => X3
    case "x3withManyStakeholders" => X3withManyStakeholders
    case "massive" => Massive
    case _ => throw new Exception("unknown StakeholderGroupType")
  }

  /** stakeholder groups containing only 1 counter participants example stakeholder sets with 5
    * participants:
    * (local,counterParticipant1),(local,counterParticipant2),(local,counterParticipant3),(local,counterParticipant4),(local,counterParticipant5)
    */
  case object Simple extends StakeholderGroupType

  /** counter parties are divided into stakeholder groups containing 3 members (no overlap): example
    * stakeholder sets with 5 participants:
    * (local,counterParticipant1,counterParticipant2,counterParticipant3),(local,counterParticipant4,counterParticipant5)
    */
  case object X3 extends StakeholderGroupType

  /** counter parties are divided into stakeholder groups containing 3 members (similar to X3).
    * example stakeholder sets with 5 participants:
    * (local,counterParticipant1,counterParticipant2,counterParticipant3),(local,counterParticipant4,counterParticipant5)
    *
    * difference from X3 lies in how parties are handled inside. In X3 every party is considered
    * "active" (produces and archives contract each period) in X3withManyStakeholder only the first
    * party is "active" and the rest is considered passive (only produce and archives at start and
    * end of run) this is test and ensure many passive stakeholders does not impact performance.
    */
  case object X3withManyStakeholders extends StakeholderGroupType

  /** counter parties are added into one massive stakeholder group: (py0, py1, py2, py3, py4, py5)
    * example stakeholder sets with 5 participants:
    * (local,counterParticipant1,counterParticipant2,counterParticipant3,counterParticipant4,counterParticipant5)
    */
  case object Massive extends StakeholderGroupType
}
