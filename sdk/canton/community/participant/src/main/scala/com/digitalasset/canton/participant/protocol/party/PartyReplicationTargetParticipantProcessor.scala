// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.{ActiveContract, RepairContract}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.AcsReplicationProgressRuntime
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.event.{AcsChangeSupport, RecordOrderPublisher}
import com.digitalasset.canton.participant.protocol.conflictdetection.{CommitSet, RequestTracker}
import com.digitalasset.canton.participant.protocol.party.PartyReplicationTargetParticipantProcessor.{
  PersistContracts,
  contractsToRequestEachTime,
}
import com.digitalasset.canton.participant.store.{
  AcsReplicationProgress,
  ParticipantNodePersistentState,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId, ReassignmentId, UpdateId}
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, ReassignmentTag}
import com.digitalasset.canton.{RepairCounter, checked}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** The target participant processor ingests a party's active contracts on a specific synchronizer
  * and timestamp from a source participant as part of Online Party Replication.
  *
  * The interaction happens via the
  * [[com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor]] API and
  * the target participant processor enforces the protocol guarantees made by a
  * [[PartyReplicationSourceParticipantProcessor]]. The following guarantees made by the target
  * participant processor are verifiable at the party replication protocol: The target participant
  *   - sends a [[PartyReplicationTargetParticipantMessage.Initialize]] upon (re-)connecting,
  *   - requests contracts in a strictly increasing contract ordinal order,
  *   - and sends only deserializable payloads.
  *
  * @param partyId
  *   The party id of the party to replicate active contracts for.
  * @param requestId
  *   The "add party" request id that this replication is associated with.
  * @param psid
  *   The physical id of the synchronizer to replicate active contracts in.
  * @param partyOnboardingAt
  *   The timestamp immediately on which the ACS snapshot is based.
  * @param replicationProgressState
  *   Interface for processor to read and update ACS replication progress.
  * @param onError
  *   Callback notification that the target participant has encountered an error.
  * @param onDisconnect
  *   Callback notification that the target participant has disconnected.
  * @param testOnlyInterceptor
  *   Test interceptor only alters behavior in integration tests.
  */
class PartyReplicationTargetParticipantProcessor(
    partyId: PartyId,
    requestId: AddPartyRequestId,
    protected val psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    protected val replicationProgressState: AcsReplicationProgress,
    protected val onError: String => Unit,
    protected val onDisconnect: (String, TraceContext) => Unit,
    persistContracts: PersistContracts,
    recordOrderPublisher: RecordOrderPublisher,
    requestTracker: RequestTracker,
    pureCrypto: CryptoPureApi,
    protected val futureSupervisor: FutureSupervisor,
    protected val exitOnFatalFailures: Boolean,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    protected val testOnlyInterceptor: PartyReplicationTestInterceptor,
)(implicit override val executionContext: ExecutionContext)
    extends PartyReplicationProcessor {

  protected val processorStore: TargetParticipantStore = InMemoryProcessorStore.targetParticipant()

  // The base hash for all indexer UpdateIds to avoid repeating this for all ACS batches.
  private lazy val indexerUpdateIdBaseHash = pureCrypto
    .build(HashPurpose.OnlinePartyReplicationId)
    .addString(partyId.toProtoPrimitive)
    .addString(psid.toProtoPrimitive)
    .addLong(partyOnboardingAt.value.toProtoPrimitive)
    .finish()

  override def replicatedContractsCount: NonNegativeInt = processorStore.processedContractsCount

  override protected def name: String = "party-replication-target-processor"

  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle connect to SP") {
    super.onConnected().map { _ =>
      // Upon connecting or reconnecting, clear the initial contract ordinal.
      processorStore.resetConnection()
      progressPartyReplication()
    }
  }

  /** Consume status updates and ACS batches from the source participant.
    *
    * Note: Assigning the internal contract ids to the contracts requires that all the contracts are
    * already persisted in the contract store.
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle payload from SP") {
    notifyCounterParticipantAndPartyReplicatorOnError(for {
      messageFromSP <- EitherT.fromEither[FutureUnlessShutdown](
        PartyReplicationSourceParticipantMessage
          .fromByteString(protocolVersion, payload)
          .leftMap(deserializationError =>
            s"Failed to parse payload message from SP: ${deserializationError.message}"
          )
      )
      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        processorStore.initialContractOrdinalInclusiveO.isDefined,
        s"Received unexpected message from SP before initialized by TP: ${messageFromSP.dataOrStatus}",
      )
      replicationProgress <- EitherT.fromEither[FutureUnlessShutdown](
        replicationProgressState
          .getAcsReplicationProgress(requestId)
          .toRight(s"Party replication $requestId not found in progress state")
      )
      replicatedContractCount = replicationProgress.processedContractCount
      _ <- messageFromSP.dataOrStatus match {
        case PartyReplicationSourceParticipantMessage.AcsBatch(contracts) =>
          val firstContractOrdinal = replicatedContractCount
          logger.debug(
            s"Received batch beginning at contract ordinal $firstContractOrdinal with contracts ${contracts.forgetNE
                .flatMap(_.contract.createdEvent.map(_.contractId))
                .mkString(", ")}"
          )
          for {
            _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
              !replicationProgress.fullyProcessedAcs,
              s"Received ACS batch from SP after EndOfACS at $firstContractOrdinal",
            )
            _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
              replicatedContractCount.unwrap + contracts.size <= processorStore.requestedContractsCount.unwrap,
              s"Received too many contracts from SP: processed ${replicatedContractCount.unwrap} + received ${contracts.size} > requested ${processorStore.requestedContractsCount.unwrap}",
            )
            validatedActivations <- validateContracts(contracts)
            internalContractIdsForActiveContracts <- persistContracts(
              validatedActivations.map(_.contract)
            )(executionContext)(
              traceContext
            ).leftMap(err => s"Failed to persist contracts: $err")
            repairCounter = replicationProgress.nextPersistenceCounter
            toc = TimeOfChange(partyOnboardingAt.value, Some(repairCounter))
            replicatedContracts = validatedActivations.map {
              // TODO(#26468): Use validation packages
              case ContractReassignment(contract, _, _, reassignmentCounter) =>
                (
                  contract.contractId,
                  ReassignmentTag.Source(psid.logical),
                  reassignmentCounter,
                  toc,
                )
            }
            _ <- requestTracker
              .addReplicatedContracts(requestId, partyOnboardingAt.value, replicatedContracts)
              .leftMap(e =>
                s"Failed to add contracts $replicatedContracts to ActiveContractStore: $e"
              )
            validatedActivationsWithInternalContractIds = checked(
              tryAddInternalContractIds(
                validatedActivations,
                internalContractIdsForActiveContracts,
              )
            )
            _ <- EitherT.rightT[FutureUnlessShutdown, String](
              recordOrderPublisher.schedulePublishAddContracts(
                repairEventFromSerializedContract(
                  repairCounter = repairCounter,
                  activeContracts = validatedActivationsWithInternalContractIds,
                )
              )
            )
            updatedProcessedContractsCount =
              replicatedContractCount + NonNegativeInt.size(contracts)
            _ <- replicationProgressState.updateAcsReplicationProgress(
              requestId,
              AcsReplicationProgressRuntime(
                updatedProcessedContractsCount,
                repairCounter + 1,
                fullyProcessedAcs = false,
                this,
              ),
            )
            _ = processorStore.setProcessedContractsCount(updatedProcessedContractsCount)
          } yield ()
        case PartyReplicationSourceParticipantMessage.EndOfACS =>
          logger.info(
            s"Target participant has received end of data after ${replicatedContractCount.unwrap} contracts"
          )
          replicationProgressState
            .updateAcsReplicationProgress(
              requestId,
              AcsReplicationProgressRuntime(
                replicationProgress.processedContractCount,
                replicationProgress.nextPersistenceCounter,
                fullyProcessedAcs = true,
                this,
              ),
            )
            .map(_ => processorStore.setHasEndOfACSBeenReached())
      }
    } yield ()).map(_ => progressPartyReplication())
  }

  // This function requires that all contracts are already present in the contract store and
  // therefore their internal contract ids can be looked up.
  private def tryAddInternalContractIds(
      contractReassignments: NonEmpty[Seq[ContractReassignment]],
      internalContractIds: Map[LfContractId, Long],
  )(implicit
      traceContext: TraceContext
  ): NonEmpty[Seq[(ContractReassignment, Long)]] =
    contractReassignments.map { contractReassignment =>
      val contractId = contractReassignment.contract.contractId
      val internalContractId =
        internalContractIds.getOrElse(
          contractId,
          ErrorUtil
            .invalidState(
              s"Not found internal contract id for contract $contractId"
            ),
        )
      (contractReassignment, internalContractId)
    }

  private def validateContracts(
      contracts: NonEmpty[Seq[ActiveContract]]
  ): EitherT[FutureUnlessShutdown, String, NonEmpty[Seq[ContractReassignment]]] =
    EitherT.fromEither[FutureUnlessShutdown](
      contracts.toNEF
        .traverse(activeContract =>
          for {
            repairContract <- RepairContract.toRepairContract(activeContract.contract)
            _ <- Either.cond(
              repairContract.synchronizerId == psid.logical,
              (),
              s"Received contract ${repairContract.contractId} has unexpected synchronizer ${repairContract.synchronizerId}",
            )
            contractInstance <- ContractInstance.create(repairContract.contract)

          } yield {
            // TODO(#26468): Use representative package
            ContractReassignment(
              contractInstance,
              Source(contractInstance.templateId.packageId),
              Target(contractInstance.templateId.packageId),
              repairContract.reassignmentCounter,
            )
          }
        )
    )

  override def progressPartyReplication()(implicit traceContext: TraceContext): Unit =
    // Skip progress check if more than one other task is already queued that performs this same progress check or
    // is going to schedule a progress check.
    if (executionQueue.isAtMostOneTaskScheduled) {
      executeAsync(s"Respond to source participant if needed")(
        EitherTUtil.ifThenET(
          isChannelOpenForCommunication &&
            testOnlyInterceptor.onTargetParticipantProgress(
              processorStore
            ) == PartyReplicationTestInterceptor.Proceed
        )(respondToSourceParticipant())
      )
    }

  private def respondToSourceParticipant()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
    replicationProgress <- EitherT.fromEither[FutureUnlessShutdown](
      replicationProgressState
        .getAcsReplicationProgress(requestId)
        .toRight(s"Party replication $requestId not found in progress state")
    )
    replicatedContractCount = replicationProgress.processedContractCount
    _ <-
      if (replicationProgress.fullyProcessedAcs) {
        EitherT(
          FutureUnlessShutdown
            .lift(recordOrderPublisher.publishBufferedEvents())
            .flatMap(_ =>
              sendCompleted(
                "completing in response to source participant notification of end of data"
              ).value
            )
        )
      } else if (processorStore.initialContractOrdinalInclusiveO.isEmpty) {
        val initialContractOrdinalInclusive = replicatedContractCount
        logger.info(
          s"Connected. Requesting contracts from ${initialContractOrdinalInclusive.unwrap}"
        )
        val initializeSP = PartyReplicationTargetParticipantMessage(
          PartyReplicationTargetParticipantMessage.Initialize(initialContractOrdinalInclusive)
        )(
          PartyReplicationTargetParticipantMessage.protocolVersionRepresentativeFor(
            protocolVersion
          )
        )
        sendPayload("initialize source participant", initializeSP.toByteString).map { _ =>
          // Once the SP initialize message has been sent, set the initial contract ordinal
          // and reset the requested contracts count to the processed contracts count.
          processorStore.setInitialContractOrdinalInclusive(initialContractOrdinalInclusive)
          processorStore.setRequestedContractsCount(replicatedContractCount)
          progressPartyReplication()
        }
      } else if (replicatedContractCount == processorStore.requestedContractsCount) {
        logger.debug(
          s"Target participant has received all the contracts requested before ordinal ${replicatedContractCount.unwrap}. " +
            s"Requesting ${contractsToRequestEachTime.unwrap} more contracts from source participant"
        )
        requestNextSetOfContracts()
      } else {
        EitherT.rightT[FutureUnlessShutdown, String](())
      }
  } yield ()

  private def requestNextSetOfContracts()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val updatedContractOrdinalToRequestExclusive =
      processorStore.requestedContractsCount.map(_ + contractsToRequestEachTime.unwrap)
    val inclusiveContractOrdinal = updatedContractOrdinalToRequestExclusive.unwrap - 1
    val instructionMessage = PartyReplicationTargetParticipantMessage(
      PartyReplicationTargetParticipantMessage.SendAcsUpTo(
        NonNegativeInt.tryCreate(inclusiveContractOrdinal)
      )
    )(
      PartyReplicationTargetParticipantMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    sendPayload(
      s"request next set of contracts up to ordinal $inclusiveContractOrdinal",
      instructionMessage.toByteString,
    ).map(_ => processorStore.setRequestedContractsCount(updatedContractOrdinalToRequestExclusive))
  }

  override protected def hasEndOfACSBeenReached: Boolean = processorStore.hasEndOfACSBeenReached

  private def repairEventFromSerializedContract(
      repairCounter: RepairCounter,
      activeContracts: NonEmpty[Seq[(ContractReassignment, Long)]],
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.OnPRReassignmentAccepted = {
    val uniqueUpdateId = {
      // Add the repairCounter and contract-id to the hash to arrive at unique per-OPR updateIds.
      val hash = activeContracts
        .foldLeft {
          pureCrypto
            .build(HashPurpose.OnlinePartyReplicationId)
            .addByteString(indexerUpdateIdBaseHash.unwrap)
            .addLong(repairCounter.unwrap)
        } {
          // TODO(#26468): Use validation packages
          case (builder, (ContractReassignment(contract, _, _, reassignmentCounter), _)) =>
            builder
              .addLong(reassignmentCounter.v)
              .addString(contract.contractId.coid)
        }
        .finish()
      UpdateId(hash)
    }

    val contractIdCounters = activeContracts.map {
      // TODO(#26468): Use validation packages
      case (ContractReassignment(contract, _, _, reassignmentCounter), _) =>
        (contract.contractId, reassignmentCounter)
    }

    val artificialReassignmentInfo = ReassignmentInfo(
      sourceSynchronizer = ReassignmentTag.Source(psid.logical),
      targetSynchronizer = ReassignmentTag.Target(psid.logical),
      submitter = None,
      reassignmentId = ReassignmentId(
        ReassignmentTag.Source(psid.logical),
        ReassignmentTag.Target(psid.logical),
        timestamp, // artificial unassign has same timestamp as
        contractIdCounters,
      ),
      isReassigningParticipant = false,
    )
    val commitSet = CommitSet.createForAssignment(
      artificialReassignmentInfo.reassignmentId,
      activeContracts.map(_._1),
      artificialReassignmentInfo.sourceSynchronizer,
    )
    val acsChangeFactory = AcsChangeSupport.fromCommitSet(commitSet)
    Update.OnPRReassignmentAccepted(
      workflowId = None,
      updateId = uniqueUpdateId,
      reassignmentInfo = artificialReassignmentInfo,
      reassignment = Reassignment.Batch(
        activeContracts.zipWithIndex.map {
          // TODO(#26468): Use validation packages
          case (
                (ContractReassignment(contract, _, _, reassignmentCounter), internalContractId),
                idx,
              ) =>
            Reassignment.Assign(
              ledgerEffectiveTime = contract.inst.createdAt.time,
              createNode = contract.toLf,
              contractAuthenticationData = contract.inst.authenticationData,
              reassignmentCounter = reassignmentCounter.v,
              nodeId = idx,
              internalContractId = internalContractId,
            )
        }
      ),
      repairCounter = repairCounter,
      recordTime = timestamp,
      synchronizerId = psid.logical,
      acsChangeFactory = acsChangeFactory,
    )
  }
}

object PartyReplicationTargetParticipantProcessor {
  def apply(
      partyId: PartyId,
      requestId: AddPartyRequestId,
      partyOnboardingAt: EffectiveTime,
      replicationProgressState: AcsReplicationProgress,
      onError: String => Unit,
      onDisconnect: (String, TraceContext) => Unit,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      connectedSynchronizer: ConnectedSynchronizer,
      futureSupervisor: FutureSupervisor,
      exitOnFatalFailures: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      testInterceptor: PartyReplicationTestInterceptor =
        PartyReplicationTestInterceptor.AlwaysProceed,
  )(implicit executionContext: ExecutionContext): PartyReplicationTargetParticipantProcessor =
    new PartyReplicationTargetParticipantProcessor(
      partyId,
      requestId,
      connectedSynchronizer.psid,
      partyOnboardingAt,
      replicationProgressState,
      onError,
      onDisconnect,
      persistContracts(participantNodePersistentState),
      connectedSynchronizer.ephemeral.recordOrderPublisher,
      connectedSynchronizer.ephemeral.requestTracker,
      connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi,
      futureSupervisor,
      exitOnFatalFailures,
      timeouts,
      loggerFactory
        .append("psid", connectedSynchronizer.psid.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive)
        .append("requestId", requestId.toHexString),
      testInterceptor,
    )

  // TODO(#22251): Make this configurable.
  private[party] val contractsToRequestEachTime = PositiveInt.tryCreate(10)

  private[party] type PersistContracts =
    Seq[ContractInstance] => ExecutionContext => TraceContext => EitherT[
      FutureUnlessShutdown,
      String,
      Map[LfContractId, Long],
    ]

  private def persistContracts(
      participantNodePersistentState: Eval[ParticipantNodePersistentState]
  ): PersistContracts = contracts =>
    implicit ec =>
      implicit tc =>
        EitherT.right[String](
          participantNodePersistentState.value.contractStore.storeContracts(contracts)
        )

}
