// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import cats.implicits.toTraverseOps
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{CryptoPureApi, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.admin.data.{ActiveContract, RepairContract}
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.event.{AcsChangeSupport, RecordOrderPublisher}
import com.digitalasset.canton.participant.protocol.conflictdetection.{CommitSet, RequestTracker}
import com.digitalasset.canton.participant.protocol.party.TargetParticipantAcsPersistence.PersistsContracts
import com.digitalasset.canton.participant.store.{
  AcsReplicationProgress,
  ParticipantNodePersistentState,
}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{ContractInstance, LfContractId, ReassignmentId, UpdateId}
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, ReassignmentTag}
import com.digitalasset.canton.{RepairCounter, checked}

import scala.concurrent.ExecutionContext

/** Target participant ACS persistence functionality shared between the OnPR sequencer channel
  * target processor and the file-based ACS importer.
  * @param partyId
  *   the party whose ACS is being imported or replicated
  * @param requestId
  *   the online party replication, party add request identifier
  * @param partyOnboardingAt
  *   the effective time of the onboarding PartyToParticipant topology transaction
  * @param replicationProgressState
  *   interface to update OnPR progress
  * @param persistsContracts
  *   interface to persist a batch of contracts to the contract store
  * @param recordOrderPublisher
  *   record order publisher for publishing indexer events
  * @param requestTracker
  *   request tracker to update the active contract store journal along with in-memory state
  */
abstract class TargetParticipantAcsPersistence(
    partyId: PartyId,
    requestId: AddPartyRequestId,
    psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    replicationProgressState: AcsReplicationProgress,
    persistsContracts: PersistsContracts,
    recordOrderPublisher: RecordOrderPublisher,
    requestTracker: RequestTracker,
    pureCrypto: CryptoPureApi,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  // The base hash for all indexer UpdateIds to avoid repeating this for all ACS batches.
  private lazy val indexerUpdateIdBaseHash = pureCrypto
    .build(HashPurpose.OnlinePartyReplicationId)
    .addString(partyId.toProtoPrimitive)
    .addString(psid.toProtoPrimitive)
    .addLong(partyOnboardingAt.value.toProtoPrimitive)
    .finish()

  /** Import contracts as part of online party replication performing the following activities.
    *   - validate the contracts and contract ids
    *   - persist contracts at a determined time of change updating in-memory request-tracker state
    *     accordingly
    *   - schedule publishing of the corresponding indexer event
    *   - update the persisted and ephemeral, in-memory OnPR progress state
    *
    * @param contracts
    *   the contracts to import
    * @return
    *   the updated total count of contracts imported thus far
    */
  def importContracts(
      contracts: NonEmpty[Seq[ActiveContract]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonNegativeInt] =
    for {
      replicationProgress <- EitherT.fromEither[FutureUnlessShutdown](
        replicationProgressState
          .getAcsReplicationProgress(requestId)
          .toRight(s"Party replication $requestId not found in progress state")
      )
      validatedActivations <- validateContracts(contracts)
      internalContractIdsForActiveContracts <- persistsContracts
        .persistContracts(validatedActivations.map(_.contract))
        .leftMap(err => s"Failed to persist contracts: $err")
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
        .leftMap(e => s"Failed to add contracts $replicatedContracts to ActiveContractStore: $e")
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
        replicationProgress.processedContractCount + NonNegativeInt.size(contracts)
      _ <- replicationProgressState.updateAcsReplicationProgress(
        requestId,
        newProgress(updatedProcessedContractsCount, repairCounter),
      )
    } yield updatedProcessedContractsCount

  /** The new progress depends on ephemeral state depending on the derived class.
    */
  protected def newProgress(
      updatedProcessedContractsCount: NonNegativeInt,
      usedRepairCounter: RepairCounter,
  ): PartyReplicationStatus.AcsReplicationProgress

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
            repairContract <- RepairContract.fromLapiActiveContract(activeContract.contract)
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
              ReassignmentTag.Source(contractInstance.templateId.packageId),
              ReassignmentTag.Target(contractInstance.templateId.packageId),
              repairContract.reassignmentCounter,
            )
          }
        )
    )

  /** Determines the indexer event corresponding to the imported active contracts
    * @param repairCounter
    *   the repair counter lexicographically relative to the timestamp
    * @param activeContracts
    *   the active contracts to publish
    * @param timestamp
    *   the record time to publish the event at
    * @return
    *   the event to publish
    */
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
        timestamp, // artificial unassign has same timestamp as the assign
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

object TargetParticipantAcsPersistence {

  // TODO(#22251): Make this configurable.
  private[party] val contractsToRequestEachTime = PositiveInt.tryCreate(10)

  // not sealed for testing
  trait PersistsContracts {

    /** Persist the contracts in the contract store.
      */
    def persistContracts(
        contracts: NonEmpty[Seq[ContractInstance]]
    )(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Map[LfContractId, Long]]
  }

  final class PersistsContractsImpl(
      participantNodePersistentState: Eval[ParticipantNodePersistentState]
  ) extends PersistsContracts {
    override def persistContracts(contracts: NonEmpty[Seq[ContractInstance]])(implicit
        executionContext: ExecutionContext,
        traceContext: TraceContext,
    ): EitherT[FutureUnlessShutdown, String, Map[LfContractId, Long]] = EitherT.right[String](
      participantNodePersistentState.value.contractStore.storeContracts(contracts)
    )
  }
}
