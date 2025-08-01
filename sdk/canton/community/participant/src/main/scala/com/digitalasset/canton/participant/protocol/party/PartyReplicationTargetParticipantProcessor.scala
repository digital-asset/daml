// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import cats.implicits.toTraverseOps
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.participant.admin.party.PartyReplicationTestInterceptor
import com.digitalasset.canton.participant.event.AcsChangeSupport
import com.digitalasset.canton.participant.protocol.conflictdetection.CommitSet
import com.digitalasset.canton.participant.store.ParticipantNodePersistentState
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{ContractInstance, ReassignmentId, TransactionId}
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ReassignmentTag}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

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
  * @param partyToParticipantEffectiveAt
  *   The timestamp immediately on which the ACS snapshot is based.
  * @param onComplete
  *   Callback notification that the target participant has received the entire ACS.
  * @param onError
  *   Callback notification that the target participant has encountered an error.
  * @param onDisconnect
  *   Callback notification that the target participant has disconnected.
  * @param testOnlyInterceptor
  *   Test interceptor only alters behavior in integration tests.
  */
final class PartyReplicationTargetParticipantProcessor(
    partyId: PartyId,
    partyToParticipantEffectiveAt: CantonTimestamp,
    protected val onComplete: TraceContext => Unit,
    protected val onError: String => Unit,
    protected val onDisconnect: (String, TraceContext) => Unit,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    connectedSynchronizer: ConnectedSynchronizer,
    protected val futureSupervisor: FutureSupervisor,
    protected val exitOnFatalFailures: Boolean,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    protected val testOnlyInterceptor: PartyReplicationTestInterceptor,
)(implicit override val executionContext: ExecutionContext)
    extends PartyReplicationProcessor {

  private val processorStore = InMemoryProcessorStore.targetParticipant()
  private val contractsToRequestEachTime = PositiveInt.tryCreate(10)

  override protected val psid: PhysicalSynchronizerId = connectedSynchronizer.psid

  private val pureCrypto =
    connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi

  // The base hash for all indexer UpdateIds to avoid repeating this for all ACS batches.
  private lazy val indexerUpdateIdBaseHash = pureCrypto
    .build(HashPurpose.OnlinePartyReplicationId)
    .add(partyId.toProtoPrimitive)
    .add(psid.toProtoPrimitive)
    .add(partyToParticipantEffectiveAt.toProtoPrimitive)
    .finish()

  override def replicatedContractsCount: NonNegativeInt = processorStore.processedContractsCount

  override protected def name: String = "party-replication-target-processor"

  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle connect to SP") {
    // Upon connecting or reconnecting, clear the initial contract ordinal.
    processorStore.clearInitialContractOrdinalInclusive()
    progressPartyReplication()
    EitherTUtil.unitUS
  }

  /** Consume status updates and ACS batches from the source participant.
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = execute("handle payload from SP") {
    val recordOrderPublisher = connectedSynchronizer.ephemeral.recordOrderPublisher
    (for {
      messageFromSP <- EitherT.fromEither[FutureUnlessShutdown](
        PartyReplicationSourceParticipantMessage
          .fromByteString(protocolVersion, payload)
          .leftMap(_.message)
      )
      _ <- messageFromSP.dataOrStatus match {
        case PartyReplicationSourceParticipantMessage.AcsBatch(contracts) =>
          val firstContractOrdinal = processorStore.processedContractsCount
          logger.debug(
            s"Received batch beginning at contract ordinal $firstContractOrdinal with contracts ${contracts
                .map(_.contract.contractId)
                .mkString(", ")}"
          )
          val contractsToAdd = contracts.map(_.contract)
          for {
            _ <- EitherT(
              contractsToAdd.forgetNE
                .traverse(ContractInstance.fromSerializable)
                .traverse(
                  participantNodePersistentState.value.contractStore.storeContracts
                )
            )
            repairCounter = processorStore.getAndIncrementRepairCounter()
            toc = TimeOfChange(partyToParticipantEffectiveAt, Some(repairCounter))
            contractAssignments = contracts.map {
              case ActiveContractOld(synchronizerId, contract, reassignmentCounter) =>
                (
                  contract.contractId,
                  ReassignmentTag.Source(synchronizerId),
                  reassignmentCounter,
                  toc,
                )
            }
            _ <- connectedSynchronizer.synchronizerHandle.syncPersistentState.activeContractStore
              .assignContracts(contractAssignments)
              .toEitherTWithNonaborts
              .leftMap(e =>
                s"Failed to assign contracts $contractAssignments in ActiveContractStore: $e"
              )
            reassignments <- EitherT.fromEither[FutureUnlessShutdown](contracts.toNEF.traverse {
              case ActiveContractOld(_, contract, reassignmentCounter) =>
                ContractInstance
                  .fromSerializable(contract)
                  .map(ContractReassignment(_, reassignmentCounter))
            })
            _ <- EitherT.rightT[FutureUnlessShutdown, String](
              recordOrderPublisher.schedulePublishAddContracts(
                repairEventFromSerializedContract(repairCounter, reassignments)
              )
            )
            _ = processorStore
              .increaseProcessedContractsCount(PositiveInt.size(contracts))
              .discard
          } yield ()
        case PartyReplicationSourceParticipantMessage.EndOfACS =>
          logger.info(
            s"Target participant has received end of data after ${processorStore.processedContractsCount.unwrap} contracts"
          )
          processorStore.setHasEndOfACSBeenReached()
          EitherT.rightT[FutureUnlessShutdown, String](())
      }
    } yield ()).bimap(
      _.tap { error =>
        logger.warn(s"Error while processing payload: $error")
        onError(error)
      },
      _ => progressPartyReplication(),
    )
  }

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
  ): EitherT[FutureUnlessShutdown, String, Unit] = if (hasEndOfACSBeenReached) {
    val recordOrderPublisher = connectedSynchronizer.ephemeral.recordOrderPublisher
    onComplete(traceContext)
    EitherT(
      FutureUnlessShutdown
        .lift(
          recordOrderPublisher.publishBufferedEvents()
        )
        .flatMap(_ =>
          sendCompleted(
            "completing in response to source participant notification of end of data"
          ).value
        )
    )
  } else if (processorStore.initialContractOrdinalInclusiveO.isEmpty) {
    val initialContractOrdinalInclusive = processorStore.processedContractsCount
    logger.info(s"Connected. Requesting contracts from ${initialContractOrdinalInclusive.unwrap}")
    val initializeSP = PartyReplicationTargetParticipantMessage(
      PartyReplicationTargetParticipantMessage.Initialize(initialContractOrdinalInclusive)
    )(
      PartyReplicationTargetParticipantMessage.protocolVersionRepresentativeFor(protocolVersion)
    )
    sendPayload("initialize source participant", initializeSP.toByteString).map { _ =>
      // Once the SP initialize message has been sent, set the initial contract ordinal
      // and reset the requested contracts count to the processed contracts count.
      processorStore.setInitialContractOrdinalInclusive(initialContractOrdinalInclusive)
      processorStore.resetRequestedContractsCount(processorStore.processedContractsCount)
      progressPartyReplication()
    }
  } else if (processorStore.processedContractsCount == processorStore.requestedContractsCount) {
    logger.debug(
      s"Target participant has received all the contracts requested before ordinal ${processorStore.processedContractsCount.unwrap}. " +
        s"Requesting ${contractsToRequestEachTime.unwrap} more contracts from source participant"
    )
    requestNextSetOfContracts()
  } else {
    EitherT.rightT[FutureUnlessShutdown, String](())
  }

  private def requestNextSetOfContracts()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val updatedContractOrdinalRequestedExclusive =
      processorStore.increaseRequestedContractsCount(contractsToRequestEachTime)
    val inclusiveContractOrdinal = updatedContractOrdinalRequestedExclusive.unwrap - 1
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
    )
  }

  override protected def hasEndOfACSBeenReached: Boolean = processorStore.hasEndOfACSBeenReached

  private def repairEventFromSerializedContract(
      repairCounter: RepairCounter,
      activeContracts: NonEmpty[Seq[ContractReassignment]],
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Update.OnPRReassignmentAccepted = {
    val uniqueUpdateId = {
      // Add the repairCounter and contract-id to the hash to arrive at unique per-OPR updateIds.
      val hash = activeContracts
        .foldLeft {
          pureCrypto
            .build(HashPurpose.OnlinePartyReplicationId)
            .add(indexerUpdateIdBaseHash.unwrap)
            .add(repairCounter.unwrap)
        } { case (builder, ContractReassignment(contract, reassignmentCounter)) =>
          builder
            .add(reassignmentCounter.v)
            .add(contract.contractId.coid)
        }
        .finish()
      TransactionId(hash).tryAsLedgerTransactionId
    }

    val contractIdCounters = activeContracts.map {
      case ContractReassignment(contract, reassignmentCounter) =>
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
      activeContracts,
      artificialReassignmentInfo.sourceSynchronizer,
    )
    val acsChangeFactory = AcsChangeSupport.fromCommitSet(commitSet)
    Update.OnPRReassignmentAccepted(
      workflowId = None,
      updateId = uniqueUpdateId,
      reassignmentInfo = artificialReassignmentInfo,
      reassignment = Reassignment.Batch(
        activeContracts.zipWithIndex.map {
          case (ContractReassignment(contract, reassignmentCounter), idx) =>
            Reassignment.Assign(
              ledgerEffectiveTime = contract.inst.createdAt.time,
              createNode = contract.toLf,
              contractAuthenticationData = contract.inst.cantonData,
              reassignmentCounter = reassignmentCounter.v,
              nodeId = idx,
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
      requestId: Hash,
      partyToParticipantEffectiveAt: CantonTimestamp,
      onComplete: TraceContext => Unit,
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
      partyToParticipantEffectiveAt,
      onComplete,
      onError,
      onDisconnect,
      participantNodePersistentState,
      connectedSynchronizer,
      futureSupervisor,
      exitOnFatalFailures,
      timeouts,
      loggerFactory
        .append("synchronizerId", connectedSynchronizer.psid.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive)
        .append("requestId", requestId.toHexString),
      testInterceptor,
    )
}
