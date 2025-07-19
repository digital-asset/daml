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
import com.digitalasset.daml.lf.data.Bytes as LfBytes
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
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
  *   - only sends messages after receiving
  *     [[PartyReplicationSourceParticipantMessage.SourceParticipantIsReady]],
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
  *   Callback notification that the target participant has errored.
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
  private def requestedContractsCount = processorStore.requestedContractsCount
  private def processedContractsCount = processorStore.processedContractsCount
  private val contractsToRequestEachTime = PositiveInt.tryCreate(10)
  private val isSourceParticipantReady = new AtomicBoolean(false)
  private val nextRepairCounter = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)

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

  override def replicatedContractsCount: NonNegativeInt = processedContractsCount.get()

  override protected def name: String = "party-replication-target-processor"

  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherTUtil.unitUS

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
        case PartyReplicationSourceParticipantMessage.SourceParticipantIsReady =>
          logger.info("Target participant notified that source participant is ready")
          isSourceParticipantReady.set(true)
          EitherT.rightT[FutureUnlessShutdown, String](())
        case PartyReplicationSourceParticipantMessage.AcsBatch(contracts) =>
          val firstContractOrdinal = processedContractsCount.get()
          logger.debug(
            s"Received batch beginning at contract ordinal $firstContractOrdinal with contracts ${contracts
                .map(_.contract.contractId)
                .mkString(", ")}"
          )
          val contractsToAdd = contracts.map(_.contract)
          for {
            _ <- EitherT(
              contractsToAdd.forgetNE
                .traverse(ContractInstance.apply)
                .traverse(
                  participantNodePersistentState.value.contractStore.storeContracts
                )
            )
            repairCounter = RepairCounter(nextRepairCounter.getAndUpdate(_.map(_ + 1)).unwrap)
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
                ContractInstance(contract).map(ContractReassignment(_, reassignmentCounter))
            })
            _ <- EitherT.rightT[FutureUnlessShutdown, String](
              recordOrderPublisher.schedulePublishAddContracts(
                repairEventFromSerializedContract(repairCounter, reassignments)
              )
            )
            _ = processedContractsCount.updateAndGet(_.map(_ + contracts.size)).discard
          } yield ()
        case PartyReplicationSourceParticipantMessage.EndOfACS =>
          logger.info(
            s"Target participant has received end of data after ${processedContractsCount.get().unwrap} contracts"
          )
          hasEndOfACSBeenReached.set(true)
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
            ) == PartyReplicationTestInterceptor.Proceed &&
            isSourceParticipantReady.get()
        )(respondToSourceParticipant())
      )
    }

  private def respondToSourceParticipant()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = if (hasEndOfACSBeenReached.get()) {
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
  } else if (processedContractsCount.get() == requestedContractsCount.get()) {
    logger.debug(
      s"Target participant has received all the contracts requested before ordinal ${processedContractsCount.get().unwrap}. " +
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
      requestedContractsCount.updateAndGet(_.map(_ + contractsToRequestEachTime.unwrap))
    val inclusiveContractOrdinal = updatedContractOrdinalRequestedExclusive.unwrap - 1
    val instructionMessage = PartyReplicationTargetParticipantMessage(
      PartyReplicationTargetParticipantMessage.SendAcsSnapshotUpTo(
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
              contractMetadata =
                LfBytes.fromByteString(contract.metadata.toByteString(protocolVersion)),
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
