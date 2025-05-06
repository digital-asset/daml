// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.{Reassignment, ReassignmentInfo, Update}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.ActiveContractOld
import com.digitalasset.canton.participant.store.ParticipantNodePersistentState
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.{SerializableContract, TransactionId}
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ReassignmentTag}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Bytes as LfBytes
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext

/** Helper trait to name parameters
  */
trait PersistsContracts {
  def persistIndexedContracts(
      acsChunkId: NonNegativeInt,
      contracts: NonEmpty[Seq[SerializableContract]],
  ): EitherT[FutureUnlessShutdown, String, Unit]
}

/** The target participant processor ingests a party's active contracts on a specific synchronizer
  * and timestamp from a source participant as part of Online Party Replication.
  *
  * The interaction happens via the
  * [[com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor]] API and
  * the target participant processor enforces the protocol guarantees made by a
  * [[PartyReplicationSourceParticipantProcessor]]. The following guarantees made by the target
  * participant processor are verifiable at the party replication protocol: The target participant
  *   - only sends messages after receiving
  *     [[PartyReplicationSourceMessage.SourceParticipantIsReady]],
  *   - requests contracts in a strictly increasing chunk id order,
  *   - and sends only deserializable payloads.
  *
  * @param synchronizerId
  *   The synchronizer id of the synchronizer to replicate active contracts within.
  * @param partyId
  *   The party id of the party to replicate active contracts for.
  * @param partyToParticipantEffectiveAt
  *   The timestamp immediately on which the ACS snapshot is based.
  * @param onProgress
  *   Callback to update progress wrt the number of active contracts received.
  * @param onComplete
  *   Callback notification that the target participant has received the entire ACS.
  * @param protocolVersion
  *   The protocol version to use for now for the party replication protocol. Technically the online
  *   party replication protocol is a different protocol from the canton protocol.
  */
class PartyReplicationTargetParticipantProcessor(
    synchronizerId: SynchronizerId,
    partyId: PartyId,
    partyToParticipantEffectiveAt: CantonTimestamp,
    onProgress: NonNegativeInt => Unit,
    onComplete: NonNegativeInt => Unit,
    persistContracts: PersistsContracts,
    participantNodePersistentState: Eval[ParticipantNodePersistentState],
    connectedSynchronizer: ConnectedSynchronizer,
    protected val protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override val executionContext: ExecutionContext)
    extends SequencerChannelProtocolProcessor {

  private val chunksRequestedExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val chunksConsumedExclusive = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val numberOfContractsProcessed = new AtomicReference[NonNegativeInt](NonNegativeInt.zero)
  private val numberOfChunksToRequestEachTime = PositiveInt.three

  private val pureCrypto =
    connectedSynchronizer.synchronizerHandle.syncPersistentState.pureCryptoApi

  // The base hash for all indexer UpdateIds to avoid repeating this for all ACS chunks.
  private lazy val indexerUpdateIdBaseHash = pureCrypto
    .build(HashPurpose.OnlinePartyReplicationId)
    .add(partyId.toProtoPrimitive)
    .add(synchronizerId.toProtoPrimitive)
    .add(partyToParticipantEffectiveAt.toProtoPrimitive)
    .finish()

  override def onConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = EitherTUtil.unitUS

  /** Consume status updates and ACS chunks from the source participant.
    */
  override def handlePayload(payload: ByteString)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val recordOrderPublisher = connectedSynchronizer.ephemeral.recordOrderPublisher
    for {
      acsChunkOrStatus <- EitherT.fromEither[FutureUnlessShutdown](
        PartyReplicationSourceMessage
          .fromByteString(protocolVersion, payload)
          .leftMap(_.message)
      )
      _ <- acsChunkOrStatus.dataOrStatus match {
        case PartyReplicationSourceMessage.SourceParticipantIsReady =>
          logger.info("Target participant notified that source participant is ready")
          requestNextSetOfChunks().map(_ => onProgress(NonNegativeInt.zero))
        case PartyReplicationSourceMessage.AcsChunk(chunkId, contracts) =>
          logger.debug(
            s"Received chunk $chunkId with contracts ${contracts.map(_.contract.contractId).mkString(", ")}"
          )
          val contractsToAdd = contracts.map(_.contract)
          for {
            _ <- persistContracts.persistIndexedContracts(chunkId, contractsToAdd)
            _ <- EitherT.right(
              participantNodePersistentState.value.contractStore.storeContracts(contractsToAdd)
            )
            repairCounter = RepairCounter(chunkId.unwrap)
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
            _ <- EitherT.rightT[FutureUnlessShutdown, String](
              recordOrderPublisher.schedulePublishAddContracts(
                repairEventFromSerializedContract(repairCounter, contracts)
              )
            )
            chunkConsumedUpToExclusive = chunksConsumedExclusive.updateAndGet(_.map(_ + 1))
            numContractsProcessed = numberOfContractsProcessed.updateAndGet(
              _.map(_ + contracts.size)
            )
            _ = onProgress(numContractsProcessed)
            _ <-
              if (chunkConsumedUpToExclusive == chunksConsumedExclusive.get()) {
                // Create a new trace context and log the old and new trace ids, so that we don't end up with
                // a conversation that consists of only one trace id from beginning to end.
                TraceContext.withNewTraceContext { newTraceContext =>
                  logger.debug(
                    s"Target participant has received all the chunks requested before ${chunkConsumedUpToExclusive.unwrap}." +
                      s"Requesting ${numberOfChunksToRequestEachTime.unwrap} more chunks from source participant (new tid: ${newTraceContext.traceId})"
                  )
                  requestNextSetOfChunks()(newTraceContext)
                }
              } else EitherTUtil.unitUS[String]
          } yield ()
        case PartyReplicationSourceMessage.EndOfACS =>
          logger.info(
            s"Target participant has received end of data after ${chunksConsumedExclusive.get().unwrap} chunks"
          )
          onComplete(numberOfContractsProcessed.get())
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
      }
    } yield ()
  }

  private def requestNextSetOfChunks()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val updatedChunkCountRequestedExclusive =
      chunksRequestedExclusive.updateAndGet(_.map(_ + numberOfChunksToRequestEachTime.unwrap))
    val inclusiveAcsChunkCounter = updatedChunkCountRequestedExclusive.unwrap - 1
    val instruction = PartyReplicationInstruction(
      NonNegativeInt.tryCreate(inclusiveAcsChunkCounter)
    )(
      PartyReplicationInstruction.protocolVersionRepresentativeFor(protocolVersion)
    )
    sendPayload(
      s"request next set of chunks up to $inclusiveAcsChunkCounter",
      instruction.toByteString,
    )
  }

  private def repairEventFromSerializedContract(
      repairCounter: RepairCounter,
      activeContracts: NonEmpty[Seq[ActiveContractOld]],
  )(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): NonEmpty[Seq[Update.RepairReassignmentAccepted]] =
    activeContracts
      .groupBy(_.synchronizerId)
      .toSeq
      .map { case (synchronizerId, contracts) =>
        val uniqueUpdateId = {
          // Add the repairCounter and contract-id to the hash to arrive at unique per-OPR updateIds.
          val hash = contracts
            .foldLeft {
              pureCrypto
                .build(HashPurpose.OnlinePartyReplicationId)
                .add(indexerUpdateIdBaseHash.unwrap)
            } { case (builder, ActiveContractOld(_, contract, reassignmentCounter)) =>
              builder
                .add(repairCounter.unwrap)
                .add(contract.contractId.coid)
            }
            .finish()
          TransactionId(hash).tryAsLedgerTransactionId
        }
        Update.RepairReassignmentAccepted(
          workflowId = None,
          updateId = uniqueUpdateId,
          reassignmentInfo = ReassignmentInfo(
            sourceSynchronizer = ReassignmentTag.Source(synchronizerId),
            targetSynchronizer = ReassignmentTag.Target(synchronizerId),
            submitter = None,
            unassignId = timestamp, // artificial unassign has same timestamp as assign
            isReassigningParticipant = false,
          ),
          reassignment = Reassignment.Batch(
            contracts.zipWithIndex.map {
              case (ActiveContractOld(_, contract, reassignmentCounter), idx) =>
                Reassignment.Assign(
                  ledgerEffectiveTime = contract.ledgerCreateTime.toLf,
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
          synchronizerId = synchronizerId,
        )
      }

  override def onDisconnected(status: Either[String, Unit])(implicit
      traceContext: TraceContext
  ): Unit = ()
}

object PartyReplicationTargetParticipantProcessor {
  def apply(
      synchronizerId: SynchronizerId,
      partyId: PartyId,
      partyToParticipantEffectiveAt: CantonTimestamp,
      onProgress: NonNegativeInt => Unit,
      onComplete: NonNegativeInt => Unit,
      persistContracts: PersistsContracts,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      connectedSynchronizer: ConnectedSynchronizer,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): PartyReplicationTargetParticipantProcessor =
    new PartyReplicationTargetParticipantProcessor(
      synchronizerId,
      partyId,
      partyToParticipantEffectiveAt,
      onProgress,
      onComplete,
      persistContracts,
      participantNodePersistentState,
      connectedSynchronizer,
      protocolVersion,
      timeouts,
      loggerFactory
        .append("synchronizerId", synchronizerId.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive),
    )
}
