// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationStatus,
  PartyReplicationTestInterceptor,
}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.party.TargetParticipantAcsPersistence.contractsToRequestEachTime
import com.digitalasset.canton.participant.store.{
  AcsReplicationProgress,
  ParticipantNodePersistentState,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
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
    requestId: AddPartyRequestId,
    protected val psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    protected val replicationProgressState: AcsReplicationProgress,
    protected val onError: String => Unit,
    protected val onDisconnect: (String, TraceContext) => Unit,
    persistsContracts: TargetParticipantAcsPersistence.PersistsContracts,
    recordOrderPublisher: RecordOrderPublisher,
    requestTracker: RequestTracker,
    pureCrypto: CryptoPureApi,
    protected val futureSupervisor: FutureSupervisor,
    protected val exitOnFatalFailures: Boolean,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
    protected val testOnlyInterceptor: PartyReplicationTestInterceptor,
)(implicit override val executionContext: ExecutionContext)
    extends TargetParticipantAcsPersistence(
      requestId,
      psid,
      partyOnboardingAt,
      replicationProgressState,
      persistsContracts,
      recordOrderPublisher,
      requestTracker,
      pureCrypto,
    )
    with PartyReplicationProcessor {

  protected val processorStore: TargetParticipantStore = InMemoryProcessorStore.targetParticipant()

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
            updatedProcessedContractsCount <- importContracts(contracts)
            _ = processorStore.setProcessedContractsCount(updatedProcessedContractsCount)
          } yield ()
        case PartyReplicationSourceParticipantMessage.EndOfACS =>
          logger.info(
            s"Target participant has received end of data after ${replicatedContractCount.unwrap} contracts"
          )
          replicationProgressState
            .updateAcsReplicationProgress(
              requestId,
              PartyReplicationStatus.EphemeralSequencerChannelProgress(
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

  override def progressPartyReplication()(implicit traceContext: TraceContext): Unit =
    // Skip progress check if more than one other task is already queued that performs this same progress check or
    // is going to schedule a progress check.
    if (executionQueue.isAtMostOneTaskScheduled) {
      executeAsync(s"Respond to source participant if needed")(
        EitherTUtil.ifThenET(
          isChannelOpenForCommunication &&
            replicationProgressState
              .getAcsReplicationProgress(requestId)
              .exists(progress =>
                testOnlyInterceptor.onTargetParticipantProgress(
                  progress
                ) == PartyReplicationTestInterceptor.Proceed
              )
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

  override protected def newProgress(
      updatedProcessedContractsCount: NonNegativeInt,
      usedRepairCounter: RepairCounter,
  ): PartyReplicationStatus.AcsReplicationProgress =
    PartyReplicationStatus.EphemeralSequencerChannelProgress(
      updatedProcessedContractsCount,
      usedRepairCounter + 1,
      fullyProcessedAcs = false,
      this,
    )
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
      requestId,
      connectedSynchronizer.psid,
      partyOnboardingAt,
      replicationProgressState,
      onError,
      onDisconnect,
      new TargetParticipantAcsPersistence.PersistsContractsImpl(participantNodePersistentState),
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
}
