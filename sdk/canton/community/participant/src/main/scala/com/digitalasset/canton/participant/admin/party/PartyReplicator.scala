// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransaction, TimeQuery, TopologyStore}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  ParticipantPermission,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PartyId,
  SequencerId,
  SynchronizerId,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** The party replicator acts on behalf of the participant's online party replication requests:
  *   - In response to an operator request to initiate online party replication, triggers admin
  *     workflow proposal.
  *   - Exposes callbacks to the admin workflow to validate and process channel proposals and
  *     agreements.
  *
  * Unlike the [[PartyReplicationAdminWorkflow]], the [[PartyReplicator]] survives HA-activeness
  * transitions.
  */
final class PartyReplicator(
    participantId: ParticipantId,
    syncService: CantonSyncService,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    pingParallelism: Int = 4,
)(implicit
    executionContext: ExecutionContext
) extends FlagCloseable
    with NamedLogging {

  /** Validates online party replication arguments and propose party replication via the provided
    * admin workflow service.
    */
  private[admin] def addPartyAsync(
      args: PartyReplicationArguments,
      adminWorkflow: PartyReplicationAdminWorkflow,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Hash] = {
    val PartyReplicationArguments(partyId, synchronizerId, sourceParticipantIdO, serialO) = args
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        syncService.isActive(),
        logger.info(
          s"Initiating replication of party $partyId from participant $sourceParticipantIdO on synchronizer $synchronizerId"
        ),
        s"Participant $participantId is inactive",
      )
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight(s"Unknown synchronizer $synchronizerId")
        )
      topologySnapshot = connectedSynchronizer.synchronizerHandle.topologyClient.headSnapshot
      sequencerIds <- EitherT
        .fromOptionF(
          topologySnapshot
            .sequencerGroup()
            .map(sg => NonEmpty.from(sg.toList.flatMap(_.active))),
          s"No active sequencer for synchronizer $synchronizerId",
        )
      sequencerCandidates <- selectSequencerCandidates(
        synchronizerId,
        sequencerIds,
      )
      syncPersistentState = connectedSynchronizer.synchronizerHandle.syncPersistentState
      sourceParticipantId <- ensurePartyHostingTopologyIsAsExpected(
        partyId,
        sourceParticipantIdO,
        participantId,
        syncPersistentState.topologyStore,
        serialO,
      )
      partyReplicationId =
        syncPersistentState.pureCryptoApi
          .build(HashPurpose.OnlinePartyReplicationId)
          .add(partyId.toProtoPrimitive)
          .add(synchronizerId.toProtoPrimitive)
          .add(sourceParticipantId.toProtoPrimitive)
          .finish()
      _ <- adminWorkflow.proposePartyReplication(
        partyReplicationId,
        partyId,
        synchronizerId,
        sourceParticipantId,
        sequencerCandidates,
        serialO,
      )
    } yield partyReplicationId
  }

  /** Validates a channel proposal at the source participant and chooses a sequencer to host the
    * party replication channel.
    */
  private[admin] def validatePartyReplicationProposalAtSourceParticipant(
      proposal: PartyReplicationProposalParams
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, SequencerId] = {
    val PartyReplicationProposalParams(
      _,
      partyId,
      synchronizerId,
      targetParticipantId,
      sequencerIdsProposed,
      serialO,
    ) = proposal
    for {
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight(s"Synchronizer $synchronizerId not connected")
        )
      topologySnapshot = connectedSynchronizer.synchronizerHandle.topologyClient.headSnapshot
      sequencerIdsInTopology <- EitherT
        .fromOptionF(
          topologySnapshot.sequencerGroup().map(_.map(_.active)),
          s"No sequencer group for synchronizer $synchronizerId",
        )
      sequencerIdsTopologyIntersection <- EitherT.fromEither[FutureUnlessShutdown](
        NonEmpty
          .from(
            sequencerIdsProposed.forgetNE.filter(sequencerId =>
              sequencerIdsInTopology
                .contains(sequencerId)
                .tap(isKnown =>
                  if (!isKnown)
                    logger
                      .info(
                        s"Skipping sequencer $sequencerId not active on synchronizer $synchronizerId"
                      )
                )
            )
          )
          .toRight(s"None of the proposed sequencers are active on synchronizer $synchronizerId")
      )
      candidateSequencerIds <- selectSequencerCandidates(
        synchronizerId,
        sequencerIdsTopologyIntersection,
      )
      sequencerId <- EitherT.fromEither[FutureUnlessShutdown](
        candidateSequencerIds.headOption.toRight("No common sequencer")
      )
      _ = logger.info(
        s"Choosing sequencer $sequencerId among ${candidateSequencerIds.mkString(",")}"
      )
      _ <- ensurePartyHostingTopologyIsAsExpected(
        partyId,
        Some(participantId),
        targetParticipantId,
        connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore,
        serialO,
      )
    } yield sequencerId
  }

  /** Agreement notification for the local participant to act as the source participant to replicate
    * the specified party using the provided parameters.
    */
  private[admin] def processPartyReplicationAgreementAtSourceParticipant(
      agreement: PartyReplicationAgreementParams
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Source participant ${agreement.sourceParticipantId} is ready to propose party to participant topology change " +
        s"to add target participant ${agreement.targetParticipantId} on behalf of party replication ${agreement.partyReplicationId}."
    )
    proposePartyToParticipantTopologyWithTargetParticipant(agreement)
  }

  /** Agreement notification for the local participant to act as the target participant to replicate
    * the specified party using the provided parameters.
    */
  private[admin] def processPartyReplicationAgreementAtTargetParticipant(
      agreement: PartyReplicationAgreementParams
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Target participant ${agreement.sourceParticipantId} is ready to propose party to participant topology change " +
        s"to add target participant ${agreement.targetParticipantId} on behalf of party replication ${agreement.partyReplicationId}."
    )
    proposePartyToParticipantTopologyWithTargetParticipant(agreement)
  }

  private def proposePartyToParticipantTopologyWithTargetParticipant(
      agreement: PartyReplicationAgreementParams
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val PartyReplicationAgreementParams(
      _,
      partyId,
      synchronizerId,
      _,
      targetParticipantId,
      _,
      serialO,
    ) = agreement
    val eitherT = for {
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight(s"Synchronizer $synchronizerId not connected")
        )
      syncPersistentState = connectedSynchronizer.synchronizerHandle.syncPersistentState
      partyToParticipantTopologyHeadTx <- partyToParticipantTopologyHead(
        partyId,
        syncPersistentState.topologyStore,
      )
      partyToParticipantMapping = partyToParticipantTopologyHeadTx.mapping
      proposedPartyToParticipantMapping <- EitherT.fromEither[FutureUnlessShutdown](
        PartyToParticipant.create(
          partyId,
          partyToParticipantMapping.threshold,
          partyToParticipantMapping.participants :+ HostingParticipant(
            targetParticipantId,
            ParticipantPermission.Observation,
          ),
        )
      )
      _ <- syncPersistentState.topologyManager
        .proposeAndAuthorize(
          op = TopologyChangeOp.Replace,
          mapping = proposedPartyToParticipantMapping,
          serial = serialO,
          signingKeys = Seq.empty,
          protocolVersion = syncPersistentState.topologyManager.managerVersion.serialization,
          expectFullAuthorization = false,
          forceChanges = ForceFlags.none,
          waitToBecomeEffective = Some(timeouts.network.asNonNegativeFiniteApproximation),
        )
        .leftMap { err =>
          val exception = err.asGrpcError
          logger.warn(
            s"Error proposing party to participant topology change on $participantId",
            exception,
          )
          exception.getMessage
        }
    } yield ()

    timeouts.default.awaitUS("Process OnPR agreement")(eitherT.value) match {
      case UnlessShutdown.AbortedDueToShutdown =>
        logger.warn(s"OnPR ${agreement.partyReplicationId} aborted due to shutdown")
      case UnlessShutdown.Outcome(Right(_)) =>
        logger.info(
          s"Successfully added OnPR ${agreement.partyReplicationId} TP ${agreement.targetParticipantId} to topology on behalf of party ${agreement.partyId}"
        )
      case UnlessShutdown.Outcome(Left(err)) =>
        logger.warn(
          s"Failed to add OnPR ${agreement.partyReplicationId} TP ${agreement.targetParticipantId} to topology on behalf of party ${agreement.partyId}: $err"
        )
    }
  }

  private def selectSequencerCandidates(
      synchronizerId: SynchronizerId,
      sequencerIds: NonEmpty[List[SequencerId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonEmpty[Seq[SequencerId]]] =
    for {
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(synchronizerId)
            .toRight("Synchronizer not found")
        )
      channelClient <- EitherT.fromEither[FutureUnlessShutdown](
        connectedSynchronizer.sequencerChannelClientO.toRight("Channel client not configured")
      )
      // Only propose sequencers on which the target participant can perform a channel ping.
      withChannelSupport <- EitherT.right[String](
        MonadUtil
          .parTraverseWithLimit(pingParallelism)(sequencerIds)(sequencerId =>
            channelClient
              .ping(sequencerId)
              .fold(
                err => {
                  logger.info(s"Skipping sequencer $sequencerId: $err")
                  None
                },
                _ => Some(sequencerId),
              )
          )
          .map(_.flatten)
      )
      nonEmpty <- EitherT.fromOption[FutureUnlessShutdown](
        NonEmpty.from(withChannelSupport),
        s"No sequencers ${sequencerIds.mkString(",")} support channels",
      )
    } yield nonEmpty

  /** Checks that the party is
    *   - hosted by the source participant
    *   - not yet hosted by the target participant, but can be proposed to be with the provided
    *     serial if specified
    */
  private def ensurePartyHostingTopologyIsAsExpected(
      partyId: PartyId,
      sourceParticipantIdO: Option[ParticipantId],
      targetParticipantId: ParticipantId,
      topologyStore: TopologyStore[SynchronizerStore],
      serialO: Option[PositiveInt],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, ParticipantId] =
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        !sourceParticipantIdO.contains(targetParticipantId),
        (),
        s"Source and target participants $targetParticipantId cannot match",
      )
      partyToParticipantTopologyHeadTx <- partyToParticipantTopologyHead(partyId, topologyStore)
      activeParticipantsOfParty = partyToParticipantTopologyHeadTx.mapping.participants.map(
        _.participantId
      )
      participantsExceptTargetParticipant = activeParticipantsOfParty.filterNot(
        _ == targetParticipantId
      )
      sourceParticipantId <- EitherT.fromEither[FutureUnlessShutdown](sourceParticipantIdO match {
        case None =>
          Either
            .cond(
              participantsExceptTargetParticipant.sizeCompare(1) <= 0,
              (),
              s"No source participant specified and could not infer single source participant for party $partyId among ${participantsExceptTargetParticipant
                  .mkString(",")}",
            )
            .flatMap(_ =>
              participantsExceptTargetParticipant.headOption
                .toRight(s"No source participant available to replicate party $partyId from")
            )
        case Some(sourcePid) =>
          Either.cond(
            participantsExceptTargetParticipant.contains(sourcePid),
            sourcePid,
            s"Party $partyId is not hosted by source participant $sourcePid. Only hosted on ${activeParticipantsOfParty
                .mkString(",")}",
          )
      })
      _ <- EitherT.cond[FutureUnlessShutdown](
        !activeParticipantsOfParty.contains(targetParticipantId),
        (),
        s"Party $partyId is already hosted by target participant $targetParticipantId",
      )
      expectedSerial = partyToParticipantTopologyHeadTx.serial.increment
      _ <- EitherT.cond[FutureUnlessShutdown](
        serialO.forall(_ == expectedSerial),
        (),
        s"Specified serial $serialO does not match the expected serial $expectedSerial add $partyId to $targetParticipantId.",
      )
    } yield sourceParticipantId

  private def partyToParticipantTopologyHead(
      partyId: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StoredTopologyTransaction[Replace, PartyToParticipant]] =
    EitherT(
      topologyStore
        .inspect(
          proposals = false,
          timeQuery = TimeQuery.HeadState,
          asOfExclusiveO = None,
          op = Some(TopologyChangeOp.Replace),
          types = Seq(TopologyMapping.Code.PartyToParticipant),
          idFilter = Some(partyId.uid.identifier.str),
          namespaceFilter = Some(partyId.uid.namespace.filterString),
        )
        .map(
          _.collectOfMapping[PartyToParticipant]
            .collectOfType[TopologyChangeOp.Replace]
            .result
            .headOption
            .toRight(
              s"Party $partyId not hosted on synchronizer ${topologyStore.storeId.synchronizerId}"
            )
        )
    )
}
