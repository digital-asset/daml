// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** The party replicator acts on behalf of the participant's online party replication requests:
  * - In response to an operator request to initiate online party replication, triggers admin workflow proposal.
  * - Exposes callbacks to the admin workflow to validate and process channel proposals and agreements.
  *
  * Unlike the [[PartyReplicationAdminWorkflow]], the [[PartyReplicator]] survives HA-activeness transitions.
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

  /** Validates online party replication arguments and propose party replication
    * via the provided admin workflow service.
    */
  private[admin] def startPartyReplication(
      args: PartyReplicationArguments,
      adminWorkflow: PartyReplicationAdminWorkflow,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val PartyReplicationArguments(
      channelId,
      partyId,
      sourceParticipantId,
      synchronizerId,
    ) = args
    if (syncService.isActive()) {
      logger.info(
        s"Initiating replication of party $partyId from participant $sourceParticipantId over synchronizer $synchronizerId using channel $channelId"
      )
      for {
        synchronizerTopologyClient <- EitherT.fromOption[FutureUnlessShutdown](
          syncService.syncCrypto.ips.forSynchronizer(synchronizerId),
          s"Unknown synchronizer $synchronizerId",
        )
        topologySnapshot = synchronizerTopologyClient.headSnapshot
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
        _ <- ensurePartyIsAuthorizedOnParticipants(
          partyId,
          sourceParticipantId,
          participantId,
          topologySnapshot,
        )
        // TODO(#20581): Extract ACS snapshot timestamp from PTP onboarding effective time
        //  Currently the synchronizer topology client does not expose the low-level effective time.
        acsSnapshotTs = topologySnapshot.timestamp
        startAtWatermark = NonNegativeInt.zero
        _ <- adminWorkflow.proposePartyReplication(
          partyId,
          synchronizerId,
          sequencerCandidates,
          acsSnapshotTs,
          startAtWatermark,
          sourceParticipantId,
          channelId,
        )
      } yield ()
    } else {
      logger.info(
        s"Participant $participantId is inactive, hence not initiating replication of party $partyId from participant $sourceParticipantId over synchronizer $synchronizerId using channel $channelId"
      )
      EitherT.rightT[FutureUnlessShutdown, String](())
    }
  }

  /** Validates a channel proposal at the source participant and chooses a sequencer to host the
    * party replication channel.
    */
  private[admin] def validateChannelProposalAtSourceParticipant(
      proposal: ChannelProposalParams
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, SequencerId] = {
    val ChannelProposalParams(
      ts,
      partyId,
      targetParticipantId,
      sequencerIdsProposed,
      synchronizerId,
    ) = proposal
    for {
      synchronizerTopologyClient <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService.syncCrypto.ips
            .forSynchronizer(synchronizerId)
            .toRight(s"Unknown synchronizer $synchronizerId")
        )
      // Insist that the topology snapshot is known by the source participant to
      // avoid unbounded wait by awaitSnapshot().
      _ <- EitherT.cond[FutureUnlessShutdown](
        synchronizerTopologyClient.snapshotAvailable(ts),
        (),
        s"Specified timestamp $ts is not yet available on participant $participantId and synchronizer $synchronizerId",
      )
      topologySnapshot <- EitherT.right(synchronizerTopologyClient.awaitSnapshotUS(ts))
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
          .toRight(s"None of the proposed sequencer are active on synchronizer $synchronizerId")
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
      _ <- ensurePartyIsAuthorizedOnParticipants(
        partyId,
        participantId,
        targetParticipantId,
        topologySnapshot,
      )
    } yield sequencerId
  }

  /** Agreement notification for the local participant to act as the source participant to replicate the specified
    * party using the provided parameters.
    */
  private[admin] def processChannelAgreementAtSourceParticipant(
      agreement: ChannelAgreementParams
  )(implicit traceContext: TraceContext): Unit =
    logger.info(
      s"Source participant ${agreement.sourceParticipantId} is ready to build party replication channel shared with " +
        s"target participant ${agreement.targetParticipantId} via sequencer ${agreement.sequencerId}."
    )

  /** Agreement notification for the local participant to act as the target participant to replicate the specified
    * party using the provided parameters.
    */
  private[admin] def processChannelAgreementAtTargetParticipant(
      agreement: ChannelAgreementParams
  )(implicit traceContext: TraceContext): Unit =
    logger.info(
      s"Target participant ${agreement.targetParticipantId} is ready to build party replication channel shared with " +
        s"source participant ${agreement.sourceParticipantId} via sequencer ${agreement.sequencerId}."
    )

  private def selectSequencerCandidates(
      synchronizerId: SynchronizerId,
      sequencerIds: NonEmpty[List[SequencerId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, NonEmpty[Seq[SequencerId]]] =
    for {
      syncDomain <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService.readySyncDomainById(synchronizerId).toRight("Synchronizer not found")
        )
      channelClient <- EitherT.fromEither[FutureUnlessShutdown](
        syncDomain.sequencerChannelClientO.toRight("Channel client not configured")
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

  private def ensurePartyIsAuthorizedOnParticipants(
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = for {
    _ <- EitherT.cond[FutureUnlessShutdown](
      sourceParticipantId != targetParticipantId,
      (),
      s"Source and target participants $sourceParticipantId cannot match",
    )
    activeParticipantsOfParty <- EitherT
      .right(
        snapshot.activeParticipantsOf(partyId.toLf).map(_.keySet)
      )
    _ <- EitherT.cond[FutureUnlessShutdown](
      activeParticipantsOfParty.contains(sourceParticipantId),
      (),
      s"Party $partyId is not hosted by source participant $sourceParticipantId",
    )
    _ <- EitherT.cond[FutureUnlessShutdown](
      activeParticipantsOfParty.contains(targetParticipantId),
      (),
      s"Party $partyId is not hosted by target participant $targetParticipantId",
    )
  } yield ()
}
