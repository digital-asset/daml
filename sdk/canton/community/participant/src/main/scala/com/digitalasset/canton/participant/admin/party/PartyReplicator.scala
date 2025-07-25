// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AuthorizedReplicationParams,
  ConnectedReplicationParams,
  ConnectionEstablished,
  PartyReplicationStatus,
  PartyReplicationStatusCode,
}
import com.digitalasset.canton.participant.config.UnsafeOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationProcessor,
  PartyReplicationSourceParticipantProcessor,
  PartyReplicationTargetParticipantProcessor,
}
import com.digitalasset.canton.participant.sync.{CantonSyncService, ConnectedSynchronizer}
import com.digitalasset.canton.resource.DbExceptionRetryPolicy
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TimeQuery, TopologyStore}
import com.digitalasset.canton.topology.transaction.{
  PartyToParticipant,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{
  FutureUnlessShutdownUtil,
  MonadUtil,
  SimpleExecutionQueue,
  retry,
}

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

/** The party replicator acts on behalf of the participant's online party replication requests:
  *   - In response to an operator request to initiate online party replication, triggers admin
  *     workflow proposal.
  *   - Exposes callbacks to the admin workflow to validate and process channel proposals and
  *     agreements.
  *
  * The party replicator conceptually owns the party replication admin workflow and implements the
  * gRPC party management service endpoints related to online party replication, but for practical
  * reasons its lifetime is controlled by the admin workflow service. This helps ensure that upon
  * participant HA-activeness changes, the party replication-related classes are all created or
  * closed in unison.
  */
final class PartyReplicator(
    participantId: ParticipantId,
    syncService: CantonSyncService,
    clock: Clock,
    config: UnsafeOnlinePartyReplicationConfig,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    pingParallelism: PositiveInt = PartyReplicator.defaultPingParallelism,
    progressSchedulingInterval: PositiveFiniteDuration =
      PartyReplicator.defaultProgressSchedulingInterval,
)(implicit
    executionContext: ExecutionContext
) extends FlagCloseable
    with NamedLogging {
  private type AddPartyRequestId = Hash

  private val partyReplications =
    new TrieMap[AddPartyRequestId, PartyReplicationStatus.PartyReplicationStatus]()

  private val executionQueue = new SimpleExecutionQueue(
    "party-replicator-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
    crashOnFailure = exitOnFatalFailures,
  )

  private val progressSchedulingActive = new AtomicBoolean(false)

  private val topologyWorkflow =
    new PartyReplicationTopologyWorkflow(participantId, timeouts, loggerFactory)

  private val testInterceptorO: Option[PartyReplicationTestInterceptor] =
    config.testInterceptor.map(_())

  /** Validates online party replication arguments and propose party replication via the provided
    * admin workflow service.
    */
  private[admin] def addPartyAsync(
      args: PartyReplicationArguments,
      adminWorkflow: PartyReplicationAdminWorkflow,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Hash] =
    executionQueue.executeEUS(
      {
        val PartyReplicationArguments(
          partyId,
          synchronizerId,
          sourceParticipantId,
          serial,
          participantPermission,
        ) = args
        for {
          _ <- EitherT.cond[FutureUnlessShutdown](
            syncService.isActive(),
            logger.info(
              s"Initiating replication of party $partyId from participant $sourceParticipantId on synchronizer $synchronizerId"
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
          sourceParticipantId <- ensurePartyHostedBySourceButNotTargetParticipant(
            partyId,
            sourceParticipantId,
            participantId,
            syncPersistentState.topologyStore,
            serial,
          )
          requestId =
            syncPersistentState.pureCryptoApi
              .build(HashPurpose.OnlinePartyReplicationId)
              .add(partyId.toProtoPrimitive)
              .add(synchronizerId.toProtoPrimitive)
              .add(sourceParticipantId.toProtoPrimitive)
              .add(serial.unwrap)
              .finish()
          _ <- ensureCanAddParty()
          _ <- adminWorkflow.proposePartyReplication(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            sequencerCandidates,
            serial,
            participantPermission,
          )
        } yield {
          val newStatus = PartyReplicationStatus.ProposalProcessed(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            participantId,
            serial,
            participantPermission,
          )
          partyReplications
            .put(requestId, newStatus)
            .fold(logger.info(s"Party replication $requestId proposal processed"))(alreadyExists =>
              // TODO(#23850): Conflict suggests a coding bug, but could possibly be a (low impact?) attack of a TP toward this TP.
              logger.warn(
                s"Overwrote already existing party replication $requestId that had status $alreadyExists"
              )
            )
          activateProgressMonitoring(requestId)
          requestId
        }
      },
      s"add party ${args.partyId} on ${args.synchronizerId}",
    )

  private def ensureCanAddParty(): EitherT[FutureUnlessShutdown, String, Unit] = for {
    _ <- EitherT.fromEither[FutureUnlessShutdown](
      partyReplications
        .collectFirst { case (id, error @ PartyReplicationStatus.Error(_, _)) =>
          id -> error
        }
        .fold(Right(()): Either[String, Unit]) {
          case (id, PartyReplicationStatus.Error(errorMsg, previousStatus)) =>
            Left(
              s"Participant $participantId has encountered previous error \"$errorMsg\" during add_party_async" +
                s" request $id after status $previousStatus and needs to be repaired"
            )
        }
    )
    _ <- EitherT.fromEither[FutureUnlessShutdown](
      partyReplications
        .collectFirst {
          case (id, status) if status.code != PartyReplicationStatusCode.Completed =>
            id -> status
        }
        .fold(Right(()): Either[String, Unit]) { case (id, status) =>
          Left(
            s"Only a single party replication can be in progress: $status, but found party replication $id" +
              s" of party ${status.params.partyId} on synchronizer ${status.params.synchronizerId}" +
              s" with status ${status.code}"
          )
        }
    )
  } yield ()

  private[admin] def getAddPartyStatus(
      addPartyRequestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Option[PartyReplicationStatus] = {
    val maybeStatus = partyReplications.get(addPartyRequestId)
    logger.info(s"Get party replication status: $addPartyRequestId found: $maybeStatus")
    maybeStatus
  }

  /** Validates a channel proposal at the source participant and chooses a sequencer to participate
    * in party replication and respond accordingly by invoking the provided admin workflow callback.
    */
  private[admin] def processPartyReplicationProposalAtSourceParticipant(
      proposalOrError: Either[String, PartyReplicationProposalParams],
      respondToProposal: Either[String, PartyReplicationAgreementParams] => Unit,
  )(implicit traceContext: TraceContext): Unit = {
    val operation = proposalOrError.fold(
      err => s"reject party replication: $err",
      params => s"respond to party replication proposal ${params.requestId}",
    )
    executeAsyncWithCustomResultHandling(operation, operation) {
      for {
        proposal <- EitherT.fromEither[FutureUnlessShutdown](proposalOrError)
        PartyReplicationProposalParams(
          _,
          partyId,
          synchronizerId,
          targetParticipantId,
          sequencerIdsProposed,
          serial,
          _,
        ) = proposal
        connectedSynchronizer <-
          EitherT.fromEither[FutureUnlessShutdown](
            syncService
              .readyConnectedSynchronizerById(synchronizerId)
              .toRight(s"Synchronizer $synchronizerId not connected")
          )
        _ <- ensureCanAddParty()
        topologySnapshot =
          connectedSynchronizer.synchronizerHandle.topologyClient.headSnapshot
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
            .toRight(
              s"None of the proposed sequencers are active on synchronizer $synchronizerId"
            )
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
        _ <- ensurePartyHostedBySourceButNotTargetParticipant(
          partyId,
          participantId,
          targetParticipantId,
          connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore,
          serial,
        )
      } yield PartyReplicationAgreementParams.fromProposal(proposal, participantId, sequencerId)
    } { _ =>
      _.value.map { agreementResponseE =>
        // Respond to the proposal depending on the outcome of the agreement response.
        respondToProposal(agreementResponseE)

        // Upon success indicate that the SP has processed the proposal.
        agreementResponseE.foreach { response =>
          val newStatus = PartyReplicationStatus.ProposalProcessed(
            response.requestId,
            response.partyId,
            response.synchronizerId,
            response.sourceParticipantId,
            response.targetParticipantId,
            response.serial,
            response.participantPermission,
          )
          logger.info(
            s"Party replication ${response.requestId} proposal processed at source participant"
          )
          partyReplications.put(response.requestId, newStatus).foreach { alreadyExists =>
            // TODO(#23850): Conflict could possibly be a (low impact?) attack of a TP toward this SP.
            logger.warn(
              s"Overwrote already existing party replication ${response.requestId} that had status $alreadyExists"
            )
          }
          activateProgressMonitoring(response.requestId)
        }
      }
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
    *     serial
    */
  private def ensurePartyHostedBySourceButNotTargetParticipant(
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      topologyStore: TopologyStore[SynchronizerStore],
      serial: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, ParticipantId] =
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        sourceParticipantId != targetParticipantId,
        (),
        s"Source and target participants $targetParticipantId cannot match",
      )
      partyToParticipantTopologyHeadTx <- topologyWorkflow.partyToParticipantTopologyHead(
        partyId,
        topologyStore,
      )
      activeParticipantsOfParty = partyToParticipantTopologyHeadTx.mapping.participants.map(
        _.participantId
      )
      participantsExceptTargetParticipant = activeParticipantsOfParty.filterNot(
        _ == targetParticipantId
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          participantsExceptTargetParticipant.contains(sourceParticipantId),
          (),
          s"Party $partyId is not hosted by source participant $sourceParticipantId. Only hosted on ${activeParticipantsOfParty
              .mkString(",")}",
        )
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        !activeParticipantsOfParty.contains(targetParticipantId),
        (),
        s"Party $partyId is already hosted by target participant $targetParticipantId",
      )
      expectedSerial = partyToParticipantTopologyHeadTx.serial.increment
      _ <- EitherT.cond[FutureUnlessShutdown](
        serial == expectedSerial,
        (),
        s"Specified serial $serial does not match the expected serial $expectedSerial add $partyId to $targetParticipantId.",
      )
    } yield sourceParticipantId

  /** Party replication agreement notification
    */
  private[admin] def processPartyReplicationAgreement(
      agreementParams: PartyReplicationAgreementParams
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(agreementParams.requestId, "process agreement of party replication") {
      val requestId = agreementParams.requestId
      val agreementReceived = PartyReplicationStatus.AgreementAccepted(agreementParams)
      for {
        state <- ensureParticipantStateAndSynchronizerConnected[
          PartyReplicationStatus.ProposalProcessed
        ](requestId)
        (PartyReplicationStatus.ProposalProcessed(params), _, _) = state
        // double-check that the agreement parameters match the proposed parameters
        expectedAgreementAccepted = PartyReplicationStatus.AgreementAccepted(
          params,
          agreementParams.sequencerId,
        )
        _ <- EitherT
          .cond[FutureUnlessShutdown](
            agreementReceived == expectedAgreementAccepted,
            (),
            s"The party replication $requestId agreement received $agreementReceived does not match the expected agreement $expectedAgreementAccepted",
          )
      } yield {
        logger.info(
          s"Party replication $requestId agreement accepted for party ${agreementParams.partyId}"
        )
        partyReplications.put(requestId, expectedAgreementAccepted).discard
        activateProgressMonitoring(requestId)
      }
    }

  private def authorizeOnboardingTopology(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): Unit =
    executeAsync(requestId, s"authorize party replication topology")(
      for {
        state <- ensureParticipantStateAndSynchronizerConnected[
          PartyReplicationStatus.AgreementAccepted
        ](requestId)
        (PartyReplicationStatus.AgreementAccepted(params, sequencerId), connectedSynchronizer, _) =
          state
        authorizedAtO <- topologyWorkflow.authorizeOnboardingTopology(
          params,
          connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyManager,
          connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore,
        )
        // To be sure the authorization has become effective, wait until the topology change is visible via the ledger api
        _ <- authorizedAtO match {
          case Some(authorizedAt) =>
            val operation = s"observe ${params.partyId} topology transaction via ledger api"
            retryUntilLocalStoreUpdatedInExpectedState(operation)(
              synchronizeWithClosingF(_)(
                syncService.participantNodePersistentState.value.ledgerApiStore
                  .topologyEventOffsetPublishedOnRecordTime(
                    params.synchronizerId,
                    authorizedAt,
                  )
                  .map(offsetO => Either.cond(offsetO.nonEmpty, (), s"failed to $operation"))
              )
            )
          case None => EitherT.rightT[FutureUnlessShutdown, String](())
        }
      } yield {
        val (partyId, serial) = (params.partyId, params.serial)
        authorizedAtO.fold(
          logger.debug(
            s"Onboarding topology for party replication $requestId and party $partyId not yet authorized."
          )
        ) { authorizedAt =>
          val newStatus =
            PartyReplicationStatus.TopologyAuthorized(params, sequencerId, authorizedAt)
          logger.info(
            s"Party replication $requestId onboarding topology of party $partyId authorized with serial $serial and effective time $authorizedAt"
          )
          partyReplications.put(requestId, newStatus).discard
        }
      }
    )

  private def partiesHostedByParticipant(
      participantId: ParticipantId,
      except: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
      asOfExclusive: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Set[LfPartyId]] =
    // TODO(#25766): add topology client endpoint
    EitherT(
      topologyStore
        .inspect(
          proposals = false,
          timeQuery = TimeQuery.Snapshot(asOfExclusive),
          asOfExclusiveO = None, // ignored for TimeQuery.Snapshot; always exclusive
          op = Some(TopologyChangeOp.Replace),
          types = Seq(TopologyMapping.Code.PartyToParticipant),
          idFilter = None,
          namespaceFilter = None,
        )
        .map(topologyTxns =>
          Right(
            topologyTxns
              .collectOfMapping[PartyToParticipant]
              .collectOfType[TopologyChangeOp.Replace]
              .result
              .filter { x =>
                val ptp = x.mapping
                ptp.partyId != except &&
                ptp.participants.exists(_.participantId == participantId)
              }
              .map(_.mapping.partyId.toLf)
              .toSet
          ): Either[String, Set[LfPartyId]]
        )
    )

  private def connectToSequencerChannel(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(requestId, "connect to sequencer channel on behalf of party replication")(
      for {
        state <- ensureParticipantStateAndSynchronizerConnected[
          PartyReplicationStatus.TopologyAuthorized
        ](requestId)
        (status, connectedSynchronizer, channelClient) = state
        AuthorizedReplicationParams(params, sequencerId, effectiveAt) = status.authorizedParams
        processorInfo <-
          if (participantId == params.sourceParticipantId) {
            partiesHostedByParticipant(
              status.params.targetParticipantId,
              params.partyId,
              connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore,
              effectiveAt,
            ).map(partiesAlreadyHostedByTargetParticipant =>
              (
                PartyReplicationSourceParticipantProcessor(
                  connectedSynchronizer.psid,
                  params.partyId,
                  requestId,
                  effectiveAt,
                  partiesAlreadyHostedByTargetParticipant,
                  connectedSynchronizer.synchronizerHandle.syncPersistentState.acsInspection,
                  markComplete(requestId),
                  recordError(requestId, traceContext),
                  markDisconnected(requestId),
                  futureSupervisor,
                  exitOnFatalFailures,
                  timeouts,
                  loggerFactory,
                  testInterceptorO.getOrElse(PartyReplicationTestInterceptor.AlwaysProceed),
                ): PartyReplicationProcessor,
                status.params.targetParticipantId,
                false,
              )
            )
          } else if (participantId == params.targetParticipantId) {
            EitherT.rightT[FutureUnlessShutdown, String](
              (
                PartyReplicationTargetParticipantProcessor(
                  params.partyId,
                  requestId,
                  effectiveAt,
                  markComplete(requestId),
                  recordError(requestId, traceContext),
                  markDisconnected(requestId),
                  syncService.participantNodePersistentState,
                  connectedSynchronizer,
                  futureSupervisor,
                  exitOnFatalFailures,
                  timeouts,
                  loggerFactory,
                  testInterceptorO.getOrElse(PartyReplicationTestInterceptor.AlwaysProceed),
                ): PartyReplicationProcessor,
                status.params.sourceParticipantId,
                true,
              )
            )
          } else {
            EitherT.leftT[
              FutureUnlessShutdown,
              (PartyReplicationProcessor, ParticipantId, Boolean),
            ](
              s"participant $participantId is neither source nor target"
            )
          }
        (processor, counterParticipantId, isSessionKeyOwner) = processorInfo
        _ <- channelClient
          .connectToSequencerChannel(
            sequencerId,
            SequencerChannelId(requestId.toHexString),
            counterParticipantId,
            processor,
            isSessionKeyOwner,
            effectiveAt,
          )
          .mapK(FutureUnlessShutdown.liftK)
      } yield {
        val newStatus =
          ConnectionEstablished(ConnectedReplicationParams(status.authorizedParams, processor))
        logger.info(s"Party replication $requestId connected to sequencer $sequencerId")
        partyReplications.put(requestId, newStatus).discard
      }
    )

  private def attemptToReconnectToSequencerChannel(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(requestId, "connect to sequencer channel on behalf of party replication")(
      for {
        state <- ensureParticipantStateAndSynchronizerConnected[
          PartyReplicationStatus.Disconnected
        ](requestId)
        (status, _, channelClient) = state
        ConnectedReplicationParams(
          authorizedParams @ AuthorizedReplicationParams(params, sequencerId, effectiveAt),
          processor,
        ) = status.previousConnectedStatus.connectedParams
        processorInfo <-
          if (participantId == params.sourceParticipantId) {
            EitherT.rightT[FutureUnlessShutdown, String](
              (processor, status.params.targetParticipantId, /* isSessionKeyOwner = */ false)
            )
          } else if (participantId == params.targetParticipantId) {
            EitherT.rightT[FutureUnlessShutdown, String](
              (processor, status.params.sourceParticipantId, /* isSessionKeyOwner = */ true)
            )
          } else {
            EitherT.leftT[
              FutureUnlessShutdown,
              (PartyReplicationProcessor, ParticipantId, Boolean),
            ](
              s"participant $participantId is neither source nor target"
            )
          }
        (processor, counterParticipantId, isSessionKeyOwner) = processorInfo
        // Error during attempt to reconnect should not terminally fail OnPR.
        // Instead, turn an error into an optional message as an indication that
        // a retry is warranted.
        cannotReconnectMessageO <- EitherT
          .right[String](
            // Before attempting to reconnect via the bidirectionally streaming request,
            // try a channel-ping because the former does not return an error immediately,
            // but subsequently produces another disconnect. Doing a ping lowers the
            // chances of unnecessary and noisy status-change toggling.
            channelClient
              .ping(sequencerId)
              .flatMap(_ =>
                channelClient
                  .connectToSequencerChannel(
                    sequencerId,
                    SequencerChannelId(requestId.toHexString),
                    counterParticipantId,
                    processor,
                    isSessionKeyOwner,
                    effectiveAt,
                  )
                  .mapK(FutureUnlessShutdown.liftK)
              )
              .value
              .map(_.swap.toOption)
          )
      } yield {
        cannotReconnectMessageO.fold {
          val newStatus =
            ConnectionEstablished(ConnectedReplicationParams(authorizedParams, processor))
          logger.info(s"Party replication $requestId reconnected to sequencer $sequencerId")
          partyReplications.put(requestId, newStatus).discard
        } { cannotReconnectMsg =>
          logger.info(
            s"Party replication $requestId not yet able to reconnect to sequencer $sequencerId: $cannotReconnectMsg"
          )
        }
      }
    )

  /** Transition to the replicating state if the replicated contracts count is greater than 0.
    */
  private def markAcsReplicating(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(requestId, "progress party replication") {
      for {
        state <- ensureParticipantStateAndSynchronizerConnected[
          PartyReplicationStatus.ConnectionEstablished
        ](requestId)
        (PartyReplicationStatus.ConnectionEstablished(previousParams), _, _) = state
      } yield {
        if (previousParams.replicatedContractsCount.unwrap > 0) {
          val newStatus = PartyReplicationStatus.ReplicatingAcs(previousParams)
          partyReplications.put(requestId, newStatus).discard
        }
      }
    }

  private def markComplete(requestId: AddPartyRequestId)(tc: TraceContext): Unit = {
    implicit val traceContext: TraceContext = tc
    executeAsync(requestId, "complete party replication") {
      for {
        status <- EitherT.fromEither[FutureUnlessShutdown](
          partyReplications
            .get(requestId)
            .toRight(s"Unknown party replication $requestId")
        )
        previousParams <- EitherT.fromEither[FutureUnlessShutdown](status match {
          case PartyReplicationStatus.ConnectionEstablished(params) =>
            Right(params)
          case PartyReplicationStatus.ReplicatingAcs(params) =>
            Right(params)
          case unexpectedStatus =>
            Left(
              s"Party replication $requestId status ${unexpectedStatus.code} not expected upon processor completion"
            )
        })
      } yield {
        val newStatus = PartyReplicationStatus.Completed(previousParams)
        partyReplications.put(requestId, newStatus).discard
      }
    }
  }

  private def recordError(requestId: AddPartyRequestId, tc: TraceContext)(error: String): Unit = {
    implicit val traceContext: TraceContext = tc
    logger.error(s"Party replication $requestId failed: $error")
    executeAsync(requestId, "error party replication") {
      EitherT.leftT[FutureUnlessShutdown, Unit](error)
    }
  }

  private def markDisconnected(
      requestId: AddPartyRequestId
  )(message: String, tc: TraceContext): Unit = {
    implicit val traceContext: TraceContext = tc
    executeAsync(requestId, "disconnect party replication") {
      for {
        status <- EitherT.fromEither[FutureUnlessShutdown](
          partyReplications
            .get(requestId)
            .toRight(s"Unknown party replication $requestId")
        )
        previousConnectedStatus <- EitherT.fromEither[FutureUnlessShutdown](status match {
          case ce @ PartyReplicationStatus.ConnectionEstablished(_) =>
            Right(ce)
          case ra @ PartyReplicationStatus.ReplicatingAcs(_) =>
            Right(ra)
          case unexpectedStatus =>
            Left(
              s"Party replication $requestId status ${unexpectedStatus.code} not expected upon channel disconnect"
            ): Either[String, PartyReplicationStatus.ConnectedPartyReplicationStatus]
        })
      } yield {
        val newStatus = PartyReplicationStatus.Disconnected(message, previousConnectedStatus)
        partyReplications.put(requestId, newStatus).discard
      }
    }
  }

  /** Asynchronously execute the provided code block and handle the result with a custom handler.
    * The custom "handleResult" handler allows deviating from the default error handling such as
    * when the SP rejects a TP-proposed party replication.
    */
  private def executeAsyncWithCustomResultHandling[A, I](
      requestId: I,
      operation: String,
  )(code: => EitherT[FutureUnlessShutdown, String, A])(
      handleResult: I => EitherT[
        FutureUnlessShutdown,
        String,
        A,
      ] => FutureUnlessShutdown[Unit]
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(s"About to $operation")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      handleResult(requestId)(executionQueue.executeEUS[String, A](code, operation)),
      s"$operation failed",
    )
  }

  /** Asynchronously execute the provided code block reflecting any returned "left" in the error
    * status.
    */
  private def executeAsync(requestId: AddPartyRequestId, operation: String)(
      code: => EitherT[FutureUnlessShutdown, String, Unit]
  )(implicit traceContext: TraceContext): Unit = {
    def recordIfError(requestId: AddPartyRequestId)(
        resultET: EitherT[FutureUnlessShutdown, String, Unit]
    ): FutureUnlessShutdown[Unit] = resultET.leftMap { err =>
      val previousStatusO = partyReplications.get(requestId)
      logger.warn(previousStatusO.fold(s"Unknown request id $requestId: $err")(_ => err))
      previousStatusO.foreach(prevState =>
        partyReplications
          .put(
            requestId,
            PartyReplicationStatus.Error(err, prevState),
          )
          .discard
      )
    }.merge

    executeAsyncWithCustomResultHandling(requestId, s"$operation $requestId")(code)(recordIfError)
  }

  /** Activates progress scheduling once a new party replication request is received unless already
    * active.
    */
  private def activateProgressMonitoring(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit = {
    val previouslyActive = progressSchedulingActive.getAndSet(true)
    if (previouslyActive) {
      logger.info(s"Progress scheduling already active, so no need to activate for $requestId.")
    } else {
      logger.info(s"Activating progress scheduling for party replication $requestId.")
      scheduleExecuteAsync(progressSchedulingInterval)(progressPartyReplication())
    }
  }

  /** Single point of entry for progress monitoring and advancing of party replication states for
    * those states that are driven by the party replicator.
    */
  private def progressPartyReplication()(implicit traceContext: TraceContext): Unit = {
    val activePartyReplications = partyReplications.iterator.collect {
      case (requestId, status: PartyReplicationStatus.ProgressIsExpected) =>
        requestId -> status
    }

    if (activePartyReplications.isEmpty) {
      logger.info("No party replication progress to monitor, deactivating progress scheduling.")
      progressSchedulingActive.set(false)
    } else {
      // Check if any OnPR work is currently running and back off if it is to avoid eagerly queuing
      // obsolete state transitions.
      if (!executionQueue.isEmpty) {
        logger.debug(
          s"Skipping progress scheduling because still busy with ${executionQueue.queued.mkString(", ")}."
        )
      } else {
        activePartyReplications.foreach {
          case (requestId, PartyReplicationStatus.ProposalProcessed(params)) =>
            logger.debug(
              s"Party replication $requestId proposal processed for ${params.partyId}. Progress driven by admin workflow."
            )
          case (requestId, PartyReplicationStatus.AgreementAccepted(_, _)) =>
            authorizeOnboardingTopology(requestId)
          case (requestId, PartyReplicationStatus.TopologyAuthorized(_)) =>
            connectToSequencerChannel(requestId)
          case (requestId, PartyReplicationStatus.ConnectionEstablished(params)) =>
            logger.debug(
              s"Party replication $requestId connection established for ${params.partyId}. Progress driven by processor."
            )
            markAcsReplicating(requestId)
            // check that processor has not gotten stuck
            params.partyReplicationProcessor.progressPartyReplication()
          case (requestId, PartyReplicationStatus.ReplicatingAcs(params)) =>
            logger.debug(
              s"Party replication $requestId has replicated ${params.replicatedContractsCount} contracts for ${params.partyId}. Progress driven by processor."
            )
            // check that processor has not gotten stuck
            params.partyReplicationProcessor.progressPartyReplication()
          case (requestId, PartyReplicationStatus.Disconnected(message, _)) =>
            logger.info(
              s"Party replication $requestId attempting to reconnect after: $message"
            )
            attemptToReconnectToSequencerChannel(requestId)
        }
      }
      scheduleExecuteAsync(progressSchedulingInterval)(progressPartyReplication())
    }
  }

  /** Asynchronous, scheduled execution relies on the clock's scheduled executor for scheduling, but
    * relies on the simple execution queue for execution to prevent blocking the participant clock
    * scheduler for too long. This avoids introducing another scheduler along with a mostly unused
    * thread whenever the participant does not replicate a party.
    */
  private def scheduleExecuteAsync(
      delta: PositiveFiniteDuration
  )(code: => Unit)(implicit traceContext: TraceContext): Unit = {
    logger.debug(s"Scheduling next check in $delta")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      clock.scheduleAfter(_ => code, delta.asJava),
      "party replicator progress scheduling",
    )
  }

  private def ensureParticipantStateAndSynchronizerConnected[
      PRS <: PartyReplicationStatus: ClassTag
  ](
      requestId: AddPartyRequestId
  ): EitherT[
    FutureUnlessShutdown,
    String,
    (PRS, ConnectedSynchronizer, SequencerChannelClient),
  ] = for {
    _ <- EitherT.cond[FutureUnlessShutdown](
      syncService.isActive(),
      (),
      s"Participant $participantId is inactive. Not processing party replication $requestId",
    )
    status <- EitherT.fromEither[FutureUnlessShutdown](
      partyReplications
        .get(requestId)
        .toRight(s"Unknown party replication $requestId")
    )
    expectedStatus <- EitherT.fromEither[FutureUnlessShutdown](
      status
        .select[PRS]
        .toRight(s"Party replication $requestId status $status not expected")
    )
    connectedSynchronizer <-
      EitherT.fromEither[FutureUnlessShutdown](
        syncService
          .readyConnectedSynchronizerById(status.params.synchronizerId)
          .toRight(
            s"Synchronizer ${status.params.synchronizerId} not connected, but needed for $requestId"
          )
      )
    channelClient <- EitherT.fromEither[FutureUnlessShutdown](
      connectedSynchronizer.synchronizerHandle.sequencerChannelClientO.toRight(
        s"Synchronizer ${status.params.synchronizerId} does not expose sequencer channel client needed for $requestId"
      )
    )
  } yield (expectedStatus, connectedSynchronizer, channelClient)

  private[party] def retryUntilLocalStoreUpdatedInExpectedState(
      operation: String
  )(
      checkLocalStoreState: String => FutureUnlessShutdown[Either[String, Unit]]
  )(implicit traceContext: TraceContext) =
    EitherT(
      retry
        .Backoff(
          logger,
          this,
          maxRetries = timeouts.unbounded.retries(1.second),
          initialDelay = 1.second,
          maxDelay = 10.seconds,
          operationName = operation,
        )
        .unlessShutdown(
          checkLocalStoreState(operation),
          DbExceptionRetryPolicy,
        )
    )

  override protected def onClosed(): Unit = {

    def getProcessors: Seq[AutoCloseable] =
      partyReplications.iterator.toSeq
        .collect[AutoCloseable] {
          case (_, PartyReplicationStatus.ConnectionEstablished(params)) =>
            params.partyReplicationProcessor
          case (_, PartyReplicationStatus.ReplicatingAcs(params)) =>
            params.partyReplicationProcessor
          case (_, PartyReplicationStatus.Completed(params)) =>
            params.partyReplicationProcessor
        }

    // Close the execution queue first to prevent activity and races wrt partyReplications.
    LifeCycle.close((executionQueue +: topologyWorkflow +: getProcessors)*)(logger)
  }
}

object PartyReplicator {
  lazy val defaultPingParallelism: PositiveInt = PositiveInt.tryCreate(4)
  lazy val defaultProgressSchedulingInterval: PositiveFiniteDuration =
    PositiveFiniteDuration.ofSeconds(1L)
}
