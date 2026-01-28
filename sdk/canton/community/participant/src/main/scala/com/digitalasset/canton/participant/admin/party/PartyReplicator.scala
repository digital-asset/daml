// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.crypto.{CryptoPureApi, Hash, HashPurpose}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.*
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.config.UnsafeOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationFileImporter,
  PartyReplicationProcessor,
  PartyReplicationSourceParticipantProcessor,
  PartyReplicationTargetParticipantProcessor,
}
import com.digitalasset.canton.participant.store.PartyReplicationStateManager
import com.digitalasset.canton.participant.sync.{CantonSyncService, ConnectedSynchronizer}
import com.digitalasset.canton.platform.store.backend.EventStorageBackend.SequentialIdBatch
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.{DbExceptionRetryPolicy, Storage}
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelClient
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{TimeQuery, TopologyStore}
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{
  EitherTUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  SimpleExecutionQueue,
  retry,
}
import org.apache.pekko.actor.ActorSystem

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
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
  *
  * @param markOnPRAgreementDone
  *   callback to archive the party replication agreement Daml contract and returning whether the
  *   contract is archived.
  */
final class PartyReplicator(
    participantId: ParticipantId,
    syncService: CantonSyncService,
    clock: Clock,
    config: UnsafeOnlinePartyReplicationConfig,
    markOnPRAgreementDone: (
        PartyReplicationAgreementParams,
        LfContractId,
        TraceContext,
    ) => FutureUnlessShutdown[Boolean],
    storage: Storage,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    parallelism: PositiveInt = PartyReplicator.defaultParallelism,
    progressSchedulingInterval: PositiveFiniteDuration =
      PartyReplicator.defaultProgressSchedulingInterval,
)(implicit
    executionContext: ExecutionContext,
    actorSystem: ActorSystem,
) extends FlagCloseable
    with NamedLogging {

  // Party replications state must be modified only within the simple executionQueue.
  // When read outside executeAsync*, readers must be aware that the map concurrently
  // changes and read state may be immediately stale.
  private val partyReplicationStateManager =
    new PartyReplicationStateManager(
      participantId,
      storage,
      futureSupervisor,
      exitOnFatalFailures,
      loggerFactory,
      timeouts,
    )

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
          requestId = buildRequestIdHash(args, syncPersistentState.pureCryptoApi)
          _ <- EitherT.fromEither[FutureUnlessShutdown](ensureCanAddParty())
          _ <- adminWorkflow.proposePartyReplication(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            sequencerCandidates,
            serial,
            participantPermission,
          )
          newStatus = PartyReplicationStatus(
            PartyReplicationStatus.ReplicationParams(
              requestId,
              partyId,
              synchronizerId,
              sourceParticipantId,
              participantId,
              serial,
              participantPermission,
            ),
            syncPersistentState.staticSynchronizerParameters.protocolVersion,
          )
          _ <- partyReplicationStateManager.add(newStatus)
        } yield {
          logger.info(s"Party replication $requestId proposal processed")
          requestId
        }
      },
      s"add party ${args.partyId} on ${args.synchronizerId}",
    )

  private def buildRequestIdHash(args: PartyReplicationArguments, pureCryptoApi: CryptoPureApi) =
    pureCryptoApi
      .build(HashPurpose.OnlinePartyReplicationId)
      .addString(args.partyId.toProtoPrimitive)
      .addString(args.synchronizerId.toProtoPrimitive)
      .addString(args.sourceParticipantId.toProtoPrimitive)
      .addInt(args.serial.unwrap)
      .finish()

  private def ensureCanAddParty(): Either[String, Unit] = for {
    _ <- partyReplicationStateManager
      .collectFirst {
        case (
              id,
              status @ PartyReplicationStatus(
                _,
                _,
                _,
                _,
                _,
                _,
                Some(PartyReplicationStatus.PartyReplicationFailed(errorMsg)),
              ),
            ) =>
          (id, status, errorMsg)
      }
      .fold(Right(()): Either[String, Unit]) { case (id, previousStatus, errorMsg) =>
        Left(
          s"Participant $participantId has encountered previous error \"$errorMsg\" during add_party_async" +
            s" request $id after status $previousStatus and needs to be repaired"
        )
      }
    _ <- partyReplicationStateManager
      .collectFirst { case (id, status) if !status.hasCompleted => id -> status }
      .fold(Right(()): Either[String, Unit]) { case (id, status) =>
        Left(
          s"Only a single party replication can be in progress: $status, but found party replication $id" +
            s" of party ${status.params.partyId} on synchronizer ${status.params.synchronizerId}" +
            s" with status $status"
        )
      }
  } yield ()

  private[admin] def getAddPartyStatus(
      addPartyRequestId: AddPartyRequestId
  ): Option[PartyReplicationStatus] = partyReplicationStateManager.get(addPartyRequestId)

  /** Adds a party to the local target participant using the ACS snapshot provided by a file via an
    * ACS iterator by importing the ACS synchronously, i.e. when the returned EitherT succeeds, but
    * only fully completing party replication asynchronously (e.g. clearing the onboarding flag).
    *
    * @param args
    *   arguments shared with the [[addPartyAsync]] method
    * @param acsReader
    *   iterator over ACS contracts
    * @return
    *   a request id that case be used to query for progress or errors via [[getAddPartyStatus]]
    */
  private[admin] def addPartyWithAcsAsync(
      args: PartyReplicationArguments,
      acsReader: Iterator[ActiveContract],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, AddPartyRequestId] =
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
              s"Initiating replication of party $partyId with ACS from participant $sourceParticipantId on synchronizer $synchronizerId"
            ),
            s"Participant $participantId is inactive",
          )
          connectedSynchronizer <-
            EitherT.fromEither[FutureUnlessShutdown](
              syncService
                .readyConnectedSynchronizerById(synchronizerId)
                .toRight(s"Unknown synchronizer $synchronizerId")
            )
          syncPersistentState = connectedSynchronizer.synchronizerHandle.syncPersistentState
          onboardingAt <- ensurePartyHostedBySourceAndOnboardingOnTargetParticipant(
            args,
            syncPersistentState.topologyStore,
          )
          requestId = buildRequestIdHash(args, syncPersistentState.pureCryptoApi)
          fileImporter = PartyReplicationFileImporter(
            partyId,
            requestId,
            onboardingAt,
            partyReplicationStateManager,
            syncService.participantNodePersistentState,
            connectedSynchronizer,
            acsReader,
            testInterceptorO,
            loggerFactory,
          )
          initialStatus = PartyReplicationStatus(
            PartyReplicationStatus.ReplicationParams(
              requestId,
              partyId,
              synchronizerId,
              sourceParticipantId,
              participantId,
              serial,
              participantPermission,
            ),
            syncPersistentState.staticSynchronizerParameters.protocolVersion,
            authorizationO =
              Some(PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared = false)),
            replicationO = Some(AcsReplicationProgress.initialize(fileImporter)),
          )
          _ <- partyReplicationStateManager.add(initialStatus)
          _ <- fileImporter.importEntireAcsSnapshotInOneGo()
        } yield {
          logger.info(s"Adding party $partyId with ACS request $requestId is in progress")
          activateProgressMonitoring(requestId)
          requestId
        }
      },
      s"add party ${args.partyId} on ${args.synchronizerId}",
    )

  /** Checks that the party is
    *   - hosted by the source participant
    *   - hosted by the target participant with onboarding flag set
    *   - serial matches head authorized topology
    */
  private def ensurePartyHostedBySourceAndOnboardingOnTargetParticipant(
      args: PartyReplicationArguments,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, EffectiveTime] = {
    val PartyReplicationArguments(
      partyId,
      _,
      sourceParticipantId,
      serial,
      participantPermission,
    ) = args
    val targetParticipantId = participantId

    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        sourceParticipantId != targetParticipantId,
        (),
        s"Source and target participants cannot match",
      )
      partyToParticipantTopologyHeadTx <- topologyWorkflow.partyToParticipantTopologyHead(
        partyId,
        topologyStore,
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        serial == partyToParticipantTopologyHeadTx.serial,
        (),
        s"Specified party $partyId serial $serial does not match the encountered head serial ${partyToParticipantTopologyHeadTx.serial}.",
      )
      activeParticipantsOfParty = partyToParticipantTopologyHeadTx.mapping.participants
      eligibleSourceParticipants = activeParticipantsOfParty.collect {
        case HostingParticipant(pid, _, isOnboarding)
            if pid != targetParticipantId && !isOnboarding =>
          pid
      }
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        Either.cond(
          eligibleSourceParticipants.contains(sourceParticipantId),
          (),
          s"Party $partyId is not hosted by source participant $sourceParticipantId. Only hosted on ${activeParticipantsOfParty
              .mkString(",")}",
        )
      )
      onboarding = true
      _ <- EitherT.cond[FutureUnlessShutdown](
        activeParticipantsOfParty.contains(
          HostingParticipant(targetParticipantId, participantPermission, onboarding)
        ),
        (),
        // TODO(#30328): Add resilience to the operator forgetting to set onboarding on the TP.
        s"Party $partyId is not marked as onboarding with permission $participantPermission on the target participant $targetParticipantId. Hosted on ${activeParticipantsOfParty
            .mkString(",")}",
      )
    } yield partyToParticipantTopologyHeadTx.validFrom
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
        _ <- EitherT.fromEither[FutureUnlessShutdown](ensureCanAddParty())
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
      } yield (
        PartyReplicationAgreementParams.fromProposal(proposal, participantId, sequencerId),
        connectedSynchronizer.staticSynchronizerParameters.protocolVersion,
      )
    } { _ => resET =>
      for {
        agreementResponseE <- resET.value
        // Respond to the proposal depending on the outcome of the agreement response.
        _ = respondToProposal(agreementResponseE.map { case (agreement, _pv) => agreement })
        _ <- agreementResponseE.fold(
          _ => FutureUnlessShutdown.unit,
          { case (response, protocolVersion) =>
            // Upon success indicate that the SP has processed the proposal.
            val newStatus = PartyReplicationStatus(
              PartyReplicationStatus.ReplicationParams(
                response.requestId,
                response.partyId,
                response.synchronizerId,
                response.sourceParticipantId,
                response.targetParticipantId,
                response.serial,
                response.participantPermission,
              ),
              protocolVersion,
            )
            partyReplicationStateManager
              .add(newStatus)
              .bimap(
                err =>
                  logger.warn(
                    s"Unexpectedly unable to add already-tracked party replication ${response.requestId}. Dropping $newStatus: $err"
                  ),
                _ => {
                  logger.info(
                    s"Party replication ${response.requestId} proposal processed at source participant"
                  )
                  activateProgressMonitoring(response.requestId)
                },
              )
              .merge
          },
        )
      } yield ()
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
          .parTraverseWithLimit(parallelism)(sequencerIds)(sequencerId =>
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
      damlAgreementCid: LfContractId,
      mightNotRememberProposal: Boolean,
  )(
      agreementParams: PartyReplicationAgreementParams
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(agreementParams.requestId, "process agreement of party replication") {
      val requestId = agreementParams.requestId
      val paramsReceived =
        PartyReplicationStatus.ReplicationParams.fromAgreementParams(agreementParams)
      val agreement =
        SequencerChannelAgreement(damlAgreementCid, agreementParams.sequencerId)

      // If the party replication is legitimately not yet known (after a source participant node restart),
      // set the AgreementAccepted status.
      def processUntrackedAgreement() = {
        // TODO(#20636): Remove this eager action on the part of the SP, and have the TP
        //  renegotiate OnPR via a new proposal.
        logger.info(
          s"Backfilling party replication $requestId agreement not known due to a suspected participant node restart"
        )
        val synchronizerId = agreementParams.synchronizerId
        (for {
          latestSynchronizerConnectionConfig <- EitherT.fromEither[FutureUnlessShutdown](
            syncService.synchronizerConnectionConfigStore
              .getActive(synchronizerId)
              .leftMap(_.message)
          )
          psid <- EitherT.fromEither[FutureUnlessShutdown](
            latestSynchronizerConnectionConfig.configuredPSId.toOption
              .toRight(
                s"Latest synchronizer $synchronizerId config $latestSynchronizerConnectionConfig has no physical synchronizer id set"
              )
          )
          agreementReceived = PartyReplicationStatus(
            paramsReceived,
            psid.protocolVersion,
            agreementO = Some(agreement),
          )
          _ <- partyReplicationStateManager.add(agreementReceived)
        } yield activateProgressMonitoring(requestId)).leftMap(err =>
          s"Unable to process party replication $requestId agreement: $err"
        )
      }

      def processExpectedAgreement(statusO: Option[PartyReplicationStatus]) =
        for {
          status <- EitherT.fromEither[FutureUnlessShutdown](
            statusO.toRight(s"Unknown request id $requestId")
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            status.ensureCanSetAgreement(paramsReceived)
          )
          _ <- partyReplicationStateManager.update_(requestId, _.setAgreementO(Some(agreement)))
        } yield {
          logger.info(
            s"Party replication $requestId agreement $agreement accepted for party ${agreementParams.partyId}"
          )
          activateProgressMonitoring(requestId)
        }

      val statusO = partyReplicationStateManager.get(requestId)
      if (mightNotRememberProposal && statusO.isEmpty)
        processUntrackedAgreement()
      else processExpectedAgreement(statusO)
    }

  private def authorizeOnboardingTopology(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnected(requestId) {
      case (
            PartyReplicationStatus(params, _, None, _, _, _, _),
            connectedSynchronizer,
            _,
          ) =>
        for {
          authorizedAtO <- topologyWorkflow.authorizeOnboardingTopology(
            params,
            connectedSynchronizer,
          )
          // To be sure the authorization has become effective, wait until the topology change is visible via the ledger api
          _ <- authorizedAtO match {
            case Some(EffectiveTime(authorizedAt)) =>
              val operation = s"observe ${params.partyId} topology transaction via ledger api"
              val retryCounter = new AtomicInteger(0)
              retryUntilLocalStoreUpdatedInExpectedState(operation)(
                synchronizeWithClosingF(_) {
                  for {
                    offsetO <- syncService.participantNodePersistentState.value.ledgerApiStore
                      .topologyEventOffsetPublishedOnRecordTime(
                        params.synchronizerId,
                        authorizedAt,
                      )
                    _ <- {
                      // Hopefully temporary logging aid to debug a rare OnlinePartyReplicationRecoverFromDisruptionsTest
                      // flake in which a PartyToParticipant addition has been authorized by topology, but the
                      // corresponding event does not appear in the ledger api store.
                      val currentCounter = retryCounter.get()
                      // Only begin additional debug logging once sufficiently many retries have not helped.
                      if (currentCounter <= 10 || !logger.underlying.isDebugEnabled()) Future.unit
                      else {
                        syncService.participantNodePersistentState.value.ledgerApiStore
                          .topologyPartyEventBatch(SequentialIdBatch.IdRange(0L, 1000000L))
                          .map { partyAuthorizations =>
                            logger.debug(
                              s"Party events on $participantId (querying at $authorizedAt retry $currentCounter, offset $offsetO):\n${partyAuthorizations
                                  .mkString("\n")}"
                            )
                          }
                      }
                    }
                  } yield {
                    retryCounter.incrementAndGet().discard
                    Either.cond(offsetO.nonEmpty, (), s"failed to $operation")
                  }
                }
              )
            case None => EitherT.rightT[FutureUnlessShutdown, String](())
          }
          _ <- authorizedAtO.fold {
            logger.debug(
              s"Onboarding topology for party replication $requestId and party ${params.partyId} not yet authorized."
            )
            EitherTUtil.unitUS[String]
          } { authorizedAt =>
            logger.info(
              s"Party replication $requestId onboarding topology of party ${params.partyId} authorized with serial ${params.serial} and effective time $authorizedAt"
            )
            partyReplicationStateManager.update_(
              requestId,
              _.setAuthorization(
                PartyReplicationAuthorization(authorizedAt, isOnboardingFlagCleared = false)
              ),
            )
          }
        } yield ()
    }

  private def partiesHostedByParticipant(
      participantId: ParticipantId,
      except: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
      asOfExclusive: EffectiveTime,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Set[PartyId]] =
    // TODO(#25766): add topology client endpoint
    EitherT(
      topologyStore
        .inspect(
          proposals = false,
          timeQuery = TimeQuery.Snapshot(asOfExclusive.value),
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
              .map(_.mapping.partyId)
              .toSet
          ): Either[String, Set[PartyId]]
        )
    )

  private def connectToSequencerChannel(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val ownsSessionKey = true
    val noSessionKey = false
    ensureParticipantStateAndSynchronizerConnected(requestId) {
      case (
            PartyReplicationStatus(
              params,
              Some(SequencerChannelAgreement(_, sequencerId)),
              Some(PartyReplicationAuthorization(onboardingAt, _)),
              _,
              _,
              _,
              _,
            ),
            connectedSynchronizer,
            channelClient,
          ) =>
        for {
          processorInfo <-
            if (participantId == params.sourceParticipantId) {
              for {
                partiesAlreadyHostedByTargetParticipant <- partiesHostedByParticipant(
                  params.targetParticipantId,
                  params.partyId,
                  connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore,
                  onboardingAt,
                )
                effectiveAtLapiOffset <- EitherT(
                  synchronizeWithClosingF(s"Locate PTP lapi offset for $requestId")(
                    syncService.participantNodePersistentState.value.ledgerApiStore
                      .topologyEventOffsetPublishedOnRecordTime(
                        params.synchronizerId,
                        onboardingAt.value,
                      )
                      .map(
                        _.toRight(
                          s"Cannot locate PTP ${params.partyId} offset at $onboardingAt for $requestId"
                        )
                      )
                  )
                )
                indexService <- EitherT.fromEither[FutureUnlessShutdown](
                  syncService.internalIndexService.toRight(
                    "Internal index service not available for source participant processor due to shutdown or becoming active?"
                  )
                )
              } yield {
                (
                  PartyReplicationSourceParticipantProcessor(
                    connectedSynchronizer.psid,
                    params.partyId,
                    requestId,
                    effectiveAtLapiOffset,
                    partiesAlreadyHostedByTargetParticipant,
                    indexService,
                    partyReplicationStateManager,
                    recordError(requestId, traceContext),
                    markDisconnected(requestId),
                    futureSupervisor,
                    exitOnFatalFailures,
                    timeouts,
                    loggerFactory,
                    testInterceptorO.getOrElse(PartyReplicationTestInterceptor.AlwaysProceed),
                  ): PartyReplicationProcessor,
                  params.targetParticipantId,
                  noSessionKey,
                )
              }
            } else if (participantId == params.targetParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                (
                  PartyReplicationTargetParticipantProcessor(
                    params.partyId,
                    requestId,
                    onboardingAt,
                    partyReplicationStateManager,
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
                  params.sourceParticipantId,
                  ownsSessionKey,
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
          // Set replication state before channel connect so that processor always finds "progress" state.
          _ <- partyReplicationStateManager.update_(requestId, _.setProcessor(processor))
          _ <- channelClient
            .connectToSequencerChannel(
              sequencerId,
              SequencerChannelId(requestId.toHexString),
              counterParticipantId,
              processor,
              isSessionKeyOwner,
              onboardingAt.value,
            )
            .mapK(FutureUnlessShutdown.liftK)
        } yield {
          logger.info(s"Party replication $requestId connected to sequencer $sequencerId")
        }
    }
  }

  private def attemptToReconnectToSequencerChannel(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnected(requestId) {
      case (
            PartyReplicationStatus(
              params,
              Some(SequencerChannelAgreement(_, sequencerId)),
              Some(PartyReplicationAuthorization(effectiveAt, _)),
              Some(EphemeralSequencerChannelProgress(_, _, _, processor)),
              _,
              _,
              Some(Disconnected(_)),
            ),
            _,
            channelClient,
          ) =>
        for {
          processorInfo <-
            if (participantId == params.sourceParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                (processor, params.targetParticipantId, /* isSessionKeyOwner = */ false)
              )
            } else if (participantId == params.targetParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                (processor, params.sourceParticipantId, /* isSessionKeyOwner = */ true)
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
                      effectiveAt.value,
                    )
                    .mapK(FutureUnlessShutdown.liftK)
                )
                .value
                .map(_.swap.toOption)
            )
          _ <- cannotReconnectMessageO.fold {
            logger.info(s"Party replication $requestId reconnected to sequencer $sequencerId")
            partyReplicationStateManager.update_(requestId, _.modifyErrorO(_ => None))
          } { cannotReconnectMsg =>
            logger.info(
              s"Party replication $requestId not yet able to reconnect to sequencer $sequencerId: $cannotReconnectMsg"
            )
            EitherTUtil.unitUS
          }
        } yield ()
    }

  /** This completes party replication by executing the following final steps if they are found to
    * not have been executed yet:
    *
    *   - Clear the onboarding flag from the target participant in the PartyToParticipant topology.
    *   - Archive the party replication agreement Daml contract to inform the SP that the TP no
    *     longer needs help replicating the party's ACS.
    */
  private def finishPartyReplication(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    ensureParticipantStateAndSynchronizerConnected(requestId) {
      case (
            previous @ PartyReplicationStatus(
              params,
              agreementO,
              Some(PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared)),
              _,
              _,
              false, // not completed
              None,
            ),
            connectedSynchronizer,
            _,
          ) =>
        for {
          isAgreementArchived <- EitherT.right[String](
            agreementO match {
              case Some(SequencerChannelAgreement(damlAgreementCid, sequencerId)) =>
                markOnPRAgreementDone(
                  PartyReplicationAgreementParams.fromAgreedReplicationStatus(params, sequencerId),
                  damlAgreementCid,
                  traceContext,
                )
              case None => FutureUnlessShutdown.pure(false)
            }
          )
          isOnboardingFlagVerifiedCleared <-
            if (!isOnboardingFlagCleared)
              topologyWorkflow.authorizeClearingOnboardingFlag(
                params,
                onboardingAt,
                connectedSynchronizer,
              )
            else EitherT.rightT[FutureUnlessShutdown, String](false)

          statusUpdates = {
            def statusUpdate(
                condition: Boolean,
                update: PartyReplicationStateManager.Modification,
            ): Seq[PartyReplicationStateManager.Modification] =
              if (condition) Seq(update) else Seq.empty

            statusUpdate(isAgreementArchived, _.setAgreementO(None))
              ++ statusUpdate(
                isOnboardingFlagVerifiedCleared,
                _.setAuthorization(
                  PartyReplicationAuthorization(onboardingAt, isOnboardingFlagCleared = true)
                ),
              )
              ++ statusUpdate(
                (isAgreementArchived || agreementO.isEmpty) && (isOnboardingFlagVerifiedCleared || isOnboardingFlagCleared),
                _.setCompleted(),
              )
          }

          status <-
            if (statusUpdates.nonEmpty) {
              // Compose potentially multiple modifications into a single modification.
              val combinedModification = statusUpdates
                .foldLeft[PartyReplicationStateManager.Modification](
                  // Seed the modification chain with a dummy modification that returns the previous status.
                  identity
                ) { case (composedModifications, nextModification) =>
                  prev => nextModification(composedModifications(prev))
                }
              partyReplicationStateManager.update(requestId, combinedModification)
            } else EitherT.rightT[FutureUnlessShutdown, String](previous)
        } yield {
          if (status.hasCompleted) {
            logger.info(s"Party replication $requestId has completed")
          } else {
            logger.debug(
              s"Party replication $requestId not yet completed. AgreementArchived $isAgreementArchived, PartyOnboarded $isOnboardingFlagVerifiedCleared."
            )
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
          partyReplicationStateManager
            .get(requestId)
            .toRight(s"Unknown party replication $requestId")
        )
        _ <- status match {
          case PartyReplicationStatus(_, _, _, Some(_), _, _, None) =>
            partyReplicationStateManager.update_(
              requestId,
              _.modifyErrorO(_ => Some(Disconnected(message))),
            )
          case PartyReplicationStatus(_, _, _, Some(_), _, _, Some(_)) =>
            EitherTUtil.unitUS[String]
          case unexpectedStatus =>
            EitherT.leftT[FutureUnlessShutdown, Unit](
              s"Party replication $requestId status $unexpectedStatus not expected upon channel disconnect"
            )
        }
      } yield ()
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
    ): FutureUnlessShutdown[Unit] = resultET.leftSemiflatMap { err =>
      partyReplicationStateManager
        .update(
          requestId,
          _.modifyErrorO { prevErrorO =>
            prevErrorO.foreach(prevError =>
              logger.warn(
                s"Party replication $requestId has unexpectedly encountered error after previous error $prevError. Ignoring new error: $err"
              )
            )
            Some(PartyReplicationFailed(err))
          },
        )
        .fold(updateErr => logger.warn(s"$updateErr: $err"), _ => logger.warn(err))
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
      scheduleExecuteAsync(progressSchedulingInterval)(progressPartyReplications())
    }
  }

  /** Single point of entry for progress monitoring and advancing of party replication states for
    * those states that are driven by the party replicator.
    */
  private def progressPartyReplications()(implicit traceContext: TraceContext): Unit = {
    val activePartyReplications = partyReplicationStateManager.collect {
      case (requestId, status) if status.isProgressExpected => requestId
    }

    if (activePartyReplications.isEmpty) {
      logger.info("No party replication progress to monitor, deactivating progress scheduling.")
      progressSchedulingActive.set(false)
    }
    // Check if any OnPR work is currently running and back off if it is to avoid eagerly queuing
    // obsolete state transitions.
    else {
      if (!executionQueue.isEmpty) {
        logger.debug(
          s"Skipping advancing party replication progress because still busy with ${executionQueue.queued
              .mkString(", ")}."
        )
      } else {
        // In case the set of requestIds has changed (particularly if grown) since "activePartyReplications"
        // has been read above, we will pick it up on the next invocation scheduled below.
        activePartyReplications.foreach(progressPartyReplication)
      }
      // Schedule the next time to progress-check party replications asynchronously, i.e. not recursively.
      scheduleExecuteAsync(progressSchedulingInterval)(progressPartyReplications())
    }
  }

  private def progressPartyReplication(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(requestId, s"progress party replication $requestId")(
      partyReplicationStateManager
        .get(requestId)
        .flatMap { case s @ PartyReplicationStatus(p, agO, auO, reO, inO, _, errO) =>
          errO match {
            case None => Option.when(s.isProgressExpected)((p, agO, auO, reO, inO, None))
            case Some(d: Disconnected) =>
              Option.when(s.isProgressExpected)((p, agO, auO, reO, inO, Some(d)))
            case Some(PartyReplicationFailed(_)) => None
          }
        }
        .fold(EitherTUtil.unitUS[String]) {
          case (params, None, None, None, _, None) =>
            logger.debug(
              s"Party replication $requestId proposal processed for ${params.partyId}. Progress driven by admin workflow."
            )
            EitherTUtil.unitUS

          case (_, Some(_agreement), None, None, _, None) =>
            logger.debug(s"Authorizing party replication $requestId topology")
            authorizeOnboardingTopology(requestId)

          case (_, agreementO, Some(_authorization), None, _, None) =>
            EitherTUtil.ifThenET(agreementO.nonEmpty) {
              logger.debug(s"Connecting to sequencer channel for party replication $requestId")
              connectToSequencerChannel(requestId)
            }

          case (p, _, _, Some(progress), _, None) =>
            if (progress.fullyProcessedAcs) {
              logger.debug(
                s"Party replication $requestId has finished replicating all ${progress.processedContractCount} contracts for ${p.partyId}."
              )
              finishPartyReplication(requestId)(traceContext)
            } else {
              progress match {
                case EphemeralSequencerChannelProgress(_, _, _, processor) =>
                  logger.debug(
                    s"Party replication $requestId has replicated ${progress.processedContractCount} contracts for ${p.partyId}. Progress driven by processor."
                  )
                  // ensure that the processor is making progress
                  EitherT.rightT[FutureUnlessShutdown, String](processor.progressPartyReplication())
                case _: EphemeralFileImporterProgress =>
                  logger.debug(
                    s"Party replication $requestId has replicated ${progress.processedContractCount} contracts for ${p.partyId}. Progress driven by file importer."
                  )
                  // The file importer is self-paced and does not need to be pinged to make progress unlike the sequencer processors
                  EitherTUtil.unitUS
                case replicationNonRuntime: PersistentProgress =>
                  // TODO(#29498): As part of TP-resilience to restart and crash recovery, rebuild target participant
                  //  processor once reconnected to synchronizer.
                  EitherT.leftT[FutureUnlessShutdown, Unit](
                    s"Party replication ${p.requestId} AcsReplicationProgress not in runtime state: $replicationNonRuntime"
                  )
              }
            }

          case (_, agreementO, _, _, _, Some(Disconnected(message))) =>
            EitherTUtil.ifThenET(agreementO.nonEmpty) {
              logger.info(s"Party replication $requestId attempting to reconnect after: $message")
              attemptToReconnectToSequencerChannel(requestId)
            }
        }
    )

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

  /** Ensures that a number of prerequisites are met for the party replication with the specified
    * request id. Particularly, verify that:
    *   1. the participant is HA-active as only the active replica should perform OnPR,
    *   1. the request id is known,
    *   1. the participant is connected to the synchronizer logging only if it isn't rather than
    *      failing OnPR,
    *   1. the current status of the party replication is as expected,
    *   1. if connected, the synchronizer exposes a sequencer channel client.
    *
    * If all prerequisites are met, invokes the provided `onSuccess` callback. Returns an error if
    * any prerequisite is not met other than synchronizer connectivity such that progress can resume
    * upon reconnect.
    */
  private def ensureParticipantStateAndSynchronizerConnected[T](requestId: AddPartyRequestId)(
      matchIfStateIsAsExpected: PartialFunction[
        (PartyReplicationStatus, ConnectedSynchronizer, SequencerChannelClient),
        EitherT[FutureUnlessShutdown, String, Unit],
      ]
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val stateET = for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        syncService.isActive(),
        (),
        AddPartyError.Other
          .Failure(requestId, s"Stopping as participant $participantId is inactive"),
      )
      status <- EitherT.fromEither[FutureUnlessShutdown](
        partyReplicationStateManager
          .get(requestId)
          .toRight(AddPartyError.Other.Failure(requestId, "Unknown request id"))
      )
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(status.params.synchronizerId)
            .toRight(
              AddPartyError.DisconnectedFromSynchronizer.Failure(
                requestId,
                status.params.synchronizerId,
                s"Synchronizer ${status.params.synchronizerId} not connected during $status",
              )
            )
        )
      channelClient <- EitherT.fromEither[FutureUnlessShutdown](
        connectedSynchronizer.synchronizerHandle.sequencerChannelClientO.toRight(
          AddPartyError.Other.Failure(
            requestId,
            s"Synchronizer ${status.params.synchronizerId} does not expose necessary sequencer channel client",
          )
        )
      )
      _ <- matchIfStateIsAsExpected
        .lift((status, connectedSynchronizer, channelClient))
        .getOrElse(EitherT.leftT[FutureUnlessShutdown, Unit](s"Unexpected status $status"))
        .leftMap(AddPartyError.Other.Failure(requestId, _): AddPartyError)
    } yield ()

    // Skip processing if the participant is not connected to the synchronizer instead of returning an error.
    stateET.leftFlatMap {
      case AddPartyError.DisconnectedFromSynchronizer.Failure(requestId, synchronizerId, err) =>
        logger.info(
          s"Party replication $requestId disconnected from synchronizer $synchronizerId. Waiting until reconnect: $err"
        )
        EitherTUtil.unitUS
      case err: AddPartyError.Other.Failure =>
        EitherT.leftT[FutureUnlessShutdown, Unit](err.cause)
    }
  }

  private[party] def retryUntilLocalStoreUpdatedInExpectedState[T](
      operation: String
  )(
      checkLocalStoreState: String => FutureUnlessShutdown[Either[String, T]]
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
      partyReplicationStateManager.collect[PartyReplicationProcessor] {
        case (
              _,
              PartyReplicationStatus(
                _,
                _,
                _,
                Some(repl: EphemeralSequencerChannelProgress),
                _,
                _,
                _,
              ),
            ) =>
          repl.processor
      }

    // Close the execution queue first to prevent activity and races wrt partyReplications.
    LifeCycle.close(
      (executionQueue +: topologyWorkflow +: getProcessors :+ partyReplicationStateManager)*
    )(logger)
  }
}

object PartyReplicator {
  type AddPartyRequestId = Hash

  lazy val defaultParallelism: PositiveInt = PositiveInt.tryCreate(4)
  lazy val defaultProgressSchedulingInterval: PositiveFiniteDuration =
    PositiveFiniteDuration.ofSeconds(1L)
}
