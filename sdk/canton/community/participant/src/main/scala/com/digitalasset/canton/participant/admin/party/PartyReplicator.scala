// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.crypto.{Hash, HashPurpose}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.PartyReplicationArguments
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus.{
  AuthorizedReplicationParams,
  ConnectionEstablished,
  PartyReplicationStatus,
}
import com.digitalasset.canton.participant.protocol.party.{
  PartyReplicationSourceParticipantProcessor,
  PartyReplicationTargetParticipantProcessor,
}
import com.digitalasset.canton.participant.sync.{CantonSyncService, ConnectedSynchronizer}
import com.digitalasset.canton.resource.DbExceptionRetryPolicy
import com.digitalasset.canton.sequencing.client.channel.{
  SequencerChannelClient,
  SequencerChannelProtocolProcessor,
}
import com.digitalasset.canton.sequencing.protocol.channel.SequencerChannelId
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
import com.digitalasset.canton.util.{
  EitherTUtil,
  FutureUnlessShutdownUtil,
  MonadUtil,
  SimpleExecutionQueue,
  retry,
}

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.util.chaining.scalaUtilChainingOps

/** The party replicator acts on behalf of the participant's online party replication requests:
  *   - In response to an operator request to initiate online party replication, triggers admin
  *     workflow proposal.
  *   - Exposes callbacks to the admin workflow to validate and process channel proposals and
  *     agreements.
  *
  * The party replicator conceptually owns the party replication admin workflow and implements the
  * grpc party management service endpoints related to online party replication, but for practical
  * reasons its lifetime is controlled by the admin workflow service. This helps ensure that upon
  * participant HA-activeness changes, the party replication-related classes are all created or
  * closed in unison.
  */
final class PartyReplicator(
    participantId: ParticipantId,
    syncService: CantonSyncService,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    pingParallelism: PositiveInt = PositiveInt.tryCreate(4),
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

  /** Validates online party replication arguments and propose party replication via the provided
    * admin workflow service.
    */
  private[admin] def addPartyAsync(
      args: PartyReplicationArguments,
      adminWorkflow: PartyReplicationAdminWorkflow,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Hash] =
    executionQueue.executeEUS(
      {
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
          sourceParticipantId <- ensurePartyHostedBySourceButNotTargetParticipant(
            partyId,
            sourceParticipantIdO,
            participantId,
            syncPersistentState.topologyStore,
            serialO,
          )
          requestId =
            syncPersistentState.pureCryptoApi
              .build(HashPurpose.OnlinePartyReplicationId)
              .add(partyId.toProtoPrimitive)
              .add(synchronizerId.toProtoPrimitive)
              .add(sourceParticipantId.toProtoPrimitive)
              .finish()
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            partyReplications.headOption.fold(Right(()): Either[String, Unit]) {
              case (id, status) =>
                Left(
                  s"Only a single party replication can be in progress: $status, but found party replication $id" +
                    s" of party ${status.params.partyId} on synchronizer ${status.params.synchronizerId}" +
                    s" with status ${status.code}"
                )
            }
          )
          _ <- adminWorkflow.proposePartyReplication(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            sequencerCandidates,
            serialO,
          )
        } yield {
          val newStatus = PartyReplicationStatus.ProposalProcessed(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            participantId,
            serialO,
          )
          logger.info(s"Party replication $requestId proposal processed")
          partyReplications.put(requestId, newStatus).discard
          requestId
        }
      },
      s"add party ${args.partyId} on ${args.synchronizerId}",
    )

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
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(
      proposalOrError.fold(
        err => s"reject party replication: $err",
        params => s"respond to party replication proposal ${params.requestId}",
      )
    ) {
      val responseET = for {
        proposal <- EitherT.fromEither[FutureUnlessShutdown](proposalOrError)
        PartyReplicationProposalParams(
          _,
          partyId,
          synchronizerId,
          targetParticipantId,
          sequencerIdsProposed,
          serialO,
        ) = proposal
        connectedSynchronizer <-
          EitherT.fromEither[FutureUnlessShutdown](
            syncService
              .readyConnectedSynchronizerById(synchronizerId)
              .toRight(s"Synchronizer $synchronizerId not connected")
          )
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
          Some(participantId),
          targetParticipantId,
          connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore,
          serialO,
        )
      } yield PartyReplicationAgreementParams.fromProposal(proposal, participantId, sequencerId)

      responseET.value.map { agreementResponseE =>
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
            response.serialO,
          )
          logger.info(
            s"Party replication ${response.requestId} proposal processed at source participant"
          )
          partyReplications.put(response.requestId, newStatus).discard
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
    *     serial if specified
    */
  private def ensurePartyHostedBySourceButNotTargetParticipant(
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

  /** Party replication agreement notification
    */
  private[admin] def processPartyReplicationAgreement(
      agreementParams: PartyReplicationAgreementParams
  )(implicit traceContext: TraceContext): Unit = {
    val requestId = agreementParams.requestId
    val newStatus = PartyReplicationStatus.AgreementAccepted(agreementParams)
    logger.info(
      s"Party replication $requestId agreement accepted for party ${agreementParams.partyId}"
    )
    partyReplications.put(requestId, newStatus).discard
    authorizeTopology(requestId)
  }

  private def authorizeTopology(requestId: AddPartyRequestId)(implicit
      traceContext: TraceContext
  ): Unit = executeAsync(
    s"authorize party replication topology for $requestId"
  )(
    recordIfError(
      requestId,
      for {
        state <- ensureParticipantStateAndSynchronizerConnected[
          PartyReplicationStatus.AgreementAccepted
        ](requestId)
        (status, connectedSynchronizer, _) = state
        PartyReplicationStatus.AgreementAccepted(
          params @ PartyReplicationStatus
            .ReplicationParams(
              _,
              partyId,
              synchronizerId,
              sourceParticipantId,
              targetParticipantId,
            ),
          sequencerId,
          serialO,
        ) = status
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
        // TODO(#25055): Make this work for decentralized parties in which case the source
        //  participant cannot sign on behalf of the replicating party. Instead this needs
        //  to be turned into a wait until all the party signers have authorized the topology
        //  transaction.
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
        // Retrieve PartyToParticipant topology along with effective time, double check that our serial is the
        // latest head state and in case no serial was specified that the SP and TP are now indeed authorized to
        // host the party.
        partyToParticipantTopologyPartyAdded <- partyToParticipantTopologyHead(
          partyId,
          syncPersistentState.topologyStore,
        )
        _ <- EitherT.cond[FutureUnlessShutdown](
          serialO.forall(_ == partyToParticipantTopologyPartyAdded.serial),
          (),
          s"Specified serial $serialO does not match the actual serial ${partyToParticipantTopologyPartyAdded.serial} when adding $partyId to $targetParticipantId as part of $requestId.",
        )
        _ <- EitherT.cond[FutureUnlessShutdown](
          partyToParticipantTopologyPartyAdded.mapping.participants.exists(
            _.participantId == targetParticipantId
          ),
          (),
          s"Target participant $targetParticipantId missing from party $partyId even though just added as part of $requestId. This might be a race between expected serial $serialO and actual head serial ${partyToParticipantTopologyPartyAdded.serial}.",
        )
        _ <- EitherT.cond[FutureUnlessShutdown](
          partyToParticipantTopologyPartyAdded.mapping.participants.exists(
            _.participantId == sourceParticipantId
          ),
          (),
          s"Source participant $sourceParticipantId missing from party $partyId even though kept around as part of $requestId. This might be a race between expected serial $serialO and actual head serial ${partyToParticipantTopologyPartyAdded.serial}.",
        )
        // On the source participant wait until the topology change is visible via the ledger api
        _ <- EitherTUtil.ifThenET(participantId == sourceParticipantId) {
          val operation = s"observe $partyId topology transaction via ledger api"
          EitherT(
            retry
              .Pause(
                logger,
                this,
                maxRetries = 10,
                delay = timeouts.storageMaxRetryInterval.asFiniteApproximation,
                operationName = operation,
              )
              .unlessShutdown(
                performUnlessClosingF(operation)(
                  syncService.participantNodePersistentState.value.ledgerApiStore
                    .topologyEventOffsetPublishedOnRecordTime(
                      synchronizerId,
                      partyToParticipantTopologyPartyAdded.validFrom.value,
                    )
                    .map(offsetO => Either.cond(offsetO.nonEmpty, (), s"failed to $operation"))
                ),
                DbExceptionRetryPolicy,
              )
          )
        }
      } yield {
        val newStatus = PartyReplicationStatus.TopologyAuthorized(
          params,
          sequencerId,
          partyToParticipantTopologyPartyAdded.serial,
          partyToParticipantTopologyPartyAdded.validFrom.value,
        )
        logger.info(
          s"Party replication $requestId topology of party $partyId authorized with serial ${partyToParticipantTopologyPartyAdded.serial} and effective time ${partyToParticipantTopologyPartyAdded.validFrom.value}"
        )
        partyReplications.put(requestId, newStatus).discard
        connectToSequencerChannel(requestId)
      },
    )
  )

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

  private def connectToSequencerChannel(
      requestId: AddPartyRequestId
  )(implicit traceContext: TraceContext): Unit =
    executeAsync(
      s"connect to sequencer channel on behalf of party replication $requestId"
    )(
      recordIfError(
        requestId,
        for {
          state <- ensureParticipantStateAndSynchronizerConnected[
            PartyReplicationStatus.TopologyAuthorized
          ](requestId)
          (status, connectedSynchronizer, channelClient) = state
          AuthorizedReplicationParams(
            _,
            partyId,
            synchronizerId,
            sourceParticipantId,
            targetParticipantId,
            sequencerId,
            _,
            effectiveAt,
          ) = status.authorizedParams
          processorInfo <-
            if (participantId == sourceParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                (
                  PartyReplicationSourceParticipantProcessor(
                    synchronizerId,
                    partyId,
                    effectiveAt,
                    connectedSynchronizer.synchronizerHandle.syncPersistentState.acsInspection,
                    updateProgress(requestId, traceContext),
                    markComplete(requestId, traceContext),
                    connectedSynchronizer.staticSynchronizerParameters.protocolVersion,
                    timeouts,
                    loggerFactory,
                  ): SequencerChannelProtocolProcessor,
                  status.params.targetParticipantId,
                  false,
                )
              )
            } else if (participantId == targetParticipantId) {
              EitherT.rightT[FutureUnlessShutdown, String](
                (
                  PartyReplicationTargetParticipantProcessor(
                    synchronizerId,
                    partyId,
                    effectiveAt,
                    updateProgress(requestId, traceContext),
                    markComplete(requestId, traceContext),
                    (_, _) => EitherTUtil.unitUS,
                    syncService.participantNodePersistentState,
                    connectedSynchronizer,
                    connectedSynchronizer.staticSynchronizerParameters.protocolVersion,
                    timeouts,
                    loggerFactory,
                  ): SequencerChannelProtocolProcessor,
                  status.params.sourceParticipantId,
                  true,
                )
              )
            } else {
              EitherT.leftT[
                FutureUnlessShutdown,
                (SequencerChannelProtocolProcessor, ParticipantId, Boolean),
              ](
                s"participant $participantId is neither source nor target"
              )
            }
          (processor, participantIdToConnectTo, isSessionKeyOwner) = processorInfo
          _ <- channelClient
            .connectToSequencerChannel(
              sequencerId,
              SequencerChannelId(requestId.toHexString),
              participantIdToConnectTo,
              processor,
              isSessionKeyOwner,
              effectiveAt,
            )
            .mapK(FutureUnlessShutdown.liftK)
        } yield {
          val newStatus = ConnectionEstablished(status.authorizedParams)
          logger.info(s"Party replication $requestId connected to sequencer $sequencerId")
          partyReplications.put(requestId, newStatus).discard
        },
      )
    )

  private def updateProgress(requestId: AddPartyRequestId, tc: TraceContext)(
      contractsReplicated: NonNegativeInt
  ): Unit = {
    implicit val traceContext: TraceContext = tc
    executeAsync(s"progress party replication $requestId") {
      recordIfError(
        requestId,
        for {
          status <- EitherT.fromEither[FutureUnlessShutdown](
            partyReplications
              .get(requestId)
              .toRight(s"Unknown party replication $requestId")
          )
          previousParams <- EitherT.fromEither[FutureUnlessShutdown](status match {
            case PartyReplicationStatus.ConnectionEstablished(params) =>
              Right(params)
            case PartyReplicationStatus.ReplicatingAcs(params, _) =>
              Right(params)
            case unexpectedStatus =>
              Left(
                s"Party replication $requestId status ${unexpectedStatus.code} not expected"
              )
          })
        } yield {
          val newStatus = PartyReplicationStatus.ReplicatingAcs(
            previousParams,
            contractsReplicated,
          )
          partyReplications.put(requestId, newStatus).discard
        },
      )
    }
  }

  private def markComplete(requestId: AddPartyRequestId, tc: TraceContext)(
      contractsReplicated: NonNegativeInt
  ): Unit = {
    implicit val traceContext: TraceContext = tc
    executeAsync(s"progress party replication $requestId") {
      recordIfError(
        requestId,
        for {
          status <- EitherT.fromEither[FutureUnlessShutdown](
            partyReplications
              .get(requestId)
              .toRight(s"Unknown party replication $requestId")
          )
          replicatingAcs <- EitherT.fromEither[FutureUnlessShutdown](
            status
              .select[PartyReplicationStatus.ReplicatingAcs]
              .toRight(s"Party replication $requestId status $status not expected")
          )
        } yield {
          val newStatus = PartyReplicationStatus.Completed(
            replicatingAcs.authorizedParams,
            contractsReplicated,
          )
          partyReplications.put(requestId, newStatus).discard
        },
      )
    }
  }

  private def executeAsync(
      operation: String
  )(code: => FutureUnlessShutdown[Unit])(implicit traceContext: TraceContext): Unit = {
    logger.info(s"About to $operation")
    FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
      executionQueue.executeUS(code, operation),
      s"$operation failed",
    )
  }

  private def recordIfError(
      requestId: AddPartyRequestId,
      resultET: => EitherT[FutureUnlessShutdown, String, Unit],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = resultET
    .leftMap(err =>
      partyReplications
        .get(requestId)
        .fold(
          // If party replication id is unknown, log the unexpected call.
          logger.warn(err)
        )(prevState =>
          partyReplications
            .put(
              requestId,
              PartyReplicationStatus.Error(err, prevState),
            )
            .discard
        )
    )
    .merge

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

  override protected def onClosed(): Unit = LifeCycle.close(executionQueue)(logger)
}
