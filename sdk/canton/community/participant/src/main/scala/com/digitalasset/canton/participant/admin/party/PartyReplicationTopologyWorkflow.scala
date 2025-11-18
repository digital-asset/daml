// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.{
  FlagNotSet,
  FlagSet,
  PartyOnboardingFlagStatus,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.protocol.DynamicSynchronizerParametersHistory
import com.digitalasset.canton.topology.TopologyManagerError.NoAppropriateSigningKeyInStore
import com.digitalasset.canton.topology.store.TopologyStoreId.SynchronizerStore
import com.digitalasset.canton.topology.store.{StoredTopologyTransaction, TimeQuery, TopologyStore}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Replace
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  PartyToParticipant,
  TopologyChangeOp,
  TopologyMapping,
}
import com.digitalasset.canton.topology.{
  ForceFlags,
  ParticipantId,
  PartyId,
  SynchronizerTopologyManager,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps

/** The OnPR topology workflow manages the interaction with topology processing with respect to
  * authorizing PartyToParticipant topology changes and verifying that authorized topology changes
  * permit party replication.
  */
class PartyReplicationTopologyWorkflow(
    participantId: ParticipantId,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends FlagCloseable
    with NamedLogging {

  /** Attempt to authorize the onboarding topology for the party replication request on the target
    * participant. Once the onboarding topology with the expected serial is authorized, verify the
    * topology transaction, e.g. the party has a hosting permission on the source and target
    * participants. Do so in an idempotent way such that this function can be retried.
    *
    * @param params
    *   party replication parameters
    * @param connectedSynchronizer
    *   connected synchronizer
    * @return
    *   effective time of the onboarding topology transaction or None if not yet authorized
    */
  private[party] def authorizeOnboardingTopology(
      params: PartyReplicationStatus.ReplicationParams,
      connectedSynchronizer: ConnectedSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[CantonTimestamp]] = {
    val PartyReplicationStatus
      .ReplicationParams(
        requestId,
        partyId,
        _,
        sourceParticipantId,
        targetParticipantId,
        serial,
        _,
      ) = params
    val topologyStore = connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore
    val topologyManager =
      connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyManager
    for {
      _ <- EitherT(
        partyToParticipantTopologyHeadO(partyId, topologyStore).map(txO =>
          Either.cond(
            txO.exists(_.mapping.participants.exists(_.participantId == sourceParticipantId)),
            (),
            s"Party $partyId is not hosted by source participant $sourceParticipantId",
          )
        )
      )
      _ <- EitherTUtil.ifThenET(participantId == targetParticipantId)(
        authorizeByTargetParticipant(params, topologyManager, topologyStore)
      )
      // Only verify the authorized topology once the expected serial has been authorized.
      // It is conceivable that not only the topology transaction with the expected serial has been authorized,
      // but a subsequent serial as well. Therefore, proceed with topology verification if the head serial is larger
      // than or equal (">=") the expected serial.
      partyToParticipantTopologyPartyAddedO <- EitherT.right[String](
        partyToParticipantTopologyHeadO(partyId, topologyStore).map(
          _.filter(_.serial >= serial)
        )
      )
      // Insist that our serial is the latest head state to raise an error if a potentially conflicting
      // topology transaction has been authorized in the meantime.
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyPartyAddedO.forall(serial == _.serial),
        (),
        s"Specified serial $serial does not match the newest serial ${partyToParticipantTopologyPartyAddedO
            .map(_.serial)} when adding $partyId to $targetParticipantId as part of $requestId. Has there been another potentially conflicting party hosting modification?",
      )
      _ <- partyToParticipantTopologyPartyAddedO.fold(
        EitherT.rightT[FutureUnlessShutdown, String](())
      )(verifyAuthorizedTopology(params, _))
    } yield partyToParticipantTopologyPartyAddedO.map(_.validFrom.value)
  }

  /** Only called on the target participant. Authorize party replication onboarding from the target
    * participant point of view unless the expected serial has already been authorized. The called
    * verifies the validity of the authorized topology transaction for party replication once
    * authorized.
    */
  private def authorizeByTargetParticipant(
      params: PartyReplicationStatus.ReplicationParams,
      topologyManager: SynchronizerTopologyManager,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    require(
      participantId == params.targetParticipantId,
      "must only be called on target participant",
    )
    for {
      partyToParticipantTopologyHeadTx <- partyToParticipantTopologyHead(
        params.partyId,
        topologyStore,
      )
      // If the topology transaction with the matching serial has not yet been authorized, have the
      // target participant propose and sign the party onboarding topology if the TP signature is missing.
      _ <- EitherTUtil.ifThenET(partyToParticipantTopologyHeadTx.serial < params.serial)(
        addTargetParticipantSignatureIfMissing(
          params,
          partyToParticipantTopologyHeadTx.mapping,
          topologyManager,
          topologyStore,
        )
      )
    } yield ()
  }

  /** Only called on the target participant to check if the target participant has already signed
    * the onboarding topology transaction, and add the signature if necessary.
    */
  private def addTargetParticipantSignatureIfMissing(
      params: PartyReplicationStatus.ReplicationParams,
      ptpPrevious: PartyToParticipant,
      topologyManager: SynchronizerTopologyManager,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val PartyReplicationStatus
      .ReplicationParams(
        requestId,
        partyId,
        _,
        _,
        targetParticipantId,
        serial,
        participantPermission,
      ) = params

    require(participantId == targetParticipantId, "must only be called on target participant")

    for {
      ptpProposal <- EitherT.fromEither[FutureUnlessShutdown](
        PartyToParticipant
          .create(
            partyId,
            ptpPrevious.threshold,
            ptpPrevious.participants :+ HostingParticipant(
              targetParticipantId,
              participantPermission,
              onboarding = true,
            ),
          )
      )
      existingProposalO <- EitherT.right[String](
        partyToParticipantTopologyHeadO(
          partyId,
          topologyStore,
          proposal = true,
        ).map(_.filter { proposal =>
          proposal.serial == serial && (proposal.mapping match {
            case PartyToParticipant(partyId, _thresholdMayDiffer, participants, _) =>
              partyId == ptpProposal.partyId && participants == ptpProposal.participants
          })
        })
      )
      // For idempotency, check if the TP has already signed the proposal in a previous try.
      hasTargetParticipantAlreadySigned <- existingProposalO.fold {
        logger.debug(
          s"No existing onboarding topology proposal found for party replication $requestId and party $partyId"
        )
        EitherT.rightT[FutureUnlessShutdown, String](false)
      } { existingProposal =>
        logger.debug(
          s"About to check if target participant signature is missing from onboarding topology proposal for party replication $requestId and party $partyId: Existing proposal: $existingProposal"
        )
        // Check if the target participant signature is already present by extending the signed transaction.
        // If the signed transaction does not change, the TP has already signed.
        topologyManager
          .extendSignature(
            existingProposal.transaction,
            // Don't specify signing keys to let the topology manager figure out the TP keys as it is complicated
            // for code outside the topology manager to determine the signing keys in general topologies.
            signingKeys = Seq.empty,
            forceFlags = ForceFlags.none,
          )
          .map { proposalSignedByTP =>
            (proposalSignedByTP.transaction == existingProposal.transaction.transaction &&
              // since signatures don't compare by content, check the size
              proposalSignedByTP.signatures.sizeCompare(
                existingProposal.transaction.signatures
              ) == 0).tap(
              if (_)
                logger.debug(
                  s"Onboarding proposal for party replication $requestId and party $partyId on target participant $targetParticipantId already signed by TP"
                )
              else
                logger.info(
                  s"Onboarding proposal for party replication $requestId and party $partyId on target participant $targetParticipantId missing TP signature; proposal signed by TP: $proposalSignedByTP"
                )
            )
          }
          .recover { case err @ NoAppropriateSigningKeyInStore.Failure(_, _) =>
            // The existingProposal may have been authorized between the proposal query above and the topology manager
            // call. Such a race condition results in a NoAppropriateSigningKeyInStore error because the authorized
            // topology transaction cannot be signed anymore by any key. Accordingly return true to indicate that
            // the TP signature is no longer needed.
            logger.info(
              s"No appropriate key response during key lookup indicates race with proposal authorization: $err"
            )
            true
          }
          .leftMap(_.asGrpcError.getMessage)
      }
      // Sign and authorize the party addition on the target participant if the TP has not already signed.
      _ <- EitherTUtil.ifThenET(!hasTargetParticipantAlreadySigned)(
        {
          topologyManager
            .proposeAndAuthorize(
              op = TopologyChangeOp.Replace,
              mapping = existingProposalO.map(_.mapping).getOrElse(ptpProposal),
              serial = Some(serial),
              signingKeys = Seq.empty, // Rely on topology manager to use the right TP signing keys
              protocolVersion = topologyManager.managerVersion.serialization,
              expectFullAuthorization = false,
              forceChanges = ForceFlags.none,
              waitToBecomeEffective = None,
            )
            .map(_ => ())
            .recover { case err @ NoAppropriateSigningKeyInStore.Failure(_, _) =>
              // See the note above on the possible race condition between the existingProposal and the topology manager call.
              logger.info(
                s"No appropriate key response to proposing topology change indicates race with proposal authorization: $err"
              )
            }
            .leftMap { err =>
              val exception = err.asGrpcError
              logger.warn(
                s"Error proposing party to participant topology change on $participantId",
                exception,
              )
              exception.getMessage
            }
        }
      )
    } yield ()
  }

  /** Verifies that party onboarding has been properly authorized, i.e. that no concurrent topology
    * change conflicts with party replication.
    */
  private def verifyAuthorizedTopology(
      params: PartyReplicationStatus.ReplicationParams,
      partyToParticipantTopologyPartyAdded: StoredTopologyTransaction[
        TopologyChangeOp.Replace,
        PartyToParticipant,
      ],
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val PartyReplicationStatus
      .ReplicationParams(
        requestId,
        partyId,
        _,
        sourceParticipantId,
        targetParticipantId,
        _,
        _,
      ) = params
    for {
      // Check that the SP and TP are now indeed authorized to host the party.
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyPartyAdded.mapping.participants.exists(p =>
          p.participantId == targetParticipantId && p.onboarding
        ),
        (),
        s"Target participant $targetParticipantId not authorized to onboard party $partyId even though just added as part of request $requestId.",
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        partyToParticipantTopologyPartyAdded.mapping.participants.exists(
          _.participantId == sourceParticipantId
        ),
        (),
        s"Source participant $sourceParticipantId authorization to host party $partyId has been removed, but is necessary for request $requestId.",
      )
    } yield ()
  }

  /** Attempt to authorize the onboarded topology for the party replication request on the target
    * participant. Once the onboarded topology is authorized, verify the topology transaction, e.g.
    * the party has a hosting permission on the target participants without the onboarding flag. Do
    * so in an idempotent way such that this function can be retried.
    *
    * @param params
    *   party replication parameters
    * @param onboardingEffectiveAt
    *   effective time of the onboarding topology transaction needed to determine the safe time to
    *   clear the onboarding flag.
    * @param connectedSynchronizer
    *   connected synchronizer
    * @return
    *   whether the onboarded topology has been authorized
    */
  private[party] def authorizeOnboardedTopology(
      params: PartyReplicationStatus.ReplicationParams,
      onboardingEffectiveAt: CantonTimestamp,
      connectedSynchronizer: ConnectedSynchronizer,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean] = {
    val PartyReplicationStatus
      .ReplicationParams(
        requestId,
        partyId,
        _,
        _,
        targetParticipantId,
        _,
        _,
      ) = params
    val res = authorizeOnboardedTopology(
      partyId,
      targetParticipantId,
      onboardingEffectiveAt,
      connectedSynchronizer,
      Some(requestId),
    )
    res.map(_.status._1)
  }

  private[admin] def authorizeOnboardedTopology(
      partyId: PartyId,
      targetParticipantId: ParticipantId,
      onboardingEffectiveAt: CantonTimestamp,
      connectedSynchronizer: ConnectedSynchronizer,
      requestId: Option[Hash] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, PartyOnboardingFlagStatus] = {
    val synchronizerId = connectedSynchronizer.psid.logical
    val requestIdLogPart = if (requestId.nonEmpty) s"For request $requestId: " else ""
    val synchronizerTimeTracker = connectedSynchronizer.ephemeral.timeTracker
    val topologyManager =
      connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyManager
    val topologyStore = connectedSynchronizer.synchronizerHandle.syncPersistentState.topologyStore
    val topologyClient = connectedSynchronizer.synchronizerHandle.topologyClient
    for {
      ptpHeadTxn <- EitherT(
        partyToParticipantTopologyHeadO(partyId, topologyStore).map(txO =>
          txO
            .filter(_.mapping.participants.exists(_.participantId == targetParticipantId))
            .toRight(
              s"${requestIdLogPart}Party $partyId is not hosted by target participant $targetParticipantId"
            )
        )
      ): EitherT[
        FutureUnlessShutdown,
        String,
        StoredTopologyTransaction[Replace, PartyToParticipant],
      ]
      onboardedPtpProposalO <- EitherT.fromEither[FutureUnlessShutdown](
        if (
          ptpHeadTxn.mapping.participants
            .exists(p => p.participantId == targetParticipantId && p.onboarding)
        ) {
          PartyToParticipant
            .create(
              ptpHeadTxn.mapping.partyId,
              ptpHeadTxn.mapping.threshold,
              ptpHeadTxn.mapping.participants.map {
                case HostingParticipant(`targetParticipantId`, permission, true) =>
                  HostingParticipant(targetParticipantId, permission, onboarding = false)
                case otherParticipant => otherParticipant
              },
            )
            .map(ptp => Some(ptp -> ptpHeadTxn.serial.increment))
        } else Right(None)
      )
      latestSynchronizerTimestampObservedO = synchronizerTimeTracker.latestTime

      partyOnboardingStatus <- onboardedPtpProposalO match {
        case None =>
          // The party does not have the 'onboarding' flag set, return FlagNotSet flag status
          EitherT.rightT[FutureUnlessShutdown, String](FlagNotSet)

        case Some((ptpProposal, serial))
            if participantId == targetParticipantId && latestSynchronizerTimestampObservedO.isDefined =>
          // This is the target participant, and it is responsible for clearing the flag
          logger.info(
            s"${requestIdLogPart}About to mark party $partyId as onboarded on target participant"
          )

          for {
            _ <- EitherT.cond[FutureUnlessShutdown](
              topologyClient.snapshotAvailable(onboardingEffectiveAt),
              (),
              s"Synchronizer $synchronizerId does not have a snapshot at onboarding effective time $onboardingEffectiveAt",
            )
            onboardingTsSnapshot <- EitherT.right[String](
              topologyClient.snapshot(onboardingEffectiveAt)
            )
            synchronizerParameterHistory <- EitherT.right[String](
              onboardingTsSnapshot.listDynamicSynchronizerParametersChanges()
            )
            decisionDeadline = DynamicSynchronizerParametersHistory
              .latestDecisionDeadlineEffectiveAt(
                synchronizerParameterHistory,
                onboardingEffectiveAt,
              )
            _ = logger.debug(
              s"""safe timestamp: $decisionDeadline compared to
                   |latest synchronizer ts $latestSynchronizerTimestampObservedO
                   |with onboardingEffectiveAt $onboardingEffectiveAt"
                """.stripMargin
            )

            isSafeToOnboard = latestSynchronizerTimestampObservedO.exists(_ > decisionDeadline)
            _ <-
              if (isSafeToOnboard) {
                topologyManager
                  .proposeAndAuthorize(
                    op = TopologyChangeOp.Replace,
                    mapping = ptpProposal,
                    serial = Some(serial),
                    signingKeys =
                      Seq.empty, // Rely on topology manager to use the right TP signing keys
                    protocolVersion = topologyManager.managerVersion.serialization,
                    expectFullAuthorization =
                      true, // expect full authorization when onboarding is done
                    forceChanges = ForceFlags.none,
                    waitToBecomeEffective = None,
                  )
                  .map(_ => ())
                  .recover { case err @ NoAppropriateSigningKeyInStore.Failure(_, _) =>
                    // See the note above on the possible race condition between the existingProposal and the topology manager call.
                    logger.info(
                      s"${requestIdLogPart}No appropriate key response to proposing topology change for $partyId indicates race with proposal authorization: $err"
                    )
                  }
                  .leftMap { err =>
                    val exception = err.asGrpcError
                    logger.warn(
                      s"${requestIdLogPart}Error proposing party to participant topology change on $participantId for $partyId",
                      exception,
                    )
                    exception.getMessage
                  }
              } else {
                // If it is not yet safe to onboard, ask for a time proof in case the synchronizer does not
                // serve any load, so that the party does not stay in the onboarding state until the next
                // "minObservationDuration" (24 hours by default).
                logger.info(
                  s"Requesting time proof to advance synchronizer time to the safe onboarded timestamp $decisionDeadline"
                )
                synchronizerTimeTracker.requestTick(decisionDeadline.immediateSuccessor).discard
                EitherTUtil.unitUS[String]
              }
          } yield FlagSet(decisionDeadline)

        case Some((_, _)) =>
          // This case handles a non-target participant or a target participant whose synchronizer has not yet observed time.
          // In either case, this node takes no action, so the flag status is effectively FlagNotSet.
          EitherT.rightT[FutureUnlessShutdown, String](FlagNotSet)
      }
    } yield partyOnboardingStatus
  }

  private def partyToParticipantTopologyHeadO(
      partyId: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
      proposal: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Option[StoredTopologyTransaction[Replace, PartyToParticipant]]] =
    // TODO(#25766): add topology client endpoint
    topologyStore
      .inspect(
        proposals = proposal,
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
      )

  private[party] def partyToParticipantTopologyHead(
      partyId: PartyId,
      topologyStore: TopologyStore[SynchronizerStore],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, StoredTopologyTransaction[Replace, PartyToParticipant]] =
    EitherT(
      partyToParticipantTopologyHeadO(partyId, topologyStore).map(
        _.toRight(
          s"Party $partyId not hosted on synchronizer ${topologyStore.storeId.psid}"
        )
      )
    )
}
