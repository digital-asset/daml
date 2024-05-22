// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.data.{Chain, EitherT, OptionT}
import cats.implicits.catsStdInstancesForFuture
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.data.BlockUpdateEphemeralState
import com.digitalasset.canton.domain.block.update.SubmissionRequestValidator.{
  SubmissionRequestValidationResult,
  isMemberRegistered,
  updateTrafficState,
}
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.{
  SequencerRateLimitError,
  SequencerRateLimitManager,
}
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.client.SequencedEventValidator
import com.digitalasset.canton.sequencing.client.SequencedEventValidator.TopologyTimestampVerificationError
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.{ExecutionContext, Future}

/** Validates a single [[SubmissionRequest]] within a chunk.
  */
private[update] final class SubmissionRequestValidator(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    override val loggerFactory: NamedLoggerFactory,
)(implicit closeContext: CloseContext)
    extends NamedLogging {

  /** Returns the snapshot for signing the events (if the submission request specifies a signing timestamp)
    * and the sequenced events by member.
    *
    * Drops the submission request if the sender is not registered or
    * the [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.maxSequencingTime]]
    * is before the `sequencingTimestamp`.
    *
    * Produces a [[com.digitalasset.canton.sequencing.protocol.DeliverError]]
    * if some recipients are unknown or the requested
    * [[com.digitalasset.canton.sequencing.protocol.SubmissionRequest.topologyTimestamp]]
    * is too old or after the `sequencingTime`.
    */
  def validateAndGenerateSequencedEvents(
      state: BlockUpdateEphemeralState,
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      sequencingSnapshot: SyncCryptoApi,
      signingSnapshot: Option[SyncCryptoApi],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[SubmissionRequestValidationResult] = {
    // A bit more convoluted than we'd like here, but the goal is to be able to use the traffic updated state
    //  in the result, even if the aggregation logic performed in 'finalizeProcessing' short-circuits
    //  (for instance because we've already reached the aggregation threshold).
    validateAndUpdateTraffic(
      sequencingTimestamp,
      signedSubmissionRequest,
      state,
      sequencingSnapshot,
      signingSnapshot,
      latestSequencerEventTimestamp,
    )
      .flatMap { case (groupToMembers, stateAfterTrafficConsume) =>
        finalizeProcessing(
          groupToMembers,
          stateAfterTrafficConsume,
          sequencingTimestamp,
          signedSubmissionRequest.content,
          state,
          sequencingSnapshot,
          signingSnapshot,
        )
          // Use the traffic updated ephemeral state in the response even if the rest of the processing stopped
          .recover { errorSubmissionOutcome =>
            SubmissionRequestValidationResult(
              stateAfterTrafficConsume,
              errorSubmissionOutcome,
              None,
            )
          }
      }
      .leftMap { errorSubmissionOutcome =>
        SubmissionRequestValidationResult(state, errorSubmissionOutcome, None)
      }
      .merge
  }

  // Below are a 3 functions, each a for-comprehension of EitherT.
  // In each Lefts are used to stop processing the submission request and immediately produce the sequenced events
  // They are split into 3 functions to make it possible to re-use intermediate results (specifically
  // BlockUpdateEphemeralState containing updated traffic states), even if further processing fails.

  // After performing initial validations, consumes traffic for the sender and updates the ephemeral state
  private def validateAndUpdateTraffic(
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      state: BlockUpdateEphemeralState,
      sequencingSnapshot: SyncCryptoApi,
      signingSnapshot: Option[SyncCryptoApi],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    SubmissionRequestOutcome,
    (Map[GroupRecipient, Set[Member]], BlockUpdateEphemeralState),
  ] =
    for {
      groupToMembers <- performInitialValidations(
        state,
        sequencingTimestamp,
        signedSubmissionRequest,
        sequencingSnapshot,
        signingSnapshot,
        latestSequencerEventTimestamp,
      )
      stateAfterTrafficConsume <- updateRateLimiting(
        state,
        signedSubmissionRequest.content,
        sequencingTimestamp,
        sequencingSnapshot,
        groupToMembers,
        latestSequencerEventTimestamp,
        warnIfApproximate = state.headCounterAboveGenesis(sequencerId),
      )
    } yield (groupToMembers, stateAfterTrafficConsume)

  // Performs initial validations and resolves groups to members
  private def performInitialValidations(
      state: BlockUpdateEphemeralState,
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      sequencingSnapshot: SyncCryptoApi,
      signingSnapshotO: Option[SyncCryptoApi],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ) = {
    val submissionRequest = signedSubmissionRequest.content
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        state.registeredMembers.contains(submissionRequest.sender),
        (),
        // we expect callers to validate the sender exists before queuing requests on their behalf
        // if we hit this case here it likely means the caller didn't check, or the member has subsequently
        // been deleted.
        {
          logger.warn(
            s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] " +
              "is not registered so cannot send or receive events. Dropping send request."
          )
          SubmissionRequestOutcome.discardSubmissionRequest
        },
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        sequencingTimestamp <= submissionRequest.maxSequencingTime,
        (),
        // The sequencer is beyond the timestamp allowed for sequencing this request so it is silently dropped.
        // A correct sender should be monitoring their sequenced events and notice that the max-sequencing-time has been
        // exceeded and trigger a timeout.
        // We don't log this as a warning as it is expected behaviour. Within a distributed network, the source of
        // a delay can come from different nodes and we should only log this as a warning in a way where we can
        // attribute the delay to a specific node.
        {
          SequencerError.ExceededMaxSequencingTime
            .Error(
              sequencingTimestamp,
              submissionRequest.maxSequencingTime,
              submissionRequest.messageId.unwrap,
            )
            .discard
          SubmissionRequestOutcome.discardSubmissionRequest
        },
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](
        checkRecipientsAreKnown(
          state,
          submissionRequest,
          sequencingTimestamp,
        )
      )
      // Warn if we use an approximate snapshot but only after we've read at least one
      _ <- checkSignatureOnSubmissionRequest(
        sequencingTimestamp,
        signedSubmissionRequest,
        sequencingSnapshot,
        latestSequencerEventTimestamp,
      )
      _ <- validateTopologyTimestamp(
        state,
        sequencingTimestamp,
        submissionRequest,
        sequencerId,
        latestSequencerEventTimestamp,
      )
      topologySnapshot = signingSnapshotO.getOrElse(sequencingSnapshot)
      // TODO(i17584): revisit the consequences of no longer enforcing that
      //  aggregated submissions with signed envelopes define a topology snapshot
      _ <- validateMaxSequencingTimeForAggregationRule(
        state,
        submissionRequest,
        topologySnapshot,
        sequencingTimestamp,
      ).mapK(FutureUnlessShutdown.outcomeK)
      _ <- checkClosedEnvelopesSignatures(
        signingSnapshotO,
        submissionRequest,
        sequencingTimestamp,
      ).mapK(FutureUnlessShutdown.outcomeK)
      groupToMembers <-
        groupRecipientsToMembers(
          state,
          submissionRequest,
          sequencingTimestamp,
          topologySnapshot,
        )
    } yield groupToMembers
  }

  // TODO(#18401): This method should be harmonized with the GroupAddressResolver
  private def groupRecipientsToMembers(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologySnapshot: SyncCryptoApi,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
    val groupRecipients = submissionRequest.batch.allRecipients.collect {
      case group: GroupRecipient =>
        group
    }

    if (groupRecipients.isEmpty)
      EitherT.rightT(Map.empty)
    else
      for {
        participantsOfPartyToMembers <-
          expandParticipantGroupRecipients(
            state,
            submissionRequest,
            sequencingTimestamp,
            groupRecipients,
            topologySnapshot,
          )
        mediatorGroupsToMembers <-
          expandMediatorGroupRecipients(
            state,
            submissionRequest,
            sequencingTimestamp,
            groupRecipients,
            topologySnapshot,
          )
        allMembersOfDomainToMembers <-
          expandAllMembersOfDomainGroupRecipients(
            state,
            submissionRequest,
            sequencingTimestamp,
            topologySnapshot,
            groupRecipients,
          )
        sequencersOfDomainToMembers <-
          expandSequencersOfDomainGroupRecipients(
            state,
            submissionRequest,
            sequencingTimestamp,
            topologySnapshot,
            groupRecipients,
          )
      } yield participantsOfPartyToMembers ++ mediatorGroupsToMembers ++ sequencersOfDomainToMembers ++ allMembersOfDomainToMembers
  }

  private def expandSequencersOfDomainGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologySnapshot: SyncCryptoApi,
      groupRecipients: Set[GroupRecipient],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
    val useSequencersOfDomain = groupRecipients.contains(SequencersOfDomain)
    if (useSequencersOfDomain) {
      for {
        sequencers <- EitherT(
          topologySnapshot.ipsSnapshot
            .sequencerGroup()
            .map(
              _.fold[Either[SubmissionRequestOutcome, Set[Member]]](
                // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
                Left(
                  invalidSubmissionRequest(
                    state,
                    submissionRequest,
                    sequencingTimestamp,
                    SequencerErrors.SubmissionRequestRefused("No sequencer group found"),
                  )
                )
              )(group => Right((group.active.forgetNE ++ group.passive).toSet))
            )
        )
        _ <- {
          val nonRegistered = sequencers.filterNot(isMemberRegistered(state))
          EitherT.cond[Future](
            nonRegistered.isEmpty,
            (),
            // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"The sequencer group contains non registered sequencers $nonRegistered"
              ),
            ),
          )
        }
      } yield Map((SequencersOfDomain: GroupRecipient) -> sequencers)
    } else
      EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def expandAllMembersOfDomainGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologySnapshot: SyncCryptoApi,
      groupRecipients: Set[GroupRecipient],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
    if (!groupRecipients.contains(AllMembersOfDomain)) {
      EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
    } else {
      for {
        allMembers <- EitherT.right[SubmissionRequestOutcome](
          topologySnapshot.ipsSnapshot.allMembers()
        )
        _ <- {
          // this can happen when a
          val nonRegistered = allMembers.filterNot(isMemberRegistered(state))
          EitherT.cond[Future](
            nonRegistered.isEmpty,
            (),
            // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"The broadcast group contains non registered members $nonRegistered"
              ),
            ),
          )
        }
      } yield Map((AllMembersOfDomain: GroupRecipient, allMembers))
    }
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def expandMediatorGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      groupRecipients: Set[GroupRecipient],
      topologySnapshot: SyncCryptoApi,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
    val mediatorGroups =
      groupRecipients.collect { case MediatorGroupRecipient(group) =>
        group
      }.toSeq
    if (mediatorGroups.isEmpty)
      EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
    else
      for {
        groups <- topologySnapshot.ipsSnapshot
          .mediatorGroupsOfAll(mediatorGroups)
          .leftMap(nonExistingGroups =>
            // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"The following mediator groups do not exist $nonExistingGroups"
              ),
            )
          )
        _ <- groups.parTraverse { group =>
          val nonRegistered =
            (group.active ++ group.passive).filterNot(isMemberRegistered(state))
          EitherT.cond[Future](
            nonRegistered.isEmpty,
            (),
            // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"The mediator group ${group.index} contains non registered mediators $nonRegistered"
              ),
            ),
          )
        }
      } yield GroupAddressResolver.asGroupRecipientsToMembers(groups)
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def expandParticipantGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      groupRecipients: Set[GroupRecipient],
      topologySnapshot: SyncCryptoApi,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
    val parties = groupRecipients.collect { case ParticipantsOfParty(party) =>
      party.toLf
    }
    if (parties.isEmpty)
      EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
    else
      for {
        _ <- topologySnapshot.ipsSnapshot
          .allHaveActiveParticipants(parties)
          .leftMap(parties =>
            // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"The following parties do not have active participants $parties"
              ),
            )
          )
        mapping <- EitherT.right[SubmissionRequestOutcome](
          topologySnapshot.ipsSnapshot.activeParticipantsOfParties(parties.toSeq)
        )
        _ <- mapping.toList.parTraverse { case (party, participants) =>
          val nonRegistered = participants.filterNot(isMemberRegistered(state))
          EitherT.cond[Future](
            nonRegistered.isEmpty,
            (),
            // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"The party $party is hosted on non registered participants $nonRegistered"
              ),
            ),
          )
        }
      } yield GroupAddressResolver.asGroupRecipientsToMembers(mapping)
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def checkClosedEnvelopesSignatures(
      signingSnapshotO: Option[SyncCryptoApi],
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] =
    signingSnapshotO match {
      case None =>
        EitherT.right[SubmissionRequestOutcome](Future.unit)
      case Some(signingSnapshot) =>
        submissionRequest.batch.envelopes
          .parTraverse_ { closedEnvelope =>
            closedEnvelope.verifySignatures(
              signingSnapshot,
              submissionRequest.sender,
            )
          }
          .leftMap { error =>
            SequencerError.InvalidEnvelopeSignature
              .Error(
                submissionRequest,
                error,
                sequencingTimestamp,
                signingSnapshot.ipsSnapshot.timestamp,
              )
              .report()
            SubmissionRequestOutcome.discardSubmissionRequest
          }
    }

  private def validateMaxSequencingTimeForAggregationRule(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      topologySnapshot: SyncCryptoApi,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] =
    submissionRequest.aggregationRule.traverse_ { _aggregationRule =>
      for {
        domainParameters <- EitherT(topologySnapshot.ipsSnapshot.findDynamicDomainParameters())
          .leftMap(error =>
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.SubmissionRequestRefused(
                s"Could not fetch dynamic domain parameters: $error"
              ),
            )
          )
        maxSequencingTimeUpperBound = sequencingTimestamp.toInstant.plus(
          domainParameters.parameters.sequencerAggregateSubmissionTimeout.duration
        )
        _ <- EitherTUtil.condUnitET[Future](
          submissionRequest.maxSequencingTime.toInstant.isBefore(maxSequencingTimeUpperBound),
          invalidSubmissionRequest(
            state,
            submissionRequest,
            sequencingTimestamp,
            SequencerErrors.MaxSequencingTimeTooFar(
              submissionRequest.messageId,
              submissionRequest.maxSequencingTime,
              maxSequencingTimeUpperBound,
            ),
          ),
        )
      } yield ()
    }

  private def updateRateLimiting(
      state: BlockUpdateEphemeralState,
      request: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencingSnapshot: SyncCryptoApi,
      groupToMembers: Map[GroupRecipient, Set[Member]],
      lastSeenTopologyTimestamp: Option[CantonTimestamp],
      warnIfApproximate: Boolean,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, BlockUpdateEphemeralState] = {
    val newState =
      for {
        _ <- OptionT.when[FutureUnlessShutdown, Unit]({
          request.sender match {
            // Sequencers are not rate limited
            case _: SequencerId => false
            case _ => true
          }
        })(())
        parameters <- OptionT(
          sequencingSnapshot.ipsSnapshot.trafficControlParameters(protocolVersion)
        )
        sender = request.sender
        // Get the traffic from the ephemeral state
        trafficState = state.trafficState.getOrElse(
          sender,
          // If there's no trace of this member. But we have ensured that the sender is registered
          // and all registered members should have a traffic state.
          ErrorUtil.invalidState(s"Sender $sender unknown by rate limiter."),
        )
        _ <-
          if (sequencingTimestamp <= trafficState.timestamp) {
            logger.warn(
              s"Trying to consume an event with a sequencing timestamp ($sequencingTimestamp)" +
                s" <= to the current traffic state timestamp ($trafficState)."
            )
            OptionT.none[FutureUnlessShutdown, Unit]
          } else OptionT.some[FutureUnlessShutdown](())
        _ = logger.trace(
          s"Consuming traffic cost for event with messageId ${request.messageId}" +
            s" from sender $sender at $sequencingTimestamp"
        )
        // Consume traffic for the sender
        newSenderTrafficState <- OptionT.liftF(
          rateLimitManager
            .consume(
              request.sender,
              request.batch,
              sequencingTimestamp,
              trafficState,
              parameters,
              groupToMembers,
              lastBalanceUpdateTimestamp = lastSeenTopologyTimestamp,
              warnIfApproximate = warnIfApproximate,
            )
            .map(Right(_))
            .valueOr {
              case error: SequencerRateLimitError.EventOutOfOrder =>
                logger.warn(
                  s"Consumed an event out of order for member ${error.member} with traffic state '$trafficState'. " +
                    s"Current traffic state timestamp is ${error.currentTimestamp} " +
                    s"but event timestamp is ${error.eventTimestamp}. The traffic state will not be updated."
                )
                Right(trafficState)
              case error: SequencerRateLimitError.AboveTrafficLimit
                  if parameters.enforceRateLimiting =>
                logger.info(
                  s"Submission from member ${error.member} with traffic state '${error.trafficState.toString}' " +
                    s"was above traffic limit. Submission cost: ${error.trafficCost.value}." +
                    s" The message will not be delivered."
                )
                Left(
                  SubmissionRequestOutcome.reject(
                    request,
                    sender,
                    DeliverError.create(
                      state.tryNextCounter(sender),
                      sequencingTimestamp,
                      domainId,
                      request.messageId,
                      SequencerErrors
                        .TrafficCredit(
                          s"Not enough traffic credit for sender $sender to send message with ID ${request.messageId}:" +
                            s" $error"
                        ),
                      protocolVersion,
                    ),
                  )
                )
              case error: SequencerRateLimitError.AboveTrafficLimit =>
                logger.info(
                  s"Submission from member ${error.member} with traffic state '${error.trafficState.toString}' " +
                    s"was above traffic limit. Submission cost: ${error.trafficCost.value}. " +
                    s"The message will still be delivered."
                )
                Right(error.trafficState)
              case error: SequencerRateLimitError.UnknownBalance =>
                logger.warn(
                  s"Could not obtain valid balance at $sequencingTimestamp for member ${error.member} " +
                    s"with traffic state '$trafficState'. The message will still be delivered " +
                    "but the traffic state has not been updated."
                )
                Right(trafficState)
            }
        )
      } yield newSenderTrafficState.map(updateTrafficState(state, sender, _))

    EitherT(newState.getOrElse(Right(state)))
  }

  private def checkRecipientsAreKnown(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Either[SubmissionRequestOutcome, Unit] = {
    // group addresses checks are covered separately later on
    val unknownRecipients = submissionRequest.batch.allMembers diff state.registeredMembers
    Either.cond(
      unknownRecipients.isEmpty,
      (),
      invalidSubmissionRequest(
        state,
        submissionRequest,
        sequencingTimestamp,
        SequencerErrors.UnknownRecipients(unknownRecipients.toSeq),
      ),
    )
  }

  private def checkSignatureOnSubmissionRequest(
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      sequencingSnapshot: SyncCryptoApi,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Unit] = {
    val submissionRequest = signedSubmissionRequest.content

    // If we haven't seen any topology transactions yet, then we cannot verify signatures, so we skip it.
    // In practice this should only happen for the first ever transaction, which contains the initial topology data.
    val skipCheck =
      latestSequencerEventTimestamp.isEmpty || !submissionRequest.sender.isAuthenticated
    if (skipCheck) {
      EitherT.pure[FutureUnlessShutdown, SubmissionRequestOutcome](())
    } else {
      val alarm = for {
        timestampOfSigningKey <- EitherT.fromEither[FutureUnlessShutdown](
          signedSubmissionRequest.timestampOfSigningKey.toRight[BaseAlarm](
            SequencerError.MissingSubmissionRequestSignatureTimestamp.Error(
              signedSubmissionRequest,
              sequencingTimestamp,
            )
          )
        )
        // We are consciously picking the sequencing timestamp for checking the signature instead of the
        // timestamp of the signing key provided by the submitting sequencer.
        // That's in order to avoid a series of issues, one of which is that if the submitting sequencer is
        // malicious, it could pretend to be late and use an old timestamp for a specific client that would
        // allow a signature with a key that was already rolled (and potentially even compromised)
        // to be successfully verified here.
        //
        // The downside is that a client could be rolling a key at the same time that it has ongoing requests,
        // which could end up not being verified here (depending on the order events end up with) even though
        // it would be expected that they would. But this is a very edge-case scenario and it is recommended that
        // clients use their new keys for a while before rolling the old key to avoid any issues.
        _ <- signedSubmissionRequest
          .verifySignature(
            sequencingSnapshot,
            submissionRequest.sender,
            HashPurpose.SubmissionRequestSignature,
          )
          .mapK(FutureUnlessShutdown.outcomeK)
          .leftMap[BaseAlarm](error =>
            SequencerError.InvalidSubmissionRequestSignature.Error(
              signedSubmissionRequest,
              error,
              sequencingTimestamp,
              // timestampOfSigningKey is not being used for sig check,
              // but it is still useful to be made part of the error message
              timestampOfSigningKey,
            )
          )
      } yield ()

      alarm.leftMap { a =>
        a.report()
        SubmissionRequestOutcome.discardSubmissionRequest
      }
    }
  }

  private def validateTopologyTimestamp(
      state: BlockUpdateEphemeralState,
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
      sequencerId: SequencerId,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Unit] =
    submissionRequest.topologyTimestamp match {
      case None => EitherT.pure(())
      case Some(topologyTimestamp) =>
        // Silence the warning if we have not delivered anything to the sequencer's topology client.
        val warnIfApproximate =
          state.headCounter(sequencerId).exists(_ > SequencerCounter.Genesis)
        SequencedEventValidator
          .validateTopologyTimestampUS(
            domainSyncCryptoApi,
            topologyTimestamp,
            sequencingTimestamp,
            latestSequencerEventTimestamp,
            protocolVersion,
            warnIfApproximate,
          )
          .bimap(
            rejectInvalidTopologyTimestamp(
              state,
              submissionRequest,
              sequencingTimestamp,
              topologyTimestamp,
            ),
            signingSnapshot => (),
          )
    }

  private def rejectInvalidTopologyTimestamp(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologyTimestamp: CantonTimestamp,
  )(
      reason: TopologyTimestampVerificationError
  )(implicit traceContext: TraceContext): SubmissionRequestOutcome = {
    val rejection = reason match {
      case SequencedEventValidator.TopologyTimestampAfterSequencingTime =>
        SequencerErrors.TopologyTimestampAfterSequencingTimestamp(
          topologyTimestamp,
          sequencingTimestamp,
        )
      case SequencedEventValidator.TopologyTimestampTooOld(_) |
          SequencedEventValidator.NoDynamicDomainParameters(_) =>
        SequencerErrors.TopoologyTimestampTooEarly(
          topologyTimestamp,
          sequencingTimestamp,
        )
    }
    invalidSubmissionRequest(
      state,
      submissionRequest,
      sequencingTimestamp,
      rejection,
    )
  }

  // Performs additional checks and runs the aggregation logic
  // If this succeeds, it will produce a SubmissionRequestOutcome containing DeliverEvents
  private def finalizeProcessing(
      groupToMembers: Map[GroupRecipient, Set[Member]],
      stateAfterTrafficConsume: BlockUpdateEphemeralState,
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
      state: BlockUpdateEphemeralState,
      sequencingSnapshot: SyncCryptoApi,
      signingSnapshotO: Option[SyncCryptoApi],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[
    FutureUnlessShutdown,
    SubmissionRequestOutcome,
    SubmissionRequestValidationResult,
  ] =
    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        SequencerValidations.checkToAtMostOneMediator(submissionRequest),
        (), {
          SequencerError.MultipleMediatorRecipients
            .Error(submissionRequest, sequencingTimestamp)
            .report()
          SubmissionRequestOutcome.discardSubmissionRequest
        },
      )
      aggregationIdO = submissionRequest.aggregationId(domainSyncCryptoApi.pureCrypto)
      aggregationOutcome <- EitherT.fromEither[FutureUnlessShutdown](
        aggregationIdO.traverse { aggregationId =>
          val inFlightAggregation = stateAfterTrafficConsume.inFlightAggregations.get(aggregationId)
          validateAggregationRuleAndUpdateInFlightAggregation(
            stateAfterTrafficConsume,
            submissionRequest,
            sequencingTimestamp,
            aggregationId,
            inFlightAggregation,
          ).map(inFlightAggregationUpdate =>
            (aggregationId, inFlightAggregationUpdate, inFlightAggregation)
          )
        }
      )
      aggregatedBatch = aggregationOutcome.fold(submissionRequest.batch) {
        case (aggregationId, inFlightAggregationUpdate, inFlightAggregation) =>
          val updatedInFlightAggregation = InFlightAggregation.tryApplyUpdate(
            aggregationId,
            inFlightAggregation,
            inFlightAggregationUpdate,
            ignoreInFlightAggregationErrors = false,
          )
          submissionRequest.batch
            .focus(_.envelopes)
            .modify(_.lazyZip(updatedInFlightAggregation.aggregatedSignatures).map {
              (envelope, signatures) => envelope.copy(signatures = signatures)
            })
      }

      topologyTimestampO = submissionRequest.topologyTimestamp
      events =
        (groupToMembers.values.flatten.toSet ++ submissionRequest.batch.allMembers + submissionRequest.sender).toSeq.map {
          member =>
            val groups = groupToMembers.collect {
              case (groupAddress, members) if members.contains(member) => groupAddress
            }.toSet
            val deliver = Deliver.create(
              stateAfterTrafficConsume.tryNextCounter(member),
              sequencingTimestamp,
              domainId,
              Option.when(member == submissionRequest.sender)(submissionRequest.messageId),
              Batch.filterClosedEnvelopesFor(aggregatedBatch, member, groups),
              topologyTimestampO,
              protocolVersion,
            )
            member -> deliver
        }.toMap
      members =
        groupToMembers.values.flatten.toSet ++ submissionRequest.batch.allMembers + submissionRequest.sender
      aggregationUpdate = aggregationOutcome.map {
        case (aggregationId, inFlightAggregationUpdate, _) =>
          aggregationId -> inFlightAggregationUpdate
      }

      // We need to know whether the group of sequencers was addressed in order to update `latestSequencerEventTimestamp`.
      // Simply checking whether this sequencer is within the resulting event recipients opens up
      // the door for a malicious participant to target a single sequencer, which would result in the
      // various sequencers reaching a different value.
      //
      // Currently, the only use cases of addressing a sequencer are:
      //   * via AllMembersOfDomain for topology transactions
      //   * via SequencersOfDomain for traffic control top-up messages
      //
      // Therefore, we check whether this sequencer was addressed via a group address to avoid the above
      // case.
      //
      // NOTE: Pruning concerns
      // ----------------------
      // `latestSequencerEventTimestamp` is relevant for pruning.
      // For the traffic top-ups, we can use the block's last timestamp to signal "safe-to-prune", because
      // the logic to compute the balance based on `latestSequencerEventTimestamp` sits inside the manager
      // and we can make it work together with pruning.
      // For topology, pruning is not yet implemented. However, the logic to compute snapshot timestamps
      // sits outside of the topology processor and so from the topology processor's point of view,
      // `latestSequencerEventTimestamp` should be part of a "safe-to-prune" timestamp calculation.
      //
      // See https://github.com/DACH-NY/canton/pull/17676#discussion_r1515926774
      sequencerEventTimestamp =
        Option.when(isThisSequencerAddressed(groupToMembers))(sequencingTimestamp)

    } yield SubmissionRequestValidationResult(
      stateAfterTrafficConsume,
      SubmissionRequestOutcome(
        events,
        aggregationUpdate,
        signingSnapshotO,
        outcome = SubmissionOutcome.Deliver(
          submissionRequest,
          sequencingTimestamp,
          members,
          aggregatedBatch,
        ),
      ),
      sequencerEventTimestamp,
    )

  private def validateAggregationRuleAndUpdateInFlightAggregation(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      aggregationId: AggregationId,
      inFlightAggregationO: Option[InFlightAggregation],
  )(implicit
      traceContext: TraceContext
  ): Either[SubmissionRequestOutcome, InFlightAggregationUpdate] =
    for {
      inFlightAggregationAndUpdate <- inFlightAggregationO
        .map(_ -> InFlightAggregationUpdate.empty)
        .toRight(())
        .leftFlatMap { _ =>
          val rule = submissionRequest.aggregationRule.getOrElse(
            ErrorUtil.internalError(
              new IllegalStateException(
                "A submission request with an aggregation id must have an aggregation rule"
              )
            )
          )
          validateAggregationRule(state, submissionRequest, sequencingTimestamp, rule)
        }
      (inFlightAggregation, inFlightAggregationUpdate) = inFlightAggregationAndUpdate
      aggregatedSender = AggregatedSender(
        submissionRequest.sender,
        AggregationBySender(
          sequencingTimestamp,
          submissionRequest.batch.envelopes.map(_.signatures),
        ),
      )

      newAggregation <- inFlightAggregation
        .tryAggregate(aggregatedSender)
        .leftMap {
          case InFlightAggregation.AlreadyDelivered(deliveredAt) =>
            val message =
              s"The aggregatable request with aggregation ID $aggregationId was previously delivered at $deliveredAt"
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.AggregateSubmissionAlreadySent(message),
            )
          case InFlightAggregation.AggregationStuffing(_, at) =>
            val message =
              s"The sender ${submissionRequest.sender} previously contributed to the aggregatable submission with ID $aggregationId at $at"
            invalidSubmissionRequest(
              state,
              submissionRequest,
              sequencingTimestamp,
              SequencerErrors.AggregateSubmissionStuffing(message),
            )
        }
      fullInFlightAggregationUpdate = inFlightAggregationUpdate.tryMerge(
        InFlightAggregationUpdate(None, Chain.one(aggregatedSender))
      )
      // If we're not delivering the request to all recipients right now, just send a receipt back to the sender
      _ <- Either.cond(
        newAggregation.deliveredAt.nonEmpty,
        logger.debug(
          s"Aggregation ID $aggregationId has reached its threshold ${newAggregation.rule.threshold} and will be delivered at $sequencingTimestamp."
        ), {
          logger.debug(
            s"Aggregation ID $aggregationId has now ${newAggregation.aggregatedSenders.size} senders aggregated. Threshold is ${newAggregation.rule.threshold.value}."
          )
          val deliverReceiptEvent =
            deliverReceipt(state, submissionRequest, sequencingTimestamp)
          SubmissionRequestOutcome(
            Map(submissionRequest.sender -> deliverReceiptEvent),
            Some(aggregationId -> fullInFlightAggregationUpdate),
            None,
            outcome = SubmissionOutcome.DeliverReceipt(submissionRequest, sequencingTimestamp),
          )
        },
      )
    } yield fullInFlightAggregationUpdate

  private def validateAggregationRule(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  )(implicit
      traceContext: TraceContext
  ): Either[SubmissionRequestOutcome, (InFlightAggregation, InFlightAggregationUpdate)] =
    for {
      _ <- SequencerValidations
        .wellformedAggregationRule(submissionRequest.sender, rule)
        .leftMap(message =>
          invalidSubmissionRequest(
            state,
            submissionRequest,
            sequencingTimestamp,
            SequencerErrors.SubmissionRequestMalformed(message),
          )
        )
      unregisteredEligibleMembers =
        rule.eligibleSenders.filterNot(
          state.registeredMembers.contains
        )
      _ <- Either.cond(
        unregisteredEligibleMembers.isEmpty,
        (),
        // TODO(#14322): review if still applicable and consider an error code (SequencerDeliverError)
        invalidSubmissionRequest(
          state,
          submissionRequest,
          sequencingTimestamp,
          SequencerErrors.SubmissionRequestRefused(
            s"Aggregation rule contains unregistered eligible members: $unregisteredEligibleMembers"
          ),
        ),
      )
      fresh = FreshInFlightAggregation(submissionRequest.maxSequencingTime, rule)
    } yield InFlightAggregation.initial(fresh) -> InFlightAggregationUpdate(
      Some(fresh),
      Chain.empty,
    )

  private def deliverReceipt(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  ): SequencedEvent[ClosedEnvelope] =
    Deliver.create(
      state.tryNextCounter(submissionRequest.sender),
      sequencingTimestamp,
      domainId,
      Some(submissionRequest.messageId),
      Batch.empty(protocolVersion),
      // Since the receipt does not contain any envelopes and does not authenticate the envelopes
      // in any way, there is no point in including a topology timestamp in the receipt,
      // as it cannot be used to prove anything about the submission anyway.
      None,
      protocolVersion,
    )

  private def invalidSubmissionRequest(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencerError: SequencerDeliverError,
  )(implicit traceContext: TraceContext): SubmissionRequestOutcome = {
    val SubmissionRequest(sender, messageId, _, _, _, _, _) = submissionRequest
    logger.debug(
      show"Rejecting submission request $messageId from $sender with error ${sequencerError.code
          .toMsg(sequencerError.cause, correlationId = None, limit = None)}"
    )
    SubmissionRequestOutcome.reject(
      submissionRequest,
      sender,
      DeliverError.create(
        state.tryNextCounter(sender),
        sequencingTimestamp,
        domainId,
        messageId,
        sequencerError,
        protocolVersion,
      ),
    )
  }

  // Off-boarded sequencers may still receive blocks (e.g., BFT sequencers still contribute to ordering for a while
  //  after being deactivated in the Canton topology, specifically until the underlying consensus algorithm
  //  allows them to be also removed from the BFT ordering topology), but they should not be considered addressed,
  //  since they are not active in the Canton topology anymore (i.e., group recipients don't include them).
  private def isThisSequencerAddressed(groupToMembers: Map[GroupRecipient, Set[Member]]): Boolean =
    groupToMembers
      .get(AllMembersOfDomain)
      .exists(_.contains(sequencerId)) ||
      groupToMembers
        .get(SequencersOfDomain)
        .exists(_.contains(sequencerId))
}

private[update] object SubmissionRequestValidator {

  final case class SubmissionRequestValidationResult(
      ephemeralState: BlockUpdateEphemeralState,
      outcome: SubmissionRequestOutcome,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )

  private def updateTrafficState(
      state: BlockUpdateEphemeralState,
      member: Member,
      trafficState: TrafficState,
  ): BlockUpdateEphemeralState =
    state
      .focus(_.trafficState)
      .modify(_.updated(member, trafficState))

  private def isMemberRegistered(state: BlockUpdateEphemeralState)(member: Member): Boolean =
    state.registeredMembers.contains(member)
}
