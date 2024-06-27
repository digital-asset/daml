// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.update

import cats.data.{Chain, EitherT, WriterT}
import cats.implicits.catsStdInstancesForFuture
import cats.kernel.Monoid
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.{DomainSyncCryptoClient, HashPurpose, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.domain.block.data.BlockUpdateEphemeralState
import com.digitalasset.canton.domain.block.update.SubmissionRequestValidator.{
  SequencedEventValidation,
  SubmissionRequestValidationResult,
  TrafficConsumption,
}
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregation.AggregationBySender
import com.digitalasset.canton.domain.sequencing.sequencer.Sequencer.{
  SignedOrderingRequest,
  SignedOrderingRequestOps,
}
import com.digitalasset.canton.domain.sequencing.sequencer.*
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.store.SequencerMemberValidator
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.SequencerRateLimitManager
import com.digitalasset.canton.error.{BaseAlarm, BaseCantonError}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.GroupAddressResolver
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import monocle.Monocle.toAppliedFocusOps

import scala.concurrent.{ExecutionContext, Future}

import SubmissionRequestValidator.*

/** Validates a single [[SubmissionRequest]] within a chunk.
  */
private[update] final class SubmissionRequestValidator(
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    domainSyncCryptoApi: DomainSyncCryptoClient,
    sequencerId: SequencerId,
    rateLimitManager: SequencerRateLimitManager,
    override val loggerFactory: NamedLoggerFactory,
    metrics: SequencerMetrics,
    unifiedSequencer: Boolean,
    memberValidator: SequencerMemberValidator,
)(implicit closeContext: CloseContext)
    extends NamedLogging {

  val trafficControlValidator = new TrafficControlValidator(
    domainId,
    protocolVersion,
    rateLimitManager,
    loggerFactory,
    metrics,
    unifiedSequencer = unifiedSequencer,
  )

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
      signedOrderingRequest: SignedOrderingRequest,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      topologyTimestampError: Option[SequencerDeliverError],
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): FutureUnlessShutdown[SubmissionRequestValidationResult] = {
    val processingResult = performInitialValidations(
      state,
      sequencingTimestamp,
      signedOrderingRequest.signedSubmissionRequest,
      topologyOrSequencingSnapshot,
      topologyTimestampError,
    )
      .flatMap { groupToMembers =>
        finalizeProcessing(
          groupToMembers,
          state,
          sequencingTimestamp,
          signedOrderingRequest.submissionRequest,
        ).mapK(validationFUSK)
          // Use the traffic updated ephemeral state in the response even if the rest of the processing stopped
          .recover { errorSubmissionOutcome =>
            SubmissionRequestValidationResult(
              state,
              errorSubmissionOutcome,
              None,
            )
          }
      }
      .leftMap { errorSubmissionOutcome =>
        SubmissionRequestValidationResult(state, errorSubmissionOutcome, None)
      }
      .merge

    trafficControlValidator.applyTrafficControl(
      processingResult,
      state,
      signedOrderingRequest,
      sequencingTimestamp,
      latestSequencerEventTimestamp,
      signedOrderingRequest.submissionRequest.sender,
    )
  }

  // Below are a 3 functions, each a for-comprehension of EitherT.
  // In each Lefts are used to stop processing the submission request and immediately produce the sequenced events
  // They are split into 3 functions to make it possible to re-use intermediate results (specifically
  // BlockUpdateEphemeralState containing updated traffic states), even if further processing fails.

  // Performs initial validations and resolves groups to members
  private def performInitialValidations(
      state: BlockUpdateEphemeralState,
      sequencingTimestamp: CantonTimestamp,
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      topologyOrSequencingSnapshot: SyncCryptoApi,
      topologyTimestampError: Option[SequencerDeliverError],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): SequencedEventValidation[Map[GroupRecipient, Set[Member]]] = {
    val submissionRequest = signedSubmissionRequest.content
    for {
      isSenderRegistered <- {
        if (unifiedSequencer) {
          EitherT
            .right(
              FutureUnlessShutdown.outcomeF(
                memberValidator.isMemberRegisteredAt(submissionRequest.sender, sequencingTimestamp)
              )
            )
            .mapK(validationFUSK)
        } else {
          EitherT
            .rightT[FutureUnlessShutdown, SubmissionRequestOutcome](
              isMemberRegistered(state)(submissionRequest.sender)
            )
            .mapK(validationFUSK)
        }
      }
      _ <- EitherTUtil
        .condUnitET[FutureUnlessShutdown](
          isSenderRegistered,
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
        .mapK(validationFUSK)
      // Warn if we use an approximate snapshot but only after we've read at least one
      _ <- checkSignatureOnSubmissionRequest(
        signedSubmissionRequest,
        topologyOrSequencingSnapshot,
      ).mapK(validationFUSK)
      // At this point we know the sender has indeed properly signed the submission request
      // so we'll want to run the traffic control logic
      _ <- EitherT.liftF[SequencedEventValidationF, SubmissionRequestOutcome, Unit](
        WriterT.tell(TrafficConsumption(true))
      )
      _ <- EitherT.cond[SequencedEventValidationF](
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
      _ <- checkRecipientsAreKnown(
        state,
        submissionRequest,
        sequencingTimestamp,
      ).mapK(validationFUSK)
      _ <- EitherT.fromEither[SequencedEventValidationF](
        validateTopologyTimestamp(
          state,
          sequencingTimestamp,
          submissionRequest,
          topologyTimestampError,
        )
      )
      // TODO(i17584): revisit the consequences of no longer enforcing that
      //  aggregated submissions with signed envelopes define a topology snapshot
      _ <- validateMaxSequencingTimeForAggregationRule(
        state,
        submissionRequest,
        topologyOrSequencingSnapshot,
        sequencingTimestamp,
      )
        .mapK(validationK)
      _ <- checkClosedEnvelopesSignatures(
        topologyOrSequencingSnapshot,
        submissionRequest,
        sequencingTimestamp,
      ).mapK(validationK)
      groupToMembers <-
        groupRecipientsToMembers(
          state,
          submissionRequest,
          sequencingTimestamp,
          topologyOrSequencingSnapshot,
        ).mapK(validationFUSK)
    } yield groupToMembers
  }

  // TODO(#18401): This method should be harmonized with the GroupAddressResolver
  private def groupRecipientsToMembers(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologyOrSequencingSnapshot: SyncCryptoApi,
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
            topologyOrSequencingSnapshot,
          )
        mediatorGroupsToMembers <-
          expandMediatorGroupRecipients(
            state,
            submissionRequest,
            sequencingTimestamp,
            groupRecipients,
            topologyOrSequencingSnapshot,
          )
        allMembersOfDomainToMembers <-
          expandAllMembersOfDomainGroupRecipients(
            state,
            submissionRequest,
            sequencingTimestamp,
            topologyOrSequencingSnapshot,
            groupRecipients,
          )
        sequencersOfDomainToMembers <-
          expandSequencersOfDomainGroupRecipients(
            state,
            submissionRequest,
            sequencingTimestamp,
            topologyOrSequencingSnapshot,
            groupRecipients,
          )
      } yield participantsOfPartyToMembers ++ mediatorGroupsToMembers ++ sequencersOfDomainToMembers ++ allMembersOfDomainToMembers
  }

  private def expandSequencersOfDomainGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      groupRecipients: Set[GroupRecipient],
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Map[GroupRecipient, Set[Member]]] = {
    val useSequencersOfDomain = groupRecipients.contains(SequencersOfDomain)
    if (useSequencersOfDomain) {
      for {
        sequencers <- EitherT(
          topologyOrSequencingSnapshot.ipsSnapshot
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
      } yield Map((SequencersOfDomain: GroupRecipient) -> sequencers)
    } else
      EitherT.rightT[Future, SubmissionRequestOutcome](Map.empty[GroupRecipient, Set[Member]])
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def expandAllMembersOfDomainGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      topologyOrSequencingSnapshot: SyncCryptoApi,
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
          topologyOrSequencingSnapshot.ipsSnapshot.allMembers()
        )
        _ <- {
          if (unifiedSequencer) {
            EitherT.pure[Future, SubmissionRequestOutcome](())
          } else {
            // this can happen when a
            val nonRegistered = allMembers.filterNot(isMemberRegistered(state))
            EitherTUtil.condUnitET[Future](
              nonRegistered.isEmpty,
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
        }
      } yield Map((AllMembersOfDomain: GroupRecipient, allMembers))
    }
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def expandMediatorGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: SyncCryptoApi,
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
        groups <- topologyOrSequencingSnapshot.ipsSnapshot
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
          val nonRegisteredF = {
            if (unifiedSequencer) {
              (group.active ++ group.passive).forgetNE.parTraverseFilter { member =>
                memberValidator.isMemberRegisteredAt(member, sequencingTimestamp).map {
                  isRegistered => Option.when(!isRegistered)(member)
                }
              }
            } else {
              Future.successful(
                (group.active ++ group.passive).filterNot(isMemberRegistered(state))
              )
            }
          }

          EitherT(
            nonRegisteredF.map { nonRegistered =>
              Either.cond(
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
          )
        }
      } yield GroupAddressResolver.asGroupRecipientsToMembers(groups)
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def expandParticipantGroupRecipients(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      groupRecipients: Set[GroupRecipient],
      topologyOrSequencingSnapshot: SyncCryptoApi,
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
        _ <- topologyOrSequencingSnapshot.ipsSnapshot
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
          topologyOrSequencingSnapshot.ipsSnapshot.activeParticipantsOfParties(parties.toSeq)
        )
      } yield GroupAddressResolver.asGroupRecipientsToMembers(mapping)
  }.mapK(FutureUnlessShutdown.outcomeK)

  private def checkClosedEnvelopesSignatures(
      topologyOrSequencingSnapshot: SyncCryptoApi,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] =
    submissionRequest.batch.envelopes
      .parTraverse_ { closedEnvelope =>
        closedEnvelope.verifySignatures(
          topologyOrSequencingSnapshot,
          submissionRequest.sender,
        )
      }
      .leftMap { error =>
        SequencerError.InvalidEnvelopeSignature
          .Error(
            submissionRequest,
            error,
            sequencingTimestamp,
            topologyOrSequencingSnapshot.ipsSnapshot.timestamp,
          )
          .report()
        SubmissionRequestOutcome.discardSubmissionRequest
      }

  private def validateMaxSequencingTimeForAggregationRule(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      topologyOrSequencingSnapshot: SyncCryptoApi,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] =
    submissionRequest.aggregationRule.traverse_ { _ =>
      for {
        domainParameters <- EitherT(
          topologyOrSequencingSnapshot.ipsSnapshot.findDynamicDomainParameters()
        )
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

  private def checkRecipientsAreKnown(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Unit] = {
    // group addresses checks are covered separately later on
    for {
      unknownRecipients <-
        EitherT
          .right(
            if (unifiedSequencer) {
              submissionRequest.batch.allMembers.toList.parTraverseFilter { member =>
                memberValidator.isMemberRegisteredAt(member, sequencingTimestamp).map {
                  case true => None
                  case false => Some(member)
                }
              }
            } else {
              Future.successful(
                (submissionRequest.batch.allMembers diff state.registeredMembers).toList
              )
            }
          )
          .mapK(FutureUnlessShutdown.outcomeK)
      res <- EitherT.cond[FutureUnlessShutdown](
        unknownRecipients.isEmpty,
        (),
        invalidSubmissionRequest(
          state,
          submissionRequest,
          sequencingTimestamp,
          SequencerErrors.UnknownRecipients(unknownRecipients),
        ),
      )
    } yield res
  }

  private def checkSignatureOnSubmissionRequest(
      signedSubmissionRequest: SignedContent[SubmissionRequest],
      topologyOrSequencingSnapshot: SyncCryptoApi,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, SubmissionRequestOutcome, Unit] = {
    val submissionRequest = signedSubmissionRequest.content

    val alarm = for {
      _ <- signedSubmissionRequest
        .verifySignature(
          topologyOrSequencingSnapshot,
          submissionRequest.sender,
          HashPurpose.SubmissionRequestSignature,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .leftMap[BaseAlarm](error =>
          SequencerError.InvalidSubmissionRequestSignature.Error(
            signedSubmissionRequest,
            error,
            topologyOrSequencingSnapshot.ipsSnapshot.timestamp,
            signedSubmissionRequest.timestampOfSigningKey,
          )
        )
    } yield ()

    alarm.leftMap { a =>
      a.report()
      SubmissionRequestOutcome.discardSubmissionRequest
    }
  }

  private def validateTopologyTimestamp(
      state: BlockUpdateEphemeralState,
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
      topologyTimestampError: Option[SequencerDeliverError],
  )(implicit
      traceContext: TraceContext
  ): Either[SubmissionRequestOutcome, Unit] =
    topologyTimestampError
      .map(
        invalidSubmissionRequest(
          state,
          submissionRequest,
          sequencingTimestamp,
          _,
        )
      )
      .toLeft(())

  // Performs additional checks and runs the aggregation logic
  // If this succeeds, it will produce a SubmissionRequestOutcome containing DeliverEvents
  private def finalizeProcessing(
      groupToMembers: Map[GroupRecipient, Set[Member]],
      state: BlockUpdateEphemeralState,
      sequencingTimestamp: CantonTimestamp,
      submissionRequest: SubmissionRequest,
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
      aggregationOutcome <-
        aggregationIdO
          .traverse { aggregationId =>
            val inFlightAggregation = state.inFlightAggregations.get(aggregationId)
            validateAggregationRuleAndUpdateInFlightAggregation(
              state,
              submissionRequest,
              sequencingTimestamp,
              aggregationId,
              inFlightAggregation,
            ).map(inFlightAggregationUpdate =>
              (aggregationId, inFlightAggregationUpdate, inFlightAggregation)
            )
          }
          .mapK(FutureUnlessShutdown.outcomeK)
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
      members =
        groupToMembers.values.flatten.toSet ++ submissionRequest.batch.allMembers + submissionRequest.sender
      events =
        if (unifiedSequencer) {
          Map.empty[Member, Deliver[ClosedEnvelope]]
        } else {
          members.toSeq.map { member =>
            val groups = groupToMembers.collect {
              case (groupAddress, members) if members.contains(member) => groupAddress
            }.toSet
            val deliver = Deliver.create(
              state.tryNextCounter(member),
              sequencingTimestamp,
              domainId,
              Option.when(member == submissionRequest.sender)(submissionRequest.messageId),
              Batch.filterClosedEnvelopesFor(aggregatedBatch, member, groups),
              topologyTimestampO,
              protocolVersion,
              Option.empty[TrafficReceipt],
            )
            member -> deliver
          }.toMap
        }
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
      state,
      SubmissionRequestOutcome(
        events,
        aggregationUpdate,
        outcome = SubmissionOutcome.Deliver(
          submissionRequest,
          sequencingTimestamp,
          members,
          aggregatedBatch,
          traceContext,
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
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, SubmissionRequestOutcome, InFlightAggregationUpdate] = {
    val rule = submissionRequest.aggregationRule.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          "A submission request with an aggregation id must have an aggregation rule"
        )
      )
    )

    for {
      inFlightAggregationAndUpdate <- inFlightAggregationO match {
        case None =>
          // New aggregation
          validateAggregationRule(state, submissionRequest, sequencingTimestamp, rule).map { _ =>
            val fresh = FreshInFlightAggregation(submissionRequest.maxSequencingTime, rule)
            InFlightAggregation.initial(fresh) -> InFlightAggregationUpdate(
              Some(fresh),
              Chain.empty,
            )
          }

        case Some(inFlightAggregation) =>
          // Existing aggregation
          wellFormedAggregationRule(submissionRequest, rule)
            .map(_ => inFlightAggregation -> InFlightAggregationUpdate.empty)
      }
      (inFlightAggregation, inFlightAggregationUpdate) = inFlightAggregationAndUpdate

      aggregatedSender = AggregatedSender(
        submissionRequest.sender,
        AggregationBySender(
          sequencingTimestamp,
          submissionRequest.batch.envelopes.map(_.signatures),
        ),
      )

      newAggregation <-
        EitherT.fromEither[Future](
          inFlightAggregation
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
        )

      fullInFlightAggregationUpdate = inFlightAggregationUpdate.tryMerge(
        InFlightAggregationUpdate(None, Chain.one(aggregatedSender))
      )
      // If we're not delivering the request to all recipients right now, just send a receipt back to the sender
      _ <- EitherT.cond(
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
            outcome =
              SubmissionOutcome.DeliverReceipt(submissionRequest, sequencingTimestamp, traceContext),
          )
        },
      )
    } yield fullInFlightAggregationUpdate
  }

  private def validateAggregationRule(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      rule: AggregationRule,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] =
    for {
      _ <- wellFormedAggregationRule(submissionRequest, rule)

      unregisteredEligibleMembers <- {
        if (unifiedSequencer) {
          EitherT.right(
            rule.eligibleSenders.forgetNE.parTraverseFilter { member =>
              memberValidator.isMemberRegisteredAt(member, sequencingTimestamp).map {
                case true => None
                case false => Some(member)
              }
            }
          )
        } else {
          EitherT.pure[Future, SubmissionRequestOutcome](
            rule.eligibleSenders.filterNot(
              state.registeredMembers.contains
            )
          )
        }
      }
      _ <- EitherTUtil.condUnitET(
        unregisteredEligibleMembers.isEmpty,
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
    } yield ()

  private def wellFormedAggregationRule(
      submissionRequest: SubmissionRequest,
      rule: AggregationRule,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, SubmissionRequestOutcome, Unit] =
    EitherT.fromEither(
      SequencerValidations
        .wellformedAggregationRule(submissionRequest.sender, rule)
        .leftMap { message =>
          val alarm = SequencerErrors.SubmissionRequestMalformed
            .Error(submissionRequest, message)
          alarm.report()

          SubmissionRequestOutcome.discardSubmissionRequest
        }
    )

  private def deliverReceipt(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
  ): SequencedEvent[ClosedEnvelope] =
    Deliver.create(
      if (unifiedSequencer) {
        SequencerCounter.Genesis
      } else {
        state.tryNextCounter(submissionRequest.sender)
      },
      sequencingTimestamp,
      domainId,
      Some(submissionRequest.messageId),
      Batch.empty(protocolVersion),
      // Since the receipt does not contain any envelopes and does not authenticate the envelopes
      // in any way, there is no point in including a topology timestamp in the receipt,
      // as it cannot be used to prove anything about the submission anyway.
      None,
      protocolVersion,
      Option.empty[TrafficReceipt],
    )

  private def invalidSubmissionRequest(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencerError: SequencerDeliverError,
  )(implicit traceContext: TraceContext): SubmissionRequestOutcome = {
    SubmissionRequestValidator.invalidSubmissionRequest(
      state,
      submissionRequest,
      sequencingTimestamp,
      sequencerError,
      logger,
      domainId,
      protocolVersion,
      unifiedSequencer,
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
  // Effect type used in validation flow - passes along the traffic consumption state that is utilized
  // at the end of the processing to decide on traffic consumption
  type SequencedEventValidationF[A] = WriterT[FutureUnlessShutdown, TrafficConsumption, A]
  // Type of validation methods, uses SequencedEventValidationF as the F of an EitherT
  // This gives us short circuiting semantics while having access to the traffic consumption state at the end
  type SequencedEventValidation[A] = EitherT[SequencedEventValidationF, SubmissionRequestOutcome, A]
  def validationFUSK(implicit executionContext: ExecutionContext) =
    WriterT.liftK[FutureUnlessShutdown, TrafficConsumption]
  def validationK(implicit executionContext: ExecutionContext) =
    FutureUnlessShutdown.outcomeK.andThen(validationFUSK)

  object TrafficConsumption {
    implicit val accumulatedTrafficCostMonoid: Monoid[TrafficConsumption] =
      new Monoid[TrafficConsumption] {
        override def empty: TrafficConsumption = TrafficConsumption(false)
        override def combine(x: TrafficConsumption, y: TrafficConsumption): TrafficConsumption = {
          TrafficConsumption(x.consume || y.consume)
        }
      }
  }

  /** Encodes whether or not traffic should be consumed for the sender for a sequenced event.
    * Currently this is just a boolean but can be expended later to cover more granular cost accumulation depending
    * on delivery, validation etc...
    */
  final case class TrafficConsumption(consume: Boolean)

  final case class SubmissionRequestValidationResult(
      ephemeralState: BlockUpdateEphemeralState,
      outcome: SubmissionRequestOutcome,
      latestSequencerEventTimestamp: Option[CantonTimestamp],
  ) {

    // When we reach the end of the validation, we decide based on the outcome so far
    // if we should try to consume traffic for the event.
    def shouldTryToConsumeTraffic: Boolean = outcome.outcome match {
      // The happy case where the request will be delivered - traffic should be consumed
      case _: SubmissionOutcome.Deliver => true
      // This is a deliver receipt from an aggregated submission - traffic should be consumed
      case _: SubmissionOutcome.DeliverReceipt => true
      // If the submission is rejected, the sender will receive a receipt notifying it of the rejection
      // At this point we assume all rejections can be verified by the sender, and therefore
      // we consume the cost. We can be more granular if necessary by deciding differently based on the
      // actual reason for the rejection
      case _: SubmissionOutcome.Reject => true
      // If the submission is discarded, nothing is sent back to the sender
      // In that case we do not consume anything
      case SubmissionOutcome.Discard => false
    }

    // Wasted traffic is defined as events that have been sequenced but will not be delivered to their
    // recipients. This method return a Some with the reason if the traffic was wasted, None otherwise
    def wastedTrafficReason: Option[String] = outcome.outcome match {
      // Only events that are delivered are not wasted
      case _: SubmissionOutcome.Deliver => None
      case _: SubmissionOutcome.DeliverReceipt => None
      case reject: SubmissionOutcome.Reject =>
        BaseCantonError.statusErrorCodes(reject.error).headOption
      case SubmissionOutcome.Discard => Some("discarded")
    }

    def updateTrafficReceipt(
        sender: Member,
        trafficReceipt: Option[TrafficReceipt],
    ): SubmissionRequestValidationResult = {
      copy(outcome = outcome.updateTrafficReceipt(sender, trafficReceipt))
    }
  }

  private def isMemberRegistered(state: BlockUpdateEphemeralState)(member: Member): Boolean =
    state.registeredMembers.contains(member)

  private[update] def invalidSubmissionRequest(
      state: BlockUpdateEphemeralState,
      submissionRequest: SubmissionRequest,
      sequencingTimestamp: CantonTimestamp,
      sequencerError: SequencerDeliverError,
      logger: TracedLogger,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      unifiedSequencer: Boolean,
  )(implicit traceContext: TraceContext): SubmissionRequestOutcome = {
    val SubmissionRequest(sender, messageId, _, _, _, _, _) = submissionRequest
    logger.debug(
      show"Rejecting submission request $messageId from $sender with error ${sequencerError.code
          .toMsg(sequencerError.cause, correlationId = None, limit = None)}"
    )
    SubmissionRequestOutcome.reject(
      submissionRequest,
      sender,
      DeliverError
        .create(
          if (unifiedSequencer) {
            SequencerCounter.Genesis
          } else {
            state.tryNextCounter(sender)
          },
          sequencingTimestamp,
          domainId,
          messageId,
          sequencerError,
          protocolVersion,
          Option.empty[TrafficReceipt], // Traffic receipt is updated in at the end of processing
        ),
      traceContext,
    )
  }
}
