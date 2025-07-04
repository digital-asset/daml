// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.Show.Shown
import cats.data.OptionT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  Quorum,
  ViewConfirmationParameters,
  ViewPosition,
}
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.synchronizer.mediator.MediatorVerdict.MediatorApprove
import com.digitalasset.canton.synchronizer.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.immutable.SortedSet
import scala.concurrent.ExecutionContext

/** Aggregates the responses for a request that the mediator has processed so far.
  *
  * @param state
  *   If the [[com.digitalasset.canton.protocol.messages.MediatorConfirmationRequest]] has been
  *   finalized, this will be a `Left` otherwise a `Right` which shows which transaction view hashes
  *   are not confirmed yet.
  * @param requestTraceContext
  *   We retain the original trace context from the initial transaction confirmation request for
  *   raising timeouts to help with debugging. this ideally would be the same trace context
  *   throughout all responses could not be in a distributed setup so this is not validated
  *   anywhere. Intentionally supplied in a separate parameter list to avoid being included in
  *   equality checks.
  */
final case class ResponseAggregation[VKEY](
    override val requestId: RequestId,
    override val request: MediatorConfirmationRequest,
    timeout: CantonTimestamp,
    decisionTime: CantonTimestamp,
    override val version: CantonTimestamp,
    state: Either[MediatorVerdict, Map[VKEY, ViewState]],
)(val requestTraceContext: TraceContext)(implicit val viewKeyOps: ViewKey[VKEY])
    extends ResponseAggregator
    with PrettyPrinting {

  override type VKey = VKEY

  override def isFinalized: Boolean = state.isLeft

  def asFinalized(protocolVersion: ProtocolVersion): Option[FinalizedResponse] =
    state.swap.toOption.map { verdict =>
      FinalizedResponse(
        requestId,
        request,
        version,
        verdict.toVerdict(protocolVersion),
      )(requestTraceContext)
    }

  private def quorumsSatisfied(
      quorums: Seq[Quorum]
  ): Boolean =
    quorums.forall(_.threshold.unwrap == 0)

  private def quorumsCanBeSatisfied(
      quorums: Seq[Quorum]
  ): Boolean =
    quorums.forall(quorum =>
      quorum.threshold.unwrap <= quorum.confirmers.map { case (_, weight) => weight.unwrap }.sum
    )

  /** Record the additional confirmation response received. */
  override protected[synchronizer] def validateAndProgressInternal(
      responseTimestamp: CantonTimestamp,
      response: ConfirmationResponse,
      rootHash: RootHash,
      sender: ParticipantId,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[Option[ResponseAggregation[VKEY]]] = {
    val ConfirmationResponse(
      _viewPositionO,
      localVerdict,
      confirmingParties,
    ) = response
    val viewKeyO = ViewKey[VKEY].keyOfResponse(response)

    (for {
      viewPositionsAndParties <- validateResponse(
        viewKeyO,
        rootHash,
        responseTimestamp,
        sender,
        localVerdict,
        topologySnapshot,
        confirmingParties,
      )

      // This comes last so that the validation also runs for responses to finalized requests. Benefits:
      // - more exhaustive security alerts
      // - avoid race conditions in security tests
      statesOfViews <- OptionT.fromOption[FutureUnlessShutdown](state.leftMap { s => // move down
        loggingContext.info(
          s"Request ${requestId.unwrap} has already been finalized with verdict $s before response $responseTimestamp from $sender with $localVerdict for view $viewKeyO arrives"
        )
      }.toOption)
    } yield {
      val updatedState = MonadUtil.foldLeftM(statesOfViews, viewPositionsAndParties)(
        progressView(_, _, sender, localVerdict)
      )
      copy(version = responseTimestamp, state = updatedState)
    }).value
  }

  private def progressView(
      statesOfViews: Map[VKEY, ViewState],
      viewKeyAndParties: (VKEY, Set[LfPartyId]),
      sender: ParticipantId,
      localVerdict: LocalVerdict,
  )(implicit
      loggingContext: NamedLoggingContext
  ): Either[MediatorVerdict, Map[VKEY, ViewState]] = {
    val keyName = ViewKey[VKEY].name
    val (viewKey, authorizedParties) = viewKeyAndParties
    val stateOfView = statesOfViews.getOrElse(
      viewKey,
      ErrorUtil.internalError(
        new IllegalArgumentException(
          s"The $keyName $viewKey is not covered by the request"
        )
      ),
    )
    val ViewState(consortiumVoting, quorumsState, rejections) = stateOfView
    val pendingConfirmingParties = ViewConfirmationParameters.confirmersIdsFromQuorums(quorumsState)
    val newlyResponded = pendingConfirmingParties.intersect(authorizedParties)

    loggingContext.debug(
      show"$requestId($keyName $viewKey): Received verdict $localVerdict for pending parties $newlyResponded by participant $sender. "
    )
    val alreadyResponded = authorizedParties -- newlyResponded
    // Because some of the responders might have had some other participant already confirmed on their behalf
    // we ignore those responders
    if (alreadyResponded.nonEmpty)
      loggingContext.debug(s"Ignored responses from $alreadyResponded")

    if (newlyResponded.isEmpty) {
      loggingContext.debug(
        s"Nothing to do upon response from $sender for $requestId($keyName $viewKey) because no new responders"
      )
      Either.right[MediatorVerdict, Map[VKEY, ViewState]](statesOfViews)
    } else {

      val consortiumVotingUpdated: Map[LfPartyId, ResponseAggregation.ConsortiumVotingState] =
        updateConsortiumVoting(localVerdict, newlyResponded, consortiumVoting, sender)

      localVerdict match {
        case approve: LocalApprove =>
          val newlyRespondedFullVotes = newlyResponded.filter(consortiumVotingUpdated(_).isApproved)

          val stillPending = pendingConfirmingParties -- newlyRespondedFullVotes
          if (newlyRespondedFullVotes.isEmpty) {
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Received ${approve.name} for $newlyResponded, but awaiting additional votes for consortiums for $stillPending"
            )
          } else {
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Received ${approve.name} and reached consortium thresholds for parties: $newlyRespondedFullVotes"
            )
            if (stillPending.nonEmpty)
              loggingContext.debug(
                show"$requestId($keyName $viewKey): Awaiting approvals or additional votes for consortiums for $stillPending"
              )
          }

          val quorumUpdated = updateQuorumState(localVerdict, newlyRespondedFullVotes, quorumsState)

          val nextViewState = ViewState(
            consortiumVotingUpdated,
            quorumUpdated,
            rejections,
          )

          val nextStatesOfViews = statesOfViews + (viewKey -> nextViewState)
          Either.cond(
            nextStatesOfViews.values.exists(viewState => !quorumsSatisfied(viewState.quorumsState)),
            nextStatesOfViews,
            MediatorApprove,
          )

        case nonPositiveVerdict: NonPositiveLocalVerdict =>
          val newRejectionsFullVotes =
            authorizedParties.filter(party => consortiumVotingUpdated(party).isRejected)
          val quorumUpdated = updateQuorumState(localVerdict, newRejectionsFullVotes, quorumsState)

          if (newRejectionsFullVotes.nonEmpty) {
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Received a ${nonPositiveVerdict.name} for $newlyResponded and reached consortium thresholds for parties: $newRejectionsFullVotes"
            )

            // TODO(i26453): Consider whether we want to keep all rejections or only the latest one.
            // Right now we only keep the last rejection reason that makes the party reject fully.
            // It can be either a LocalReject or a LocalAbstain.

            val nextRejections =
              NonEmpty(List, newRejectionsFullVotes -> nonPositiveVerdict, rejections*)

            val nextViewState = ViewState(
              consortiumVotingUpdated,
              quorumUpdated,
              nextRejections,
            )
            Either.cond(
              quorumsCanBeSatisfied(nextViewState.quorumsState),
              statesOfViews + (viewKey -> nextViewState),
              // TODO(#5337): Don't discard the rejection reasons of the other views.
              MediatorVerdict.ParticipantReject(nextRejections),
            )
          } else {
            // no full votes, need more confirmations (only in consortium case)
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Received a ${nonPositiveVerdict.name} for $newlyResponded , but awaiting more consortium votes for: $pendingConfirmingParties"
            )
            val nextViewState = ViewState(
              consortiumVotingUpdated,
              quorumsState,
              rejections,
            )
            Right(statesOfViews + (viewKey -> nextViewState))
          }

      }
    }
  }

  private def updateConsortiumVoting(
      localVerdict: LocalVerdict,
      newlyResponded: Set[LfPartyId],
      consortiumVoting: Map[LfPartyId, ResponseAggregation.ConsortiumVotingState],
      sender: ParticipantId,
  ): Map[LfPartyId, ResponseAggregation.ConsortiumVotingState] =
    localVerdict match {
      case LocalApprove() =>
        newlyResponded.foldLeft(consortiumVoting) { (votes, confirmingParty) =>
          votes + (confirmingParty -> votes(confirmingParty).approveBy(sender))
        }
      case _: LocalReject =>
        newlyResponded.foldLeft(consortiumVoting) { (votes, confirmingParty) =>
          votes + (confirmingParty -> votes(confirmingParty).rejectBy(sender))
        }
      case _: LocalAbstain =>
        newlyResponded.foldLeft(consortiumVoting) { (votes, confirmingParty) =>
          votes + (confirmingParty -> votes(confirmingParty).abstainBy(sender))
        }
    }

  private def updateQuorumState(
      localVerdict: LocalVerdict,
      newlyRespondedFullVotes: Set[LfPartyId],
      quorumsState: Seq[Quorum],
  ): Seq[Quorum] = localVerdict match {
    case LocalApprove() =>
      quorumsState.map { quorum =>
        val contribution = quorum.confirmers.collect {
          case (pId, weight) if newlyRespondedFullVotes.contains(pId) => weight.unwrap
        }.sum

        // if all thresholds in the list are 0 then all quorums have been met.
        val updatedThreshold = NonNegativeInt
          .create(quorum.threshold.unwrap - contribution)
          .getOrElse(NonNegativeInt.zero)
        val updatedConfirmers = quorum.confirmers -- newlyRespondedFullVotes
        Quorum(updatedConfirmers, updatedThreshold)
      }
    case _: NonPositiveLocalVerdict =>
      quorumsState.map { quorum =>
        val updatedConfirmers = quorum.confirmers -- newlyRespondedFullVotes
        Quorum(updatedConfirmers, quorum.threshold)
      }
  }

  def copy(
      requestId: RequestId = requestId,
      request: MediatorConfirmationRequest = request,
      timeout: CantonTimestamp = timeout,
      decisionTime: CantonTimestamp = decisionTime,
      version: CantonTimestamp = version,
      state: Either[MediatorVerdict, Map[VKEY, ViewState]] = state,
  ): ResponseAggregation[VKEY] =
    ResponseAggregation(requestId, request, timeout, decisionTime, version, state)(
      requestTraceContext
    )

  def withVersion(version: CantonTimestamp): ResponseAggregation[VKEY] =
    copy(version = version)

  def timeout(
      version: CantonTimestamp
  )(implicit loggingContext: NamedLoggingContext): ResponseAggregation[VKEY] = state match {
    case Right(statesOfView) =>
      val unresponsiveParties = statesOfView
        .flatMap { case (_, viewState) =>
          if (!quorumsSatisfied(viewState.quorumsState))
            viewState.quorumsState.flatMap(_.confirmers.keys).toSet
          else Set.empty
        }
        // Sort and deduplicate the parties so that multiple mediator replicas generate the same rejection reason
        .to(SortedSet)
      copy(
        version = version,
        state = Left(
          MediatorVerdict.MediatorReject(
            MediatorError.Timeout.Reject(unresponsiveParties = unresponsiveParties.mkString(","))
          )
        ),
      )
    case Left(MediatorVerdict.MediatorReject(_: MediatorError.Timeout.Reject)) =>
      this
    case Left(verdict) =>
      ErrorUtil.invalidState(
        s"Cannot time-out request $requestId because it was previously finalized with verdict $verdict"
      )
  }

  override protected def pretty: Pretty[ResponseAggregation.this.type] = prettyOfClass(
    param("id", _.requestId),
    param("request", _.request),
    param("timeout", _.timeout),
    param("version", _.version),
    param("state", _.state),
  )

  def showMergedState: Shown = state.showMerged
}

object ResponseAggregation {

  final case class ConsortiumVotingState(
      threshold: PositiveInt,
      hostingParticipantsCount: PositiveInt,
      approvals: Set[ParticipantId],
      rejections: Set[ParticipantId],
      abstains: Set[ParticipantId],
  ) extends PrettyPrinting {

    def approveBy(participant: ParticipantId): ConsortiumVotingState =
      this.copy(approvals = this.approvals + participant)

    def rejectBy(participant: ParticipantId): ConsortiumVotingState =
      this.copy(rejections = this.rejections + participant)

    def abstainBy(participant: ParticipantId): ConsortiumVotingState =
      this.copy(abstains = this.abstains + participant)

    def isApproved: Boolean = approvals.sizeIs >= threshold.value

    def isRejected: Boolean = rejections.sizeIs >= threshold.value || !isThresholdIsReachable

    private def isThresholdIsReachable: Boolean =
      rejections.size + abstains.size + threshold.value <= hostingParticipantsCount.value

    override protected def pretty: Pretty[ConsortiumVotingState] =
      prettyOfClass(
        param("consortium-threshold", _.threshold, _.threshold.value > 1),
        param(
          "number-of-hosting-participants",
          _.hostingParticipantsCount,
          _.hostingParticipantsCount.value > 1,
        ),
        paramIfNonEmpty("approved by participants", _.approvals),
        paramIfNonEmpty("rejected by participants", _.rejections),
        paramIfNonEmpty("abstains by participants", _.abstains),
      )
  }

  object ConsortiumVotingState {
    def initialValue(
        threshold: PositiveInt,
        numberOfHostingParticipants: PositiveInt,
    ): ConsortiumVotingState =
      ConsortiumVotingState(
        threshold,
        numberOfHostingParticipants,
        approvals = Set.empty,
        rejections = Set.empty,
        abstains = Set.empty,
      )

    @VisibleForTesting
    def withDefaultValues(
        threshold: PositiveInt = PositiveInt.one,
        numberOfHostingParticipants: Option[PositiveInt] = None,
        approvals: Set[ParticipantId] = Set.empty,
        rejections: Set[ParticipantId] = Set.empty,
        abstains: Set[ParticipantId] = Set.empty,
    ): ConsortiumVotingState =
      ConsortiumVotingState(
        threshold,
        numberOfHostingParticipants.getOrElse(threshold),
        approvals = approvals,
        rejections = rejections,
        abstains = abstains,
      )

  }

  final case class ViewState(
      consortiumVoting: Map[
        LfPartyId,
        ConsortiumVotingState,
      ],
      quorumsState: Seq[Quorum],
      rejections: List[(Set[LfPartyId], NonPositiveLocalVerdict)],
  ) extends PrettyPrinting {

    override protected def pretty: Pretty[ViewState] =
      prettyOfClass(
        param("quorumsState", _.quorumsState),
        param("consortiumVoting", _.consortiumVoting),
        param("rejections", _.rejections),
      )
  }

  /** Creates a non-finalized response aggregation from a request.
    */
  def fromRequest(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      timeout: CantonTimestamp,
      decisionTime: CantonTimestamp,
      topologySnapshot: TopologySnapshot,
  )(implicit
      requestTraceContext: TraceContext,
      ec: ExecutionContext,
  ): FutureUnlessShutdown[ResponseAggregation[?]] =
    for {
      initialState <- mkInitialState(
        request.informeesAndConfirmationParamsByViewPosition,
        topologySnapshot,
      )
    } yield {
      ResponseAggregation[ViewPosition](
        requestId,
        request,
        timeout,
        decisionTime,
        requestId.unwrap,
        Right(initialState),
      )(requestTraceContext = requestTraceContext)
    }

  private def mkInitialState[K](
      informeesAndConfirmationParamsByViewPosition: Map[K, ViewConfirmationParameters],
      topologySnapshot: TopologySnapshot,
  )(implicit ec: ExecutionContext, tc: TraceContext): FutureUnlessShutdown[Map[K, ViewState]] =
    informeesAndConfirmationParamsByViewPosition.toSeq
      .parTraverse {
        case (viewKey, viewConfirmationParameters @ ViewConfirmationParameters(_, quorumsState)) =>
          for {
            confirmersPartyInfo <- topologySnapshot.activeParticipantsOfPartiesWithInfo(
              viewConfirmationParameters.confirmers.toSeq
            )
          } yield {
            val consortiumVotingState = confirmersPartyInfo.map { case (party, info) =>
              val hostingParticipantWithConfirmationPermissionCount = info.participants.count {
                case (_, attributes) => attributes.canConfirm
              }
              party -> ConsortiumVotingState.initialValue(
                info.threshold,
                PositiveInt
                  .create(hostingParticipantWithConfirmationPermissionCount)
                  .getOrElse(PositiveInt.one),
              )
            }
            viewKey -> ViewState(
              consortiumVotingState,
              quorumsState,
              rejections = Nil,
            )
          }
      }
      .map(_.toMap)
}
