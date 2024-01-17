// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.Show.Shown
import cats.data.OptionT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, Informee, ViewPosition}
import com.digitalasset.canton.domain.mediator.MediatorVerdict.MediatorApprove
import com.digitalasset.canton.domain.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{
  LocalApprove,
  LocalReject,
  LocalVerdict,
  MediatorRequest,
  MediatorResponse,
}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}

/** Aggregates the responses for a request that the mediator has processed so far.
  *
  * @param state If the [[com.digitalasset.canton.protocol.messages.MediatorRequest]] has been finalized,
  *              this will be a `Left` otherwise a `Right` which shows which transaction view hashes are not confirmed yet.
  *
  * @param requestTraceContext We retain the original trace context from the initial confirmation request
  * for raising timeouts to help with debugging. this ideally would be the same trace
  * context throughout all responses could not be in a distributed setup so this is not
  * validated anywhere. Intentionally supplied in a separate parameter list to avoid being
  * included in equality checks.
  */
final case class ResponseAggregation[VKEY](
    override val requestId: RequestId,
    override val request: MediatorRequest,
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

  /** Record the additional confirmation response received. */
  override def validateAndProgress(
      responseTimestamp: CantonTimestamp,
      response: MediatorResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): Future[Option[ResponseAggregation[VKEY]]] = {
    val MediatorResponse(
      requestId,
      sender,
      _viewPositionO,
      localVerdict,
      rootHashO,
      confirmingParties,
      _domainId,
    ) = response
    val viewKeyO = ViewKey[VKEY].keyOfResponse(response)

    (for {
      viewPositionsAndParties <- validateResponse(
        viewKeyO,
        rootHashO,
        responseTimestamp,
        sender,
        localVerdict,
        topologySnapshot,
        confirmingParties,
      )

      // This comes last so that the validation also runs for responses to finalized requests. Benefits:
      // - more exhaustive security alerts
      // - avoid race conditions in security tests
      statesOfViews <- OptionT.fromOption[Future](state.leftMap { s => // move down
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

  protected def progressView(
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
    val ViewState(pendingConfirmingParties, consortiumVoting, distanceToThreshold, rejections) =
      stateOfView
    val (newlyResponded, _) =
      pendingConfirmingParties.partition(cp => authorizedParties.contains(cp.party))

    loggingContext.debug(
      show"$requestId($keyName $viewKey): Received verdict $localVerdict for pending parties $newlyResponded by participant $sender. "
    )
    val alreadyResponded = authorizedParties -- newlyResponded.map(_.party)
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
      localVerdict match {
        case LocalApprove() =>
          val consortiumVotingUpdated =
            newlyResponded.foldLeft(consortiumVoting)((votes, confirmingParty) => {
              votes + (confirmingParty.party -> votes(confirmingParty.party).approveBy(sender))
            })
          val newlyRespondedFullVotes = newlyResponded.filter { case ConfirmingParty(party, _, _) =>
            consortiumVotingUpdated(party).isApproved
          }
          loggingContext.debug(
            show"$requestId($keyName $viewKey): Received an approval (or reached consortium thresholds) for parties: $newlyRespondedFullVotes"
          )
          val contribution = newlyRespondedFullVotes.foldLeft(0)(_ + _.weight.unwrap)
          val stillPending = pendingConfirmingParties -- newlyRespondedFullVotes
          if (newlyRespondedFullVotes.isEmpty) {
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Awaiting approvals or additional votes for consortiums for $stillPending"
            )
          }
          val nextViewState = ViewState(
            stillPending,
            consortiumVotingUpdated,
            distanceToThreshold - contribution,
            rejections,
          )
          val nextStatesOfViews = statesOfViews + (viewKey -> nextViewState)
          Either.cond(
            nextStatesOfViews.values.exists(_.distanceToThreshold > 0),
            nextStatesOfViews,
            MediatorApprove,
          )

        case rejection: LocalReject =>
          val consortiumVotingUpdated =
            authorizedParties.foldLeft(consortiumVoting)((votes, party) => {
              votes + (party -> votes(party).rejectBy(sender))
            })
          val newRejectionsFullVotes = authorizedParties.filter(party => {
            consortiumVotingUpdated(party).isRejected
          })
          if (newRejectionsFullVotes.nonEmpty) {
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Received a rejection (or reached consortium thresholds) for parties: $newRejectionsFullVotes"
            )
            val nextRejections =
              NonEmpty(List, (newRejectionsFullVotes -> rejection), rejections *)
            val stillPending =
              pendingConfirmingParties.filterNot(cp => newRejectionsFullVotes.contains(cp.party))
            val nextViewState = ViewState(
              stillPending,
              consortiumVotingUpdated,
              distanceToThreshold,
              nextRejections,
            )
            Either.cond(
              nextViewState.distanceToThreshold <= nextViewState.totalAvailableWeight,
              statesOfViews + (viewKey -> nextViewState),
              // TODO(#5337): Don't discard the rejection reasons of the other views.
              MediatorVerdict.ParticipantReject(nextRejections),
            )
          } else {
            // no full votes, need more confirmations (only in consortium case)
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Received a rejection, but awaiting more consortium votes for: $pendingConfirmingParties"
            )
            val nextViewState = ViewState(
              pendingConfirmingParties,
              consortiumVotingUpdated,
              distanceToThreshold,
              rejections,
            )
            Right(statesOfViews + (viewKey -> nextViewState))
          }
      }
    }
  }

  def copy(
      requestId: RequestId = requestId,
      request: MediatorRequest = request,
      version: CantonTimestamp = version,
      state: Either[MediatorVerdict, Map[VKEY, ViewState]] = state,
  ): ResponseAggregation[VKEY] = ResponseAggregation(requestId, request, version, state)(
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
          if (viewState.distanceToThreshold > 0) viewState.pendingConfirmingParties.map(_.party)
          else Seq.empty
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

  override def pretty: Pretty[ResponseAggregation.this.type] = prettyOfClass(
    param("id", _.requestId),
    param("request", _.request),
    param("version", _.version),
    param("state", _.state),
  )

  def showMergedState: Shown = state.showMerged
}

object ResponseAggregation {

  final case class ConsortiumVotingState(
      threshold: PositiveInt = PositiveInt.one,
      approvals: Set[ParticipantId] = Set.empty,
      rejections: Set[ParticipantId] = Set.empty,
  ) extends PrettyPrinting {
    def approveBy(participant: ParticipantId): ConsortiumVotingState = {
      this.copy(approvals = this.approvals + participant)
    }

    def rejectBy(participant: ParticipantId): ConsortiumVotingState = {
      this.copy(rejections = this.rejections + participant)
    }

    def isApproved: Boolean = approvals.size >= threshold.value

    def isRejected: Boolean = rejections.size >= threshold.value

    override def pretty: Pretty[ConsortiumVotingState] = {
      prettyOfClass(
        param("consortium-threshold", _.threshold, _.threshold.value > 1),
        paramIfNonEmpty("approved by participants", _.approvals),
        paramIfNonEmpty("rejected by participants", _.rejections),
      )
    }
  }

  final case class ViewState(
      pendingConfirmingParties: Set[ConfirmingParty],
      consortiumVoting: Map[
        LfPartyId,
        ConsortiumVotingState,
      ], // pendingConfirmingParties is always a subset of consortiumVoting.keys()
      distanceToThreshold: Int,
      rejections: List[(Set[LfPartyId], LocalReject)],
  ) extends PrettyPrinting {

    lazy val totalAvailableWeight: Int = pendingConfirmingParties.map(_.weight.unwrap).sum

    override def pretty: Pretty[ViewState] = {
      prettyOfClass(
        param("distanceToThreshold", _.distanceToThreshold),
        param("pendingConfirmingParties", _.pendingConfirmingParties),
        param("consortiumVoting", _.consortiumVoting),
        param("rejections", _.rejections),
      )
    }
  }

  /** Creates a non-finalized response aggregation from a request.
    */
  def fromRequest(
      requestId: RequestId,
      request: MediatorRequest,
      topologySnapshot: TopologySnapshot,
  )(implicit
      requestTraceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[ResponseAggregation[?]] =
    for {
      initialState <- mkInitialState(
        request.informeesAndThresholdByViewPosition,
        topologySnapshot,
      )
    } yield {
      ResponseAggregation[ViewPosition](
        requestId,
        request,
        requestId.unwrap,
        Right(initialState),
      )(requestTraceContext = requestTraceContext)
    }

  private def mkInitialState[K](
      informeesAndThresholdByView: Map[K, (Set[Informee], NonNegativeInt)],
      topologySnapshot: TopologySnapshot,
  )(implicit ec: ExecutionContext): Future[Map[K, ViewState]] = {
    informeesAndThresholdByView.toSeq
      .parTraverse { case (viewKey, (informees, threshold)) =>
        val confirmingParties = informees.collect { case cp: ConfirmingParty => cp }
        for {
          votingThresholds <- topologySnapshot.consortiumThresholds(confirmingParties.map(_.party))
        } yield {
          val consortiumVotingState = votingThresholds.map { case (party, threshold) =>
            (party -> ConsortiumVotingState(threshold))
          }
          viewKey -> ViewState(
            confirmingParties,
            consortiumVotingState,
            threshold.unwrap,
            rejections = Nil,
          )
        }
      }
      .map(_.toMap)
  }
}
