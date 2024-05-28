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
import com.digitalasset.canton.data.{
  CantonTimestamp,
  Quorum,
  ViewConfirmationParameters,
  ViewPosition,
}
import com.digitalasset.canton.domain.mediator.MediatorVerdict.MediatorApprove
import com.digitalasset.canton.domain.mediator.ResponseAggregation.ViewState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.logging.NamedLoggingContext
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.*
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
  * @param state               If the [[com.digitalasset.canton.protocol.messages.MediatorConfirmationRequest]] has been finalized,
  *                            this will be a `Left` otherwise a `Right` which shows which transaction view hashes are not confirmed yet.
  * @param requestTraceContext We retain the original trace context from the initial transaction confirmation request
  *                            for raising timeouts to help with debugging. this ideally would be the same trace
  *                            context throughout all responses could not be in a distributed setup so this is not
  *                            validated anywhere. Intentionally supplied in a separate parameter list to avoid being
  *                            included in equality checks.
  */
final case class ResponseAggregation[VKEY](
    override val requestId: RequestId,
    override val request: MediatorConfirmationRequest,
    timeout: CantonTimestamp,
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
  override def validateAndProgress(
      responseTimestamp: CantonTimestamp,
      response: ConfirmationResponse,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: NamedLoggingContext,
      ec: ExecutionContext,
  ): Future[Option[ResponseAggregation[VKEY]]] = {
    val ConfirmationResponse(
      requestId,
      sender,
      _viewPositionO,
      localVerdict,
      rootHash,
      confirmingParties,
      _domainId,
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
    val ViewState(consortiumVoting, quorumsState, rejections) =
      stateOfView
    val pendingConfirmingParties = ViewConfirmationParameters.confirmersIdsFromQuorums(quorumsState)
    val (newlyResponded, _) =
      pendingConfirmingParties.partition(party => authorizedParties.contains(party))

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
      localVerdict match {
        case LocalApprove() =>
          val consortiumVotingUpdated =
            newlyResponded.foldLeft(consortiumVoting)((votes, confirmingParty) => {
              votes + (confirmingParty -> votes(confirmingParty).approveBy(sender))
            })
          val newlyRespondedFullVotes = newlyResponded.filter(consortiumVotingUpdated(_).isApproved)
          loggingContext.debug(
            show"$requestId($keyName $viewKey): Received an approval (or reached consortium thresholds) for parties: $newlyRespondedFullVotes"
          )
          val stillPending = pendingConfirmingParties -- newlyRespondedFullVotes
          if (newlyRespondedFullVotes.isEmpty) {
            loggingContext.debug(
              show"$requestId($keyName $viewKey): Awaiting approvals or additional votes for consortiums for $stillPending"
            )
          }

          def updateQuorumsStateWithThresholdUpdate: Seq[Quorum] =
            quorumsState.map { quorum =>
              val contribution = quorum.confirmers
                .filter { case (pId, _) => newlyRespondedFullVotes.contains(pId) }
                .foldLeft(0) { case (acc, (_, weight)) =>
                  acc + weight.unwrap
                }
              // if all thresholds in the list are 0 then all quorums have been met.
              val updatedThreshold = NonNegativeInt
                .create(quorum.threshold.unwrap - contribution)
                .getOrElse(NonNegativeInt.zero)
              val updatedConfirmers = quorum.confirmers -- newlyRespondedFullVotes
              Quorum(updatedConfirmers, updatedThreshold)
            }

          val nextViewState = ViewState(
            consortiumVotingUpdated,
            updateQuorumsStateWithThresholdUpdate,
            rejections,
          )
          val nextStatesOfViews = statesOfViews + (viewKey -> nextViewState)
          Either.cond(
            nextStatesOfViews.values.exists(viewState => !quorumsSatisfied(viewState.quorumsState)),
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
              NonEmpty(List, newRejectionsFullVotes -> rejection, rejections*)

            def updateQuorumsStateWithoutThresholdUpdate(): Seq[Quorum] =
              quorumsState.map { quorum =>
                val updatedConfirmers = quorum.confirmers -- newRejectionsFullVotes
                Quorum(updatedConfirmers, quorum.threshold)
              }

            val nextViewState = ViewState(
              consortiumVotingUpdated,
              updateQuorumsStateWithoutThresholdUpdate(),
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
              show"$requestId($keyName $viewKey): Received a rejection, but awaiting more consortium votes for: $pendingConfirmingParties"
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

  def copy(
      requestId: RequestId = requestId,
      request: MediatorConfirmationRequest = request,
      timeout: CantonTimestamp = timeout,
      version: CantonTimestamp = version,
      state: Either[MediatorVerdict, Map[VKEY, ViewState]] = state,
  ): ResponseAggregation[VKEY] = ResponseAggregation(requestId, request, timeout, version, state)(
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

  override def pretty: Pretty[ResponseAggregation.this.type] = prettyOfClass(
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
      consortiumVoting: Map[
        LfPartyId,
        ConsortiumVotingState,
      ],
      quorumsState: Seq[Quorum],
      rejections: List[(Set[LfPartyId], LocalReject)],
  ) extends PrettyPrinting {

    override def pretty: Pretty[ViewState] = {
      prettyOfClass(
        param("quorumsState", _.quorumsState),
        param("consortiumVoting", _.consortiumVoting),
        param("rejections", _.rejections),
      )
    }
  }

  /** Creates a non-finalized response aggregation from a request.
    */
  def fromRequest(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      timeout: CantonTimestamp,
      topologySnapshot: TopologySnapshot,
  )(implicit
      requestTraceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[ResponseAggregation[?]] =
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
        requestId.unwrap,
        Right(initialState),
      )(requestTraceContext = requestTraceContext)
    }

  private def mkInitialState[K](
      informeesAndConfirmationParamsByViewPosition: Map[K, ViewConfirmationParameters],
      topologySnapshot: TopologySnapshot,
  )(implicit ec: ExecutionContext, tc: TraceContext): Future[Map[K, ViewState]] = {
    informeesAndConfirmationParamsByViewPosition.toSeq
      .parTraverse {
        case (viewKey, viewConfirmationParameters @ ViewConfirmationParameters(_, quorumsState)) =>
          for {
            votingThresholds <- topologySnapshot.consortiumThresholds(
              viewConfirmationParameters.confirmers
            )
          } yield {
            val consortiumVotingState = votingThresholds.map { case (party, threshold) =>
              party -> ConsortiumVotingState(threshold)
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
}
