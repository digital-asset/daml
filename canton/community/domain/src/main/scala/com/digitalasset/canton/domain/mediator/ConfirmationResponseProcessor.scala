// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.{EitherT, OptionT}
import cats.instances.future.*
import cats.syntax.alternative.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, ViewType}
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.TrustLevel
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer
import org.slf4j.event.Level

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** small helper class to extract appropriate data for logging
  *
  * please once we rewrite the mediator event stage stuff, we should
  * clean this up again.
  */
// TODO(#15627) remove me
private[mediator] final case class MediatorResultLog(
    sender: ParticipantId,
    ts: CantonTimestamp,
    approved: Int = 0,
    rejected: Seq[LocalReject] = Seq.empty,
)(val traceContext: TraceContext)
    extends PrettyPrinting {
  override def pretty: Pretty[MediatorResultLog] = prettyNode(
    "ParticipantResponse",
    param("sender", _.sender),
    param("ts", _.ts),
    param("approved", _.approved, _.approved > 0),
    paramIfNonEmpty("rejected", _.rejected),
  )

  def extend(result: LocalVerdict): MediatorResultLog = result match {
    case _: LocalApprove => copy(approved = approved + 1)(traceContext)
    case reject: LocalReject => copy(rejected = rejected :+ reject)(traceContext)
  }

}

/** Scalable service to check the received Stakeholder Trees and Confirmation Responses, derive a verdict and post
  * result messages to stakeholders.
  */
private[mediator] class ConfirmationResponseProcessor(
    domainId: DomainId,
    private val mediatorId: MediatorId,
    verdictSender: VerdictSender,
    crypto: DomainSyncCryptoClient,
    timeTracker: DomainTimeTracker,
    val mediatorState: MediatorState,
    protocolVersion: ProtocolVersion,
    protected val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit ec: ExecutionContext, tracer: Tracer)
    extends NamedLogging
    with Spanning
    with FlagCloseable
    with HasCloseContext {

  private def extractEventsForLogging(
      events: Seq[Traced[MediatorEvent]]
  ): Seq[MediatorResultLog] =
    events
      .collect { case tr @ Traced(MediatorEvent.Response(_, timestamp, response, _)) =>
        (response.message.sender, timestamp, tr.traceContext, response.message.localVerdict)
      }
      .groupBy { case (sender, ts, traceContext, _) => (sender, ts, traceContext) }
      .map { case ((sender, ts, traceContext), results) =>
        results.foldLeft(MediatorResultLog(sender, ts)(traceContext)) {
          case (acc, (_, _, _, result)) =>
            acc.extend(result)
        }
      }
      .toSeq

  /** Handle events for a single request-id.
    * Callers should ensure all events are for the same request and ordered by sequencer time.
    */
  def handleRequestEvents(
      requestId: RequestId,
      events: Seq[Traced[MediatorEvent]],
      callerTraceContext: TraceContext,
  ): HandlerResult = {

    val requestTs = requestId.unwrap
    // // TODO(#15627) clean me up after removing the MediatorStageEvent stuff
    if (logger.underlying.isInfoEnabled()) {
      extractEventsForLogging(events).foreach { result =>
        logger.info(
          show"Phase 5: Received responses for request=${requestId}: $result"
        )(result.traceContext)
      }
    }

    val future = for {
      // FIXME(i12903): do not block if requestId is far in the future
      snapshot <- crypto.ips.awaitSnapshot(requestId.unwrap)(callerTraceContext)

      domainParameters <- snapshot
        .findDynamicDomainParameters()(callerTraceContext)
        .flatMap(_.toFuture(new IllegalStateException(_)))

      participantResponseDeadline <- domainParameters.participantResponseDeadlineForF(requestTs)
      decisionTime <- domainParameters.decisionTimeForF(requestTs)

      _ <- MonadUtil.sequentialTraverse_(events) {
        _.withTraceContext { implicit traceContext =>
          {
            case MediatorEvent.Request(
                  counter,
                  _,
                  request,
                  rootHashMessages,
                  batchAlsoContainsTopologyXTransaction,
                ) =>
              processRequest(
                requestId,
                counter,
                participantResponseDeadline,
                decisionTime,
                request,
                rootHashMessages,
                batchAlsoContainsTopologyXTransaction,
              )
            case MediatorEvent.Response(counter, timestamp, response, recipients) =>
              processResponse(
                timestamp,
                counter,
                participantResponseDeadline,
                decisionTime,
                response,
                recipients,
              )
            case MediatorEvent.Timeout(_counter, timestamp, requestId) =>
              handleTimeout(requestId, timestamp, decisionTime)
          }
        }
      }
    } yield ()
    HandlerResult.synchronous(FutureUnlessShutdown.outcomeF(future))
  }

  @VisibleForTesting
  private[mediator] def handleTimeout(
      requestId: RequestId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    def pendingRequestNotFound: Future[Unit] = {
      logger.debug(
        s"Pending aggregation for request [$requestId] not found. This implies the request has been finalized since the timeout was scheduled."
      )
      Future.unit
    }

    mediatorState.getPending(requestId).fold(pendingRequestNotFound) { responseAggregation =>
      // the event causing the timeout is likely unrelated to the transaction we're actually timing out,
      // so replace the implicit trace context with the original request trace
      implicit val traceContext: TraceContext = responseAggregation.requestTraceContext

      logger
        .info(
          s"Phase 6: Request ${requestId.unwrap}: Timeout in state ${responseAggregation.state} at $timestamp"
        )

      val timeout = responseAggregation.timeout(version = timestamp)
      mediatorState
        .replace(responseAggregation, timeout)
        .semiflatMap { _ =>
          sendResultIfDone(timeout, decisionTime)
        }
        .getOrElse(())
    }
  }

  /** Stores the incoming request in the MediatorStore.
    * Sends a result message if no responses need to be received or if the request is malformed,
    * including if it declares a different mediator.
    */
  @VisibleForTesting
  private[mediator] def processRequest(
      requestId: RequestId,
      counter: SequencerCounter,
      participantResponseDeadline: CantonTimestamp,
      decisionTime: CantonTimestamp,
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      batchAlsoContainsTopologyXTransaction: Boolean,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    withSpan("ConfirmationResponseProcessor.processRequest") { implicit traceContext => span =>
      span.setAttribute("request_id", requestId.toString)
      span.setAttribute("counter", counter.toString)

      for {
        topologySnapshot <- crypto.ips.awaitSnapshot(requestId.unwrap)

        unitOrVerdictO <- validateRequest(
          requestId,
          request,
          rootHashMessages,
          topologySnapshot,
          batchAlsoContainsTopologyXTransaction,
        )

        // Take appropriate actions based on unitOrVerdictO
        _ <- unitOrVerdictO match {
          // Request is well-formed, but not yet finalized
          case Right(()) =>
            val aggregationF = ResponseAggregation.fromRequest(
              requestId,
              request,
              protocolVersion,
              topologySnapshot,
            )

            for {
              aggregation <- aggregationF
              _ <- mediatorState.add(aggregation)
            } yield {
              timeTracker.requestTick(participantResponseDeadline)
              logger.info(
                show"Phase 2: Registered request=${requestId.unwrap} with ${request.informeesAndThresholdByViewHash.size} view(s). Initial state: ${aggregation.showMergedState}"
              )
            }

          // Request is finalized, approve / reject immediately
          case Left(Some(rejection)) =>
            val verdict = rejection.toVerdict(protocolVersion)
            logger.debug(show"$requestId: finalizing immediately with verdict $verdict...")
            for {
              _ <- verdictSender.sendReject(
                requestId,
                Some(request),
                rootHashMessages,
                verdict,
                decisionTime,
              )
              _ <- mediatorState.add(
                FinalizedResponse(requestId, request, requestId.unwrap, verdict)(traceContext)
              )
            } yield ()

          // Discard request
          case Left(None) =>
            logger.debug(show"$requestId: discarding request...")
            Future.successful(None)
        }
      } yield ()
    }
  }

  /** Validate a mediator request
    *
    * Yields `Left(Some(verdict))`, if `request` can already be finalized.
    * Yields `Left(None)`, if `request` should be discarded
    */
  private def validateRequest(
      requestId: RequestId,
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      topologySnapshot: TopologySnapshot,
      batchAlsoContainsTopologyXTransaction: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Either[Option[MediatorVerdict.MediatorReject], Unit]] = (for {
    // Bail out, if this mediator or group is passive, except is the mediator itself is passive in an active group.
    isActive <- EitherT.right[Option[MediatorVerdict.MediatorReject]](
      topologySnapshot.isMediatorActive(mediatorId)
    )
    _ <- EitherTUtil.condUnitET[Future](
      isActive, {
        logger.info(
          show"Ignoring mediator request $requestId because I'm not active or mediator group is not active."
        )
        Option.empty[MediatorVerdict.MediatorReject]
      },
    )

    // Validate activeness of informee participants
    _ <- topologySnapshot
      .allHaveActiveParticipants(request.allInformees)
      .leftMap { informeesNoParticipant =>
        val reject = MediatorError.InvalidMessage.Reject(
          show"Received a mediator request with id $requestId with some informees not being hosted by an active participant: $informeesNoParticipant. Rejecting request...",
          v0.MediatorRejection.Code.InformeesNotHostedOnActiveParticipant,
        )
        reject.log()
        Option(MediatorVerdict.MediatorReject(reject))
      }

    // Validate declared mediator and the group being active
    validMediator <- checkDeclaredMediator(requestId, request, topologySnapshot)

    // Validate root hash messages
    _ <- checkRootHashMessages(
      validMediator,
      requestId,
      request,
      rootHashMessages,
      topologySnapshot,
    )
      .leftMap(Option.apply)

    // Validate minimum threshold
    _ <- EitherT
      .fromEither[Future](validateMinimumThreshold(requestId, request))
      .leftMap(Option.apply)

    // Reject, if the authorized confirming parties cannot attain the threshold
    _ <-
      validateAuthorizedConfirmingParties(requestId, request, topologySnapshot)
        .leftMap(Option.apply)

    // Reject, if the batch also contains a topology transaction
    _ <- EitherTUtil
      .condUnitET(
        !batchAlsoContainsTopologyXTransaction, {
          val rejection = MediatorError.MalformedMessage
            .Reject(
              s"Received a mediator request with id $requestId also containing a topology transaction.",
              v0.MediatorRejection.Code.MissingCode,
            )
            .reported()
          MediatorVerdict.MediatorReject(rejection)
        },
      )
      .leftMap(Option.apply)
  } yield ()).value

  private def checkDeclaredMediator(
      requestId: RequestId,
      request: MediatorRequest,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[Future, Option[MediatorVerdict.MediatorReject], MediatorRef] = {

    def rejectWrongMediator(hint: => String): Option[MediatorVerdict.MediatorReject] = {
      Some(
        MediatorVerdict.MediatorReject(
          MediatorError.MalformedMessage
            .Reject(
              show"Rejecting mediator request with $requestId, mediator ${request.mediator}, topology at ${topologySnapshot.timestamp} due to $hint",
              v0.MediatorRejection.Code.WrongDeclaredMediator,
            )
            .reported()
        )
      )
    }

    (request.mediator match {
      case MediatorRef.Single(declaredMediatorId) =>
        EitherTUtil.condUnitET[Future](
          declaredMediatorId == mediatorId,
          rejectWrongMediator(show"incorrect mediator id"),
        )
      case MediatorRef.Group(declaredMediatorGroup) =>
        for {
          mediatorGroupO <- EitherT.right(
            topologySnapshot.mediatorGroup(declaredMediatorGroup.group)
          )
          mediatorGroup <- EitherT.fromOption[Future](
            mediatorGroupO,
            rejectWrongMediator(show"unknown mediator group"),
          )
          _ <- EitherTUtil.condUnitET[Future](
            mediatorGroup.isActive,
            rejectWrongMediator(show"inactive mediator group"),
          )
          _ <- EitherTUtil.condUnitET[Future](
            mediatorGroup.active.contains(mediatorId) || mediatorGroup.passive.contains(mediatorId),
            rejectWrongMediator(show"this mediator not being part of the mediator group"),
          )

        } yield ()
    }).map(_ => request.mediator)
  }

  private def checkRootHashMessages(
      validMediator: MediatorRef,
      requestId: RequestId,
      request: MediatorRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[Future, MediatorVerdict.MediatorReject, Unit] = {

    // since `checkDeclaredMediator` already validated against the mediatorId we can safely use validMediator = request.mediator
    val (wrongRecipients, correctRecipients) = RootHashMessageRecipients.wrongAndCorrectRecipients(
      rootHashMessages.map(_.recipients),
      validMediator,
    )

    val rootHashMessagesRecipients = correctRecipients
      .flatMap(recipients =>
        recipients
          .collect {
            case m @ MemberRecipient(_: ParticipantId) => m
            case pop: ParticipantsOfParty => pop
          }
      )
    def repeatedMembers(recipients: Seq[Recipient]): Seq[Recipient] = {
      val repeatedRecipientsB = Seq.newBuilder[Recipient]
      val seen = new mutable.HashSet[Recipient]()
      recipients.foreach { recipient =>
        val fresh = seen.add(recipient)
        if (!fresh) repeatedRecipientsB += recipient
      }
      repeatedRecipientsB.result()
    }
    def wrongRootHashes(expectedRootHash: RootHash): Seq[RootHash] =
      rootHashMessages.mapFilter { envelope =>
        val rootHash = envelope.protocolMessage.rootHash
        if (rootHash == expectedRootHash) None else Some(rootHash)
      }.distinct

    def distinctPayloads: Seq[SerializedRootHashMessagePayload] =
      rootHashMessages.map(_.protocolMessage.payload).distinct

    def wrongViewType(expectedViewType: ViewType): Seq[ViewType] =
      rootHashMessages.map(_.protocolMessage.viewType).filterNot(_ == expectedViewType).distinct

    val unitOrRejectionReason = for {
      _ <- EitherTUtil
        .condUnitET[Future](
          wrongRecipients.isEmpty,
          show"Root hash messages with wrong recipients tree: $wrongRecipients",
        )
      repeated = repeatedMembers(rootHashMessagesRecipients)
      _ <- EitherTUtil.condUnitET[Future](
        repeated.isEmpty,
        show"Several root hash messages for recipients: $repeated",
      )
      _ <- EitherTUtil.condUnitET[Future](
        distinctPayloads.sizeCompare(1) <= 0,
        show"Different payloads in root hash messages. Sizes: ${distinctPayloads.map(_.bytes.size).mkShow()}.",
      )
      _ <- request.rootHash match {
        case None =>
          EitherTUtil.condUnitET[Future](
            rootHashMessages.isEmpty,
            show"No root hash messages expected, but received for recipients: $rootHashMessagesRecipients",
          )
        case Some(rootHash) =>
          val wrongHashes = wrongRootHashes(rootHash)
          val wrongViewTypes = wrongViewType(request.viewType)
          val wrongMembersF = RootHashMessageRecipients.wrongMembers(
            rootHashMessagesRecipients,
            request,
            topologySnapshot,
          )
          for {
            _ <- EitherTUtil
              .condUnitET[Future](wrongHashes.isEmpty, show"Wrong root hashes: $wrongHashes")
            wrongMems <- EitherT.liftF(wrongMembersF)
            _ <- EitherTUtil.condUnitET[Future](
              wrongViewTypes.isEmpty,
              show"View types in root hash messages differ from expected view type ${request.viewType}: $wrongViewTypes",
            )
            _ <-
              EitherTUtil.condUnitET[Future](
                wrongMems.missingInformeeParticipants.isEmpty,
                show"Missing root hash message for informee participants: ${wrongMems.missingInformeeParticipants}",
              )
            _ <- EitherTUtil.condUnitET[Future](
              wrongMems.superfluousMembers.isEmpty,
              show"Superfluous root hash message for members: ${wrongMems.superfluousMembers}",
            )
            _ <- EitherTUtil.condUnitET[Future](
              wrongMems.superfluousInformees.isEmpty,
              show"Superfluous root hash message for group addressed parties: ${wrongMems.superfluousInformees}",
            )
          } yield ()
      }
    } yield ()

    unitOrRejectionReason.leftMap { rejectionReason =>
      val rejection = MediatorError.MalformedMessage
        .Reject(
          s"Received a mediator request with id $requestId with invalid root hash messages. Rejecting... Reason: $rejectionReason",
          v0.MediatorRejection.Code.InvalidRootHashMessage,
        )
        .reported()
      MediatorVerdict.MediatorReject(rejection)
    }
  }

  private def validateMinimumThreshold(
      requestId: RequestId,
      request: MediatorRequest,
  )(implicit loggingContext: ErrorLoggingContext): Either[MediatorVerdict.MediatorReject, Unit] = {

    request.informeesAndThresholdByViewPosition.toSeq
      .traverse_ { case (viewPosition, (informees, threshold)) =>
        val minimumThreshold = request.minimumThreshold(informees)
        EitherUtil.condUnitE(
          threshold >= minimumThreshold,
          MediatorVerdict.MediatorReject(
            MediatorError.MalformedMessage
              .Reject(
                s"Received a mediator request with id $requestId having threshold $threshold for transaction view at $viewPosition, which is below the confirmation policy's minimum threshold of $minimumThreshold. Rejecting request...",
                v0.MediatorRejection.Code.ViewThresholdBelowMinimumThreshold,
              )
              .reported()
          ),
        )
      }
  }

  private def validateAuthorizedConfirmingParties(
      requestId: RequestId,
      request: MediatorRequest,
      snapshot: TopologySnapshot,
  )(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[Future, MediatorVerdict.MediatorReject, Unit] = {
    request.informeesAndThresholdByViewPosition.toList
      .parTraverse_ { case (viewPosition, (informees, threshold)) =>
        // sorting parties to get deterministic error messages
        val declaredConfirmingParties =
          informees.collect { case p: ConfirmingParty => p }.toSeq.sortBy(_.party)

        for {
          partitionedConfirmingParties <- EitherT.right[MediatorVerdict.MediatorReject](
            declaredConfirmingParties.parTraverse { p =>
              for {
                canConfirm <- snapshot.isHostedByAtLeastOneParticipantF(
                  p.party,
                  attr =>
                    attr.permission.canConfirm && attr.trustLevel.rank >= p.requiredTrustLevel.rank,
                )
              } yield Either.cond(canConfirm, p, p)
            }
          )

          (unauthorized, authorized) = partitionedConfirmingParties.separate

          _ <- EitherTUtil.condUnitET[Future](
            authorized.map(_.weight.unwrap).sum >= threshold.value, {
              // This partitioning is correct, because a VIP hosted party can always confirm.
              // So if the required trust level is VIP, the problem must be the actual trust level.
              val (insufficientTrustLevel, insufficientPermission) =
                unauthorized.partition(_.requiredTrustLevel == TrustLevel.Vip)
              val insufficientTrustLevelHint =
                if (insufficientTrustLevel.nonEmpty)
                  show"\nParties without VIP participant: ${insufficientTrustLevel.map(_.party)}"
                else ""
              val insufficientPermissionHint =
                if (insufficientPermission.nonEmpty)
                  show"\nParties without participant having permission to confirm: ${insufficientPermission
                      .map(_.party)}"
                else ""

              val authorizedPartiesHint =
                if (authorized.nonEmpty) show"\nAuthorized parties: $authorized" else ""

              val rejection = MediatorError.MalformedMessage
                .Reject(
                  s"Received a mediator request with id $requestId with insufficient authorized confirming parties for transaction view at $viewPosition. " +
                    s"Rejecting request. Threshold: $threshold." +
                    insufficientPermissionHint +
                    insufficientTrustLevelHint +
                    authorizedPartiesHint,
                  v0.MediatorRejection.Code.NotEnoughConfirmingParties,
                )
                .reported()
              MediatorVerdict.MediatorReject(rejection)
            },
          )
        } yield ()
      }
  }

  def processResponse(
      ts: CantonTimestamp,
      counter: SequencerCounter,
      participantResponseDeadline: CantonTimestamp,
      decisionTime: CantonTimestamp,
      signedResponse: SignedProtocolMessage[MediatorResponse],
      recipients: Recipients,
  )(implicit traceContext: TraceContext): Future[Unit] =
    withSpan("ConfirmationResponseProcessor.processResponse") { implicit traceContext => span =>
      span.setAttribute("timestamp", ts.toString)
      span.setAttribute("counter", counter.toString)
      val response = signedResponse.message

      (for {
        snapshot <- OptionT.liftF(crypto.awaitSnapshot(response.requestId.unwrap))
        _ <- signedResponse
          .verifySignature(snapshot, response.sender)
          .leftMap(err =>
            MediatorError.MalformedMessage
              .Reject(
                s"$domainId (requestId: $ts): invalid signature from ${response.sender} with $err"
              )
              .report()
          )
          .toOption
        _ <-
          if (signedResponse.domainId == domainId) OptionT.some[Future](())
          else {
            MediatorError.MalformedMessage
              .Reject(
                s"Request ${response.requestId}, sender ${response.sender}: Discarding mediator response for wrong domain ${signedResponse.domainId}"
              )
              .report()
            OptionT.none[Future, Unit]
          }

        _ <-
          if (ts <= participantResponseDeadline) OptionT.some[Future](())
          else {
            logger.warn(
              s"Response ${ts} is too late as request ${response.requestId} has already exceeded the participant response deadline [$participantResponseDeadline]"
            )
            OptionT.none[Future, Unit]
          }

        responseAggregation <- mediatorState.fetch(response.requestId).orElse {
          // This can happen after a fail-over or as part of an attack.
          val cause =
            s"Received a mediator response at $ts by ${response.sender} with an unknown request id ${response.requestId}. Discarding response..."
          val error = MediatorError.InvalidMessage.Reject(cause)
          error.log()

          OptionT.none
        }

        _ <- {
          if (
            // Note: This check relies on mediator trusting its sequencer
            // and the sequencer performing validation `checkFromParticipantToAtMostOneMediator`
            // in the `BlockUpdateGenerator`
            // TODO(i13849): Review the case below: the check in sequencer has to be made stricter (not to allow such messages from other than participant domain nodes)
            recipients.allRecipients.sizeCompare(1) == 0 &&
            recipients.allRecipients.contains(responseAggregation.request.mediator.toRecipient)
          ) {
            OptionT.some[Future](())
          } else {
            MediatorError.MalformedMessage
              .Reject(
                s"Request ${response.requestId}, sender ${response.sender}: Discarding mediator response with wrong recipients ${recipients}, expected ${responseAggregation.request.mediator.toRecipient}"
              )
              .report()
            OptionT.none[Future, Unit]
          }
        }
        nextResponseAggregation <- OptionT(
          responseAggregation.validateAndProgress(ts, response, snapshot.ipsSnapshot)
        )
        _unit <- mediatorState.replace(responseAggregation, nextResponseAggregation)
        _ <- OptionT.some(
          // we can send the result asynchronously, as there is no need to reply in
          // order and there is no need to guarantee delivery of verdicts
          doNotAwait(
            response.requestId,
            sendResultIfDone(nextResponseAggregation, decisionTime),
          )
        )
      } yield ()).value.map(_ => ())
    }

  /** This method is here to allow overriding the async send & determinism in tests
    */
  protected def doNotAwait(requestId: RequestId, f: => Future[Any])(implicit
      tc: TraceContext
  ): Future[Unit] = {
    FutureUtil.doNotAwait(
      performUnlessClosingF("send-result-if-done")(
        f
      ).onShutdown(()),
      s"send-result-if-done failed for request $requestId",
      level = Level.WARN,
    )
    Future.unit
  }

  private def sendResultIfDone(
      responseAggregation: ResponseAggregation[?],
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] =
    responseAggregation.asFinalized(protocolVersion) match {
      case Some(finalizedResponse) =>
        logger.info(
          s"Phase 6: Finalized request=${finalizedResponse.requestId} with verdict ${finalizedResponse.verdict}"
        )
        verdictSender.sendResult(
          finalizedResponse.requestId,
          finalizedResponse.request,
          finalizedResponse.verdict,
          decisionTime,
        )
      case None =>
        /* no op */
        Future.unit
    }
}
