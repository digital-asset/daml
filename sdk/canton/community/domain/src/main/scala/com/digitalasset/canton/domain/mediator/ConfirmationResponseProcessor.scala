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
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, DomainSyncCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, ConfirmingParty, ViewType}
import com.digitalasset.canton.domain.mediator.store.MediatorState
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
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

/** Scalable service to validate the received MediatorConfirmationRequests and ConfirmationResponses,
  * derive a verdict, and send ConfirmationResultMessages to informee participants.
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
      .collect { case tr @ Traced(MediatorEvent.Response(_, timestamp, response, _, _)) =>
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
      snapshot <- crypto.ips.awaitSnapshotUS(requestId.unwrap)(callerTraceContext)

      domainParameters <- FutureUnlessShutdown.outcomeF(
        snapshot
          .findDynamicDomainParameters()(callerTraceContext)
          .flatMap(_.toFuture(new IllegalStateException(_)))
      )

      participantResponseDeadline <- FutureUnlessShutdown.outcomeF(
        domainParameters.participantResponseDeadlineForF(requestTs)
      )
      decisionTime <- FutureUnlessShutdown.outcomeF(domainParameters.decisionTimeForF(requestTs))

      _ <- MonadUtil.sequentialTraverse_[FutureUnlessShutdown, Traced[MediatorEvent]](events) {
        _.withTraceContext[FutureUnlessShutdown[Unit]] { implicit traceContext =>
          {
            case MediatorEvent.Request(
                  counter,
                  _,
                  request,
                  rootHashMessages,
                  batchAlsoContainsTopologyTransaction,
                ) =>
              processRequest(
                requestId,
                counter,
                participantResponseDeadline,
                decisionTime,
                request,
                rootHashMessages,
                batchAlsoContainsTopologyTransaction,
              )
            case MediatorEvent.Response(
                  counter,
                  timestamp,
                  response,
                  topologyTimestamp,
                  recipients,
                ) =>
              processResponse(
                timestamp,
                counter,
                participantResponseDeadline,
                decisionTime,
                response,
                topologyTimestamp,
                recipients,
              )
            case MediatorEvent.Timeout(_counter, timestamp, requestId) =>
              handleTimeout(requestId, timestamp, decisionTime)
          }
        }
      }
    } yield ()
    HandlerResult.synchronous(future)
  }

  @VisibleForTesting
  private[mediator] def handleTimeout(
      requestId: RequestId,
      timestamp: CantonTimestamp,
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def pendingRequestNotFound: FutureUnlessShutdown[Unit] = {
      logger.debug(
        s"Pending aggregation for request [$requestId] not found. This implies the request has been finalized since the timeout was scheduled."
      )
      FutureUnlessShutdown.unit
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
      request: MediatorConfirmationRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      batchAlsoContainsTopologyTransaction: Boolean,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    withSpan("TransactionConfirmationResponseProcessor.processRequest") {
      implicit traceContext => span =>
        span.setAttribute("request_id", requestId.toString)
        span.setAttribute("counter", counter.toString)

        for {
          snapshot <- crypto.awaitSnapshotUS(requestId.unwrap)

          unitOrVerdictO <- FutureUnlessShutdown.outcomeF {
            validateRequest(
              requestId,
              request,
              rootHashMessages,
              snapshot,
              batchAlsoContainsTopologyTransaction,
            )
          }

          // Take appropriate actions based on unitOrVerdictO
          _ <- unitOrVerdictO match {
            // Request is well-formed, but not yet finalized
            case Right(()) =>
              val aggregationF = FutureUnlessShutdown.outcomeF(
                ResponseAggregation.fromRequest(
                  requestId,
                  request,
                  snapshot.ipsSnapshot,
                )
              )

              for {
                aggregation <- aggregationF
                _ <- mediatorState.add(aggregation)
              } yield {
                timeTracker.requestTick(participantResponseDeadline)
                logger.info(
                  show"Phase 2: Registered request=${requestId.unwrap} with ${request.informeesAndThresholdByViewPosition.size} view(s). Initial state: ${aggregation.showMergedState}"
                )
              }

            // Request is finalized, approve / reject immediately
            case Left(Some(rejection)) =>
              val verdict = rejection.toVerdict(protocolVersion)
              logger.debug(show"$requestId: finalizing immediately with verdict $verdict...")
              for {
                _ <-
                  verdictSender.sendReject(
                    requestId,
                    Some(request),
                    rootHashMessages,
                    verdict,
                    decisionTime,
                  )
                _ <-
                  mediatorState.add(
                    FinalizedResponse(requestId, request, requestId.unwrap, verdict)(traceContext)
                  )
              } yield ()

            // Discard request
            case Left(None) =>
              logger.debug(show"$requestId: discarding request...")
              FutureUnlessShutdown.pure(None)
          }
        } yield ()
    }
  }

  /** Validate a mediator confirmation request
    *
    * Yields `Left(Some(verdict))`, if `request` can already be finalized.
    * Yields `Left(None)`, if `request` should be discarded
    */
  private def validateRequest(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      rootHashMessages: Seq[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      snapshot: DomainSnapshotSyncCryptoApi,
      batchAlsoContainsTopologyTransaction: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[Either[Option[MediatorVerdict.MediatorReject], Unit]] = {
    val topologySnapshot = snapshot.ipsSnapshot
    (for {
      // Bail out, if this mediator or group is passive, except is the mediator itself is passive in an active group.
      isActive <- EitherT.right[Option[MediatorVerdict.MediatorReject]](
        topologySnapshot.isMediatorActive(mediatorId)
      )
      _ <- EitherTUtil.condUnitET[Future](
        isActive, {
          logger.info(
            show"Ignoring mediator confirmation request $requestId because I'm not active or mediator group is not active."
          )
          Option.empty[MediatorVerdict.MediatorReject]
        },
      )

      // Validate signature of submitting participant
      _ <- snapshot
        .verifySignature(
          request.rootHash.unwrap,
          request.submittingParticipant,
          request.submittingParticipantSignature,
        )
        .leftMap { err =>
          val reject = MediatorError.MalformedMessage.Reject(
            show"Received a mediator confirmation request with id $requestId from ${request.submittingParticipant} with an invalid signature. Rejecting request.\nDetailed error: $err"
          )
          reject.log()
          Option(MediatorVerdict.MediatorReject(reject))
        }

      // Validate activeness of informee participants
      _ <- topologySnapshot
        .allHaveActiveParticipants(request.allInformees)
        .leftMap { informeesNoParticipant =>
          val reject = MediatorError.InvalidMessage.Reject(
            show"Received a mediator confirmation request with id $requestId with some informees not being hosted by an active participant: $informeesNoParticipant. Rejecting request..."
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
          !batchAlsoContainsTopologyTransaction, {
            val rejection = MediatorError.MalformedMessage
              .Reject(
                s"Received a mediator confirmation request with id $requestId also containing a topology transaction."
              )
              .reported()
            MediatorVerdict.MediatorReject(rejection)
          },
        )
        .leftMap(Option.apply)
    } yield ()).value
  }

  private def checkDeclaredMediator(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
      topologySnapshot: TopologySnapshot,
  )(implicit
      loggingContext: ErrorLoggingContext
  ): EitherT[Future, Option[MediatorVerdict.MediatorReject], MediatorGroupRecipient] = {

    def rejectWrongMediator(hint: => String): Option[MediatorVerdict.MediatorReject] = {
      Some(
        MediatorVerdict.MediatorReject(
          MediatorError.MalformedMessage
            .Reject(
              show"Rejecting mediator confirmation request with $requestId, mediator ${request.mediator}, topology at ${topologySnapshot.timestamp} due to $hint"
            )
            .reported()
        )
      )
    }

    for {
      mediatorGroupO <- EitherT.right(
        topologySnapshot.mediatorGroup(request.mediator.group)(loggingContext)
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
    } yield request.mediator
  }

  private def checkRootHashMessages(
      validMediator: MediatorGroupRecipient,
      requestId: RequestId,
      request: MediatorConfirmationRequest,
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
      wrongHashes = wrongRootHashes(request.rootHash)
      wrongViewTypes = wrongViewType(request.viewType)
      wrongMembersF = RootHashMessageRecipients.wrongMembers(
        rootHashMessagesRecipients,
        request,
        topologySnapshot,
      )(ec, loggingContext)
      _ <- for {
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
    } yield ()

    unitOrRejectionReason.leftMap { rejectionReason =>
      val rejection = MediatorError.MalformedMessage
        .Reject(
          s"Received a mediator confirmation request with id $requestId with invalid root hash messages. Rejecting... Reason: $rejectionReason"
        )
        .reported()(loggingContext)
      MediatorVerdict.MediatorReject(rejection)
    }
  }

  private def validateMinimumThreshold(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
  )(implicit loggingContext: ErrorLoggingContext): Either[MediatorVerdict.MediatorReject, Unit] = {

    request.informeesAndThresholdByViewPosition.toSeq
      .traverse_ { case (viewPosition, (informees, threshold)) =>
        val minimumThreshold = request.minimumThreshold(informees)
        EitherUtil.condUnitE(
          threshold >= minimumThreshold,
          MediatorVerdict.MediatorReject(
            MediatorError.MalformedMessage
              .Reject(
                s"Received a mediator confirmation request with id $requestId having threshold $threshold for transaction view at $viewPosition, which is below the confirmation policy's minimum threshold of $minimumThreshold. Rejecting request..."
              )
              .reported()
          ),
        )
      }
  }

  private def validateAuthorizedConfirmingParties(
      requestId: RequestId,
      request: MediatorConfirmationRequest,
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
            snapshot
              .isHostedByAtLeastOneParticipantF(
                declaredConfirmingParties.map(_.party).toSet,
                (p, attr) => attr.permission.canConfirm,
              )(loggingContext)
              .map { hostedConfirmingParties =>
                declaredConfirmingParties.map(cp =>
                  Either.cond(hostedConfirmingParties.contains(cp.party), cp, cp)
                )
              }
          )

          (unauthorized, authorized) = partitionedConfirmingParties.separate

          _ <- EitherTUtil.condUnitET[Future](
            authorized.map(_.weight.unwrap).sum >= threshold.value, {
              val insufficientPermissionHint =
                if (unauthorized.nonEmpty)
                  show"\nParties without participant having permission to confirm: ${unauthorized
                      .map(_.party)}"
                else ""

              val authorizedPartiesHint =
                if (authorized.nonEmpty) show"\nAuthorized parties: $authorized" else ""

              val rejection = MediatorError.MalformedMessage
                .Reject(
                  s"Received a mediator confirmation request with id $requestId with insufficient authorized confirming parties for transaction view at $viewPosition. " +
                    s"Rejecting request. Threshold: $threshold." +
                    insufficientPermissionHint +
                    authorizedPartiesHint
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
      signedResponse: SignedProtocolMessage[ConfirmationResponse],
      topologyTimestamp: Option[CantonTimestamp],
      recipients: Recipients,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    withSpan("TransactionConfirmationResponseProcessor.processResponse") {
      implicit traceContext => span =>
        span.setAttribute("timestamp", ts.toString)
        span.setAttribute("counter", counter.toString)
        val response = signedResponse.message

        (for {
          snapshot <- OptionT.liftF(crypto.awaitSnapshotUS(response.requestId.unwrap))
          _ <- signedResponse
            .verifySignature(snapshot, response.sender)
            .mapK(FutureUnlessShutdown.outcomeK)
            .leftMap(err =>
              MediatorError.MalformedMessage
                .Reject(
                  s"$domainId (timestamp: $ts): invalid signature from ${response.sender} with $err"
                )
                .report()
            )
            .toOption
          _ <-
            if (signedResponse.domainId == domainId) OptionT.some[FutureUnlessShutdown](())
            else {
              MediatorError.MalformedMessage
                .Reject(
                  s"Request ${response.requestId}, sender ${response.sender}: Discarding confirmation response for wrong domain ${signedResponse.domainId}"
                )
                .report()
              OptionT.none[FutureUnlessShutdown, Unit]
            }

          _ <-
            if (ts <= participantResponseDeadline) OptionT.some[FutureUnlessShutdown](())
            else {
              logger.warn(
                s"Response ${ts} is too late as request ${response.requestId} has already exceeded the participant response deadline [$participantResponseDeadline]"
              )
              OptionT.none[FutureUnlessShutdown, Unit]
            }
          _ <- {
            // To ensure that a mediator group address is resolved in the same way as for the request
            // we require that the topology timestamp on the response submission request is set to the
            // request's sequencing time. The sequencer communicates this timestamp to the client
            // via the timestamp of signing key.
            if (topologyTimestamp.contains(response.requestId.unwrap))
              OptionT.some[FutureUnlessShutdown](())
            else {
              MediatorError.MalformedMessage
                .Reject(
                  s"Request ${response.requestId}, sender ${response.sender}: Discarding confirmation response because the topology timestamp is not set to the request id [$topologyTimestamp]"
                )
                .report()
              OptionT.none[FutureUnlessShutdown, Unit]
            }
          }

          responseAggregation <- mediatorState.fetch(response.requestId).orElse {
            // This can happen after a fail-over or as part of an attack.
            val cause =
              s"Received a confirmation response at $ts by ${response.sender} with an unknown request id ${response.requestId}. Discarding response..."
            val error = MediatorError.InvalidMessage.Reject(cause)
            error.log()

            OptionT.none[FutureUnlessShutdown, ResponseAggregator]
          }

          _ <- {
            if (
              // Note: This check relies on mediator trusting its sequencer
              // and the sequencer performing validation `checkToAtMostOneMediator`
              // in the `BlockUpdateGenerator`
              recipients.allRecipients.sizeCompare(1) == 0 &&
              recipients.allRecipients.contains(responseAggregation.request.mediator)
            ) {
              OptionT.some[FutureUnlessShutdown](())
            } else {
              MediatorError.MalformedMessage
                .Reject(
                  s"Request ${response.requestId}, sender ${response.sender}: Discarding confirmation response with wrong recipients ${recipients}, expected ${responseAggregation.request.mediator}"
                )
                .report()
              OptionT.none[FutureUnlessShutdown, Unit]
            }
          }
          nextResponseAggregation <- OptionT(
            FutureUnlessShutdown.outcomeF(
              responseAggregation.validateAndProgress(ts, response, snapshot.ipsSnapshot)
            )
          )
          _unit <- mediatorState.replace(responseAggregation, nextResponseAggregation)
          _ <- OptionT.some[FutureUnlessShutdown](
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
  protected def doNotAwait(requestId: RequestId, f: => FutureUnlessShutdown[Any])(implicit
      tc: TraceContext
  ): Future[Unit] = {
    FutureUtil.doNotAwaitUnlessShutdown(
      performUnlessClosingUSF("send-result-if-done")(f),
      s"send-result-if-done failed for request $requestId",
      level = Level.WARN,
    )
    Future.unit
  }

  private def sendResultIfDone(
      responseAggregation: ResponseAggregation[?],
      decisionTime: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
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
        FutureUnlessShutdown.unit
    }
}
