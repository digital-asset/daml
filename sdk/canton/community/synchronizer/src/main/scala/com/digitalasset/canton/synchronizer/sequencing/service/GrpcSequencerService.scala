// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.CantonRequireTypes.String73
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeNumeric, PositiveInt}
import com.digitalasset.canton.config.{PositiveFiniteDuration, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.*
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.protocol.DynamicSynchronizerParametersLookup
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.protocol.SynchronizerParametersLookup.SequencerSynchronizerParameters
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.{
  DownloadTopologyStateForInitHashRequest,
  DownloadTopologyStateForInitHashResponse,
  GetTimeRequest,
  GetTimeResponse,
}
import com.digitalasset.canton.sequencing.SequencedSerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.Sequencer
import com.digitalasset.canton.synchronizer.sequencer.config.SequencerParameters
import com.digitalasset.canton.synchronizer.sequencer.errors.SequencerError
import com.digitalasset.canton.synchronizer.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.synchronizer.sequencing.service.GrpcSequencerService.{
  SignedAcknowledgeRequest,
  WrappedAcknowledgeRequest,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStateForInitializationService,
}
import com.digitalasset.canton.tracing.{
  SerializableTraceContext,
  TraceContext,
  TraceContextGrpc,
  Traced,
}
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{EitherTUtil, FutureUtil, MaxBytesToDecompress, RateLimiter}
import com.digitalasset.canton.version.ProtocolVersion
import com.github.blemale.scaffeine.{Cache, Scaffeine}
import com.google.common.annotations.VisibleForTesting
import io.grpc.Status
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.pekko.Done
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer, UniqueKillSwitch}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Authenticate the current user can perform an operation on behalf of the given member */
private[synchronizer] trait AuthenticationCheck {

  /** Can the authenticated member perform an action on behalf of the provided member. Return a left
    * with a user presentable error message if not. Right if the operation can continue.
    */
  def authenticate(member: Member, authenticatedMember: Option[Member]): Either[String, Unit]
  def lookupCurrentMember(): Option[Member]
}

object AuthenticationCheck {

  @VisibleForTesting
  private[service] trait MatchesAuthenticatedMember extends AuthenticationCheck {

    override def authenticate(
        member: Member,
        authenticatedMember: Option[Member],
    ): Either[String, Unit] = {
      // fwiw I don't think it will be possible to reach this check for being the right member
      // if there is no member authenticated, but prepare some text for that scenario just in case.
      val authenticatedMemberText =
        authenticatedMember.map(_.toString).getOrElse("[unauthenticated]")

      Either.cond(
        authenticatedMember.contains(member),
        (),
        s"Authenticated member $authenticatedMemberText just tried to use sequencer on behalf of $member without permission",
      )
    }
  }

  /** Check the member matches member available from the GRPC context */
  object AuthenticationToken extends MatchesAuthenticatedMember {
    override def lookupCurrentMember(): Option[Member] =
      IdentityContextHelper.getCurrentStoredMember
  }

  /** No authentication check is performed */
  object Disabled extends AuthenticationCheck {

    override def authenticate(
        member: Member,
        authenticatedMember: Option[Member],
    ): Either[String, Unit] = Either.unit

    override def lookupCurrentMember(): Option[Member] = None
  }
}

object GrpcSequencerService {

  def apply(
      sequencer: Sequencer,
      metrics: SequencerMetrics,
      authenticationCheck: AuthenticationCheck,
      clock: Clock,
      synchronizerParamsLookup: DynamicSynchronizerParametersLookup[
        SequencerSynchronizerParameters
      ],
      parameters: SequencerParameters,
      protocolVersion: ProtocolVersion,
      topologyStateForInitializationService: TopologyStateForInitializationService,
      loggerFactory: NamedLoggerFactory,
      acknowledgementsConflateWindow: Option[PositiveFiniteDuration] = None,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): GrpcSequencerService =
    new GrpcSequencerService(
      sequencer,
      metrics,
      loggerFactory,
      authenticationCheck,
      new SubscriptionPool[GrpcManagedSubscription[?]](
        clock,
        metrics,
        parameters.maxSubscriptionsPerMember,
        parameters.processingTimeouts,
        loggerFactory,
      ),
      new DirectSequencerSubscriptionFactory(
        sequencer,
        parameters.processingTimeouts,
        loggerFactory,
      ),
      synchronizerParamsLookup,
      parameters,
      topologyStateForInitializationService,
      protocolVersion,
      acknowledgementsConflateWindow = acknowledgementsConflateWindow,
    )

  private sealed trait WrappedAcknowledgeRequest extends Product with Serializable {
    def unwrap: AcknowledgeRequest
  }
  // TODO(#18401): Inline SignedContent[AcknowledgeRequest] as now this is the only subtype of WrappedAcknowledgeRequest
  private final case class SignedAcknowledgeRequest(
      signedRequest: SignedContent[AcknowledgeRequest]
  ) extends WrappedAcknowledgeRequest {
    override def unwrap: AcknowledgeRequest = signedRequest.content
  }
}

/** Service providing a GRPC connection to the [[sequencer.Sequencer]] instance.
  *
  * @param sequencer
  *   The underlying sequencer implementation
  */
class GrpcSequencerService(
    sequencer: Sequencer,
    metrics: SequencerMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    authenticationCheck: AuthenticationCheck,
    subscriptionPool: SubscriptionPool[GrpcManagedSubscription[?]],
    directSequencerSubscriptionFactory: DirectSequencerSubscriptionFactory,
    synchronizerParamsLookup: DynamicSynchronizerParametersLookup[SequencerSynchronizerParameters],
    parameters: SequencerParameters,
    topologyStateForInitializationService: TopologyStateForInitializationService,
    protocolVersion: ProtocolVersion,
    maxItemsInTopologyResponse: PositiveInt = PositiveInt.tryCreate(100),
    acknowledgementsConflateWindow: Option[PositiveFiniteDuration] = None,
)(implicit ec: ExecutionContext, materializer: Materializer)
    extends v30.SequencerServiceGrpc.SequencerService
    with NamedLogging
    with FlagCloseable {

  override protected val timeouts: ProcessingTimeout = parameters.processingTimeouts

  private val rates = new TrieMap[ParticipantId, RateLimiter]()
  private val acknowledgementConflate: Option[Cache[Member, Unit]] =
    acknowledgementsConflateWindow.map { conflateWindow =>
      Scaffeine()
        .expireAfterWrite(conflateWindow.asFiniteApproximation)
        .build[Member, Unit]()
    }

  def membersWithActiveSubscriptions: Seq[Member] =
    subscriptionPool.activeSubscriptions().map(_.member)
  def disconnectMember(member: Member)(implicit traceContext: TraceContext): Unit =
    subscriptionPool.closeSubscriptions(member)
  def disconnectAllMembers()(implicit traceContext: TraceContext): Unit =
    subscriptionPool.closeAllSubscriptions()

  override def sendAsync(
      requestP: v30.SendAsyncRequest
  ): Future[v30.SendAsyncResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // This has to run at the beginning, because it reads from a thread-local.
    val senderFromMetadata = authenticationCheck.lookupCurrentMember()
    def parseAndValidate(
        maxRequestSize: MaxRequestSize
    ): Either[SequencerDeliverError, SignedContent[SubmissionRequest]] = for {
      signedContent <- SignedContent
        .fromByteString(protocolVersion, requestP.signedSubmissionRequest)
        .leftMap(requestDeserializationError(_, maxRequestSize))
      signedSubmissionRequest <- signedContent
        .deserializeContent(
          SubmissionRequest
            .fromByteString(
              protocolVersion,
              MaxBytesToDecompress(maxRequestSize.value),
            )
        )
        .leftMap(requestDeserializationError(_, maxRequestSize))
      _ <- validateSubmissionRequest(
        requestP.serializedSize,
        signedSubmissionRequest.content,
        senderFromMetadata,
      )
    } yield signedSubmissionRequest

    lazy val sendET = for {
      synchronizerParameters <- EitherT
        .right[SequencerDeliverError](
          synchronizerParamsLookup.getApproximateOrDefaultValue(
            warnOnUsingDefaults(senderFromMetadata)
          )
        )
      request <- EitherT.fromEither[FutureUnlessShutdown](
        parseAndValidate(synchronizerParameters.maxRequestSize)
      )
      _ <- checkRate(request.content)
      _ <- sequencer.sendAsyncSigned(request)
    } yield v30.SendAsyncResponse()

    val resET = synchronizeWithClosing(functionFullName)(sendET.leftMap { err =>
      logger.info(s"Rejecting submission request by $senderFromMetadata with $err")
      err.toCantonRpcError
    })
    CantonGrpcUtil.mapErrNewEUS(resET)
  }

  private def requestDeserializationError(
      error: ProtoDeserializationError,
      maxRequestSize: MaxRequestSize,
  )(implicit traceContext: TraceContext): SequencerDeliverError = {
    val message = error match {
      case ProtoDeserializationError.MaxBytesToDecompressExceeded(message) =>
        val alarm =
          SequencerError.MaxRequestSizeExceeded.Error(message, maxRequestSize)
        alarm.report()
        message
      case error: ProtoDeserializationError =>
        logger.warn(error.toString)
        error.toString
    }
    SequencerErrors.SubmissionRequestMalformed.Error("", "", message)
  }

  private def validateSubmissionRequest(
      requestSize: Int,
      request: SubmissionRequest,
      memberFromMetadata: Option[Member],
  )(implicit traceContext: TraceContext): Either[SequencerDeliverError, Unit] = {
    val messageId = request.messageId

    def refuseUnless(
        sender: Member
    )(condition: Boolean, message: => String): Either[SequencerDeliverError, Unit] =
      Either.cond(condition, (), refuse(messageId.toProtoPrimitive, sender)(message))

    def invalidUnless(
        sender: Member
    )(condition: Boolean, message: => String): Either[SequencerDeliverError, Unit] =
      Either.cond(condition, (), invalid(messageId.toProtoPrimitive, sender)(message))

    val sender = request.sender
    for {
      // do the security checks
      _ <- authenticationCheck
        .authenticate(sender, memberFromMetadata)
        .leftMap(err =>
          refuse(messageId.toProtoPrimitive, sender)(s"$sender is not authorized to send: $err")
        )

      _ = {
        val envelopesCount = request.batch.envelopesCount
        logger.info(
          s"'$sender' sends request with id '$messageId' of size $requestSize bytes with $envelopesCount envelopes."
        )
      }

      // check everything else
      _ <- invalidUnless(sender)(
        request.batch.envelopes.forall(_.recipients.allRecipients.nonEmpty),
        "Batch contains envelope without recipients.",
      )
      _ <- invalidUnless(sender)(
        request.batch.envelopes.forall(!_.bytes.isEmpty),
        "Batch contains envelope without content.",
      )
      _ <- refuseUnless(sender)(
        SubmissionRequestValidations.checkToAtMostOneMediator(request),
        "Batch contains multiple mediators as recipients.",
      )
      _ <- request.aggregationRule.traverse_(validateAggregationRule(sender, messageId, _))
    } yield {
      metrics.publicApi.bytesProcessed.mark(requestSize.toLong)(MetricsContext.Empty)
      metrics.publicApi.messagesProcessed.mark()
      if (TimeProof.isTimeProofSubmission(request)) metrics.publicApi.timeRequests.mark()

      ()
    }
  }

  private def validateAggregationRule(
      sender: Member,
      messageId: MessageId,
      aggregationRule: AggregationRule,
  )(implicit traceContext: TraceContext): Either[SequencerDeliverError, Unit] =
    SubmissionRequestValidations
      .wellformedAggregationRule(sender, aggregationRule)
      .leftMap(message => invalid(messageId.toProtoPrimitive, sender)(message))

  private def invalid(messageIdP: String, sender: Member)(
      message: String
  )(implicit traceContext: TraceContext): SequencerDeliverError = {
    val truncatedMessageId = MessageId(String73.createWithTruncation(messageIdP))
    invalid(truncatedMessageId, sender)(message)
  }

  private def invalid(messageId: MessageId, sender: Member)(
      message: String
  )(implicit traceContext: TraceContext): SequencerDeliverError =
    SequencerErrors.SubmissionRequestMalformed
      .Error(sender.toProtoPrimitive, messageId.unwrap, message)
      .reported()

  private def refuse(messageIdP: String, sender: Member)(
      message: String
  )(implicit traceContext: TraceContext): SequencerDeliverError = {
    logger.warn(s"Request '$messageIdP' from '$sender' refused: $message")
    SequencerErrors.SubmissionRequestRefused(
      s"Request '$messageIdP' from '$sender' refused: $message"
    )
  }

  private def checkRate(
      request: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] = {
    val sender = request.sender
    def checkRate(
        participantId: ParticipantId,
        confirmationRequestsMaxRate: NonNegativeInt,
    ): Either[SequencerDeliverError, Unit] = {
      val limiter = getOrUpdateRateLimiter(participantId, confirmationRequestsMaxRate)
      Either.cond(
        limiter.checkAndUpdateRate(),
        (), {
          val message = f"Submission rate exceeds rate limit of $confirmationRequestsMaxRate/s."
          logger.info(
            f"Request '${request.messageId}' from '$sender' refused: $message"
          )
          SequencerErrors.Overloaded(message)
        },
      )
    }

    sender match {
      // Rate limiting only if participants send to participants.
      case participantId: ParticipantId if request.isConfirmationRequest =>
        for {
          confirmationRequestsMaxRate <- EitherT
            .right(synchronizerParamsLookup.getApproximateOrDefaultValue())
            .map(_.confirmationRequestsMaxRate)
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            checkRate(participantId, confirmationRequestsMaxRate)
          )
        } yield ()
      case _ =>
        EitherTUtil.unitUS
    }
  }

  private def getOrUpdateRateLimiter(
      participantId: ParticipantId,
      confirmationRequestsMaxRate: NonNegativeInt,
  ): RateLimiter = {
    def rateAsNumeric = NonNegativeNumeric.tryCreate(confirmationRequestsMaxRate.value.toDouble)
    rates.get(participantId) match {
      case Some(rateLimiter) =>
        if (
          Math.abs(
            rateLimiter.maxTasksPerSecond.value - confirmationRequestsMaxRate.value.toDouble
          ) < 1.0e-6
        )
          rateLimiter
        else {
          val newRateLimiter =
            new RateLimiter(rateAsNumeric, parameters.maxConfirmationRequestsBurstFactor)
          rates.update(participantId, newRateLimiter)
          newRateLimiter
        }
      case None =>
        rates.getOrElseUpdate(
          participantId,
          new RateLimiter(rateAsNumeric, parameters.maxConfirmationRequestsBurstFactor),
        )
    }
  }

  private def toVersionSubscriptionResponseV0(event: SequencedSerializedEvent) =
    v30.SubscriptionResponse(
      signedSequencedEvent = event.signedEvent.toByteString,
      Some(SerializableTraceContext(event.traceContext).toProtoV30),
    )

  override def subscribe(
      request: v30.SubscriptionRequest,
      responseObserver: StreamObserver[v30.SubscriptionResponse],
  ): Unit =
    subscribeInternal[v30.SubscriptionResponse](
      request,
      responseObserver,
      toVersionSubscriptionResponseV0,
    )

  private def subscribeInternal[T](
      request: v30.SubscriptionRequest,
      responseObserver: StreamObserver[T],
      toSubscriptionResponse: SequencedSerializedEvent => T,
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    withServerCallStreamObserver(responseObserver) { observer =>
      val resultE = for {
        subscriptionRequest <-
          SubscriptionRequest
            .fromProtoV30(request)
            .left
            .map(err => invalidRequest(err.toString))
        SubscriptionRequest(member, timestamp) = subscriptionRequest
        _ = logger.debug(
          s"Received subscription request from $member for timestamp (inclusive) $timestamp"
        )
        _ <- Either.cond(
          !isClosing,
          (),
          Status.UNAVAILABLE.withDescription("Sequencer is being shutdown."),
        )
        _ <- checkSubscriptionMemberPermission(member)
      } yield (member, timestamp, IdentityContextHelper.getCurrentStoredAuthenticationToken)

      // Note: we cannot assign the "cancel" handler during the async subscription creation,
      // see the doc for `setOnCancelHandler`.
      val createSubscriptionP =
        PromiseUnlessShutdown.unsupervised[Either[Status, GrpcManagedSubscription[?]]]()
      observer.setOnCancelHandler { () =>
        logger.info(s"Subscription cancelled by client ${request.member}.")
        // Instead upon cancellation, we close the subscription once/if it has been successfully created.
        createSubscriptionP.future.onComplete {
          case Success(Outcome(Right(subscription))) =>
            subscription.close()
          case _ => ()
        }
      }

      // Note: we do the first part of the subscription creation above in the same thread,
      // so that we can use the GRPC interceptor injected context to grab the authentication token.
      val resultET = for {
        result <- EitherT.fromEither[Future](resultE)
        (member, timestamp, authenticationTokenO) = result
        subscription <- subscriptionPool
          .create(
            () =>
              createSubscriptionV2[T](
                member,
                authenticationTokenO.map(_.expireAt),
                timestamp,
                observer,
                toSubscriptionResponse,
              ),
            member,
          )
          .leftMap { case SubscriptionPool.PoolClosed =>
            Status.UNAVAILABLE.withDescription("Subscription pool is closed.")
          }
      } yield subscription
      createSubscriptionP.completeWith(resultET.mapK(FutureUnlessShutdown.outcomeK).value.unwrap)
      FutureUtil.doNotAwait(
        resultET.fold(err => responseObserver.onError(err.asException()), _ => ()),
        failureMessage = s"Failed to establish subscription for ${request.member}",
      )
    }
  }

  private def checkSubscriptionMemberPermission(member: Member)(implicit
      traceContext: TraceContext
  ): Either[Status, Unit] =
    checkAuthenticatedMemberPermission(member)

  override def acknowledgeSigned(
      request: v30.AcknowledgeSignedRequest
  ): Future[v30.AcknowledgeSignedResponse] = {
    val acknowledgeRequestE = SignedContent
      .fromByteString(protocolVersion, request.signedAcknowledgeRequest)
      .flatMap(_.deserializeContent(AcknowledgeRequest.fromByteString(protocolVersion, _)))
    performAcknowledge(acknowledgeRequestE.map(SignedAcknowledgeRequest.apply))
  }

  private def performAcknowledge(
      acknowledgeRequestE: Either[
        ProtoDeserializationError,
        WrappedAcknowledgeRequest,
      ]
  ): Future[v30.AcknowledgeSignedResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // deserialize the request and check that they're authorized to perform a request on behalf of the member.
    // intentionally not using an EitherT here as we want to remain on the same thread to retain the GRPC context
    // for authorization.
    val validatedRequestE = for {
      wrappedRequest <- acknowledgeRequestE
        .leftMap(err => invalidRequest(err.toString).asException())
      request = wrappedRequest.unwrap
      // check they are authenticated to perform actions on behalf of this member
      _ <- checkAuthenticatedMemberPermission(request.member)
        .leftMap(_.asException())
    } yield wrappedRequest
    val result = (for {
      request <- validatedRequestE.toEitherT[FutureUnlessShutdown]
      deduplicate = acknowledgementConflate.exists { cache =>
        cache.getIfPresent(request.unwrap.member).isDefined
      }
      _ <- (request match {
        case s: SignedAcknowledgeRequest if !deduplicate =>
          sequencer
            .acknowledgeSigned(s.signedRequest)
            .thereafter {
              case scala.util.Success(_) =>
                acknowledgementConflate.foreach(_.put(request.unwrap.member, ()))
              case _ =>
            }
        case _ =>
          // Concurrent acknowledge requests could still being processed but that's not an issue
          // This is meant to prevent a participant from DoSing the ordering layer through acks and will quickly
          // block additional requests even if concurrent ones get through at first
          logger.debug(
            s"Discarding acknowledgement from ${request.unwrap.member} as it is within the conflating window."
          )
          EitherT.pure[FutureUnlessShutdown, String](())
      }).leftMap(e =>
        Status.INVALID_ARGUMENT.withDescription(s"Could not acknowledge $e").asException()
      )
    } yield ()).foldF[v30.AcknowledgeSignedResponse](
      FutureUnlessShutdown.failed,
      _ => FutureUnlessShutdown.pure(v30.AcknowledgeSignedResponse()),
    )

    result.asGrpcResponse
  }

  private def createSubscriptionV2[T](
      member: Member,
      expireAt: Option[CantonTimestamp],
      timestamp: Option[CantonTimestamp],
      observer: ServerCallStreamObserver[T],
      toSubscriptionResponse: SequencedSerializedEvent => T,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[GrpcManagedSubscription[T]] = {

    logger.info(s"$member subscribes from timestamp=$timestamp")
    val subscription = new GrpcManagedSubscription(
      handler => directSequencerSubscriptionFactory.create(timestamp, member, handler),
      observer,
      member,
      expireAt,
      timeouts,
      loggerFactory,
      toSubscriptionResponse,
    )
    subscription.initialize().map(_ => subscription)
  }

  /** Ensure observer is a ServerCalLStreamObserver
    *
    * @param observer
    *   underlying observer
    * @param handler
    *   handler requiring a ServerCallStreamObserver
    */
  private def withServerCallStreamObserver[R](
      observer: StreamObserver[R]
  )(handler: ServerCallStreamObserver[R] => Unit)(implicit traceContext: TraceContext): Unit =
    observer match {
      case serverCallStreamObserver: ServerCallStreamObserver[R] =>
        handler(serverCallStreamObserver)
      case _ =>
        val statusException =
          Status.INTERNAL.withDescription("Unknown stream observer request").asException()
        logger.warn(statusException.getMessage)
        observer.onError(statusException)
    }

  private def checkAuthenticatedMemberPermissionWithCurrentMember(
      member: Member,
      currentMember: Option[Member],
  )(implicit traceContext: TraceContext): Either[Status, Unit] =
    authenticationCheck
      .authenticate(
        member,
        currentMember,
      ) // This has to run at the beginning, because it reads from a thread-local.
      .leftMap { message =>
        logger.warn(s"Authentication check failed: $message")
        permissionDenied(message)
      }

  private def checkAuthenticatedMemberPermission(
      member: Member
  )(implicit traceContext: TraceContext): Either[Status, Unit] =
    checkAuthenticatedMemberPermissionWithCurrentMember(
      member,
      authenticationCheck.lookupCurrentMember(),
    )

  private def getDownloadTopologyStateForInit(
      request: TopologyStateForInitRequest,
      sendResponse: Seq[StoredTopologyTransaction.GenericStoredTopologyTransaction] => Unit,
  )(implicit traceContext: TraceContext): (UniqueKillSwitch, Future[Done]) =
    topologyStateForInitializationService
      .initialSnapshot(request.member)
      .grouped(maxItemsInTopologyResponse.value)
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.foreach(sendResponse))(Keep.both)
      .run()

  override def downloadTopologyStateForInit(
      requestP: v30.DownloadTopologyStateForInitRequest,
      responseObserver: StreamObserver[v30.DownloadTopologyStateForInitResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    withServerCallStreamObserver(responseObserver) { observer =>
      TopologyStateForInitRequest
        .fromProtoV30(requestP)
        .traverse { request =>
          val (killSwitch, future) = getDownloadTopologyStateForInit(
            request,
            response =>
              observer.onNext(
                TopologyStateForInitResponse(
                  Traced(StoredTopologyTransactions(response))
                ).toProtoV30
              ),
          )
          observer.setOnCancelHandler(() => killSwitch.shutdown())
          future
        }
        .onComplete {
          case Success(Left(parsingError)) =>
            responseObserver.onError(ProtoDeserializationFailure.Wrap(parsingError).asGrpcError)

          case Success(Right(_)) =>
            responseObserver.onCompleted()

          case Failure(exception) =>
            responseObserver.onError(exception)
        }
    }
  }

  private def invalidRequest(message: String): Status =
    Status.INVALID_ARGUMENT.withDescription(message)

  private def permissionDenied(message: String): Status =
    Status.PERMISSION_DENIED.withDescription(message)

  // avoid emitting a warning during the first sequencing of the topology snapshot
  private def warnOnUsingDefaults(sender: Option[Member]): Boolean = sender match {
    case Some(_: ParticipantId) => true
    case _ => false
  }

  override def onClosed(): Unit = {
    acknowledgementConflate.foreach { cache =>
      cache.invalidateAll()
      cache.cleanUp()
    }
    subscriptionPool.close()
  }

  /** Return the currently known traffic state for a member. Callers must be authorized to request
    * the traffic state.
    */
  override def getTrafficStateForMember(
      request: v30.GetTrafficStateForMemberRequest
  ): Future[v30.GetTrafficStateForMemberResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // Grab the current member from the context before we start doing anything async otherwise we'll lose it as
    // it's stored in a thread-local
    val currentMember = authenticationCheck.lookupCurrentMember()
    val result = for {
      member <- CantonGrpcUtil
        .wrapErrUS(Member.fromProtoPrimitive(request.member, "member"))
        .leftMap(_.asGrpcError)
      timestamp <- CantonGrpcUtil
        .wrapErrUS(CantonTimestamp.fromProtoPrimitive(request.timestamp))
        .leftMap(_.asGrpcError)
      _ <- EitherT
        .fromEither[FutureUnlessShutdown](
          checkAuthenticatedMemberPermissionWithCurrentMember(member, currentMember)
        )
        .leftMap(_.asRuntimeException())
      trafficO <- sequencer
        .getTrafficStateAt(member, timestamp)
        .leftMap(err =>
          io.grpc.Status.OUT_OF_RANGE.withDescription(err.toString).asRuntimeException()
        )

    } yield v30.GetTrafficStateForMemberResponse(trafficO.map(_.toProtoV30))

    EitherTUtil.toFuture(result.onShutdown(Left(AbortedDueToShutdown.Error().asGrpcError)))
  }

  override def getTime(request: GetTimeRequest): Future[GetTimeResponse] = {
    // This call is authenticated but does not require special authorization.

    // Traffic is not impacted as no events are emitted.

    // The returned information is expected to be readily available and served from transient memory,
    //  hence this call is expected to be fast and impose minimal load on sequencers,
    //  so network-level rate limiting is enough to secure it against denial-of-service attacks;
    //  for this reason we don't apply application-level rate limiting.
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    sequencer.sequencingTime
      .map(time => GetTimeResponse(time.map(_.toProtoPrimitive)))
      .onShutdown(throw AbortedDueToShutdown.Error().asGrpcError)
  }

  /** Returns a hash of a result equivalent to DownloadTopologyStateForInit call for a BFT check
    * without transferring the full state.
    */
  override def downloadTopologyStateForInitHash(
      requestP: DownloadTopologyStateForInitHashRequest
  ): Future[DownloadTopologyStateForInitHashResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    EitherTUtil.toFuture(
      EitherT(
        TopologyStateForInitRequest
          .fromProtoV30(requestP)
          .leftMap(x => ProtoDeserializationFailure.Wrap(x).asGrpcError)
          .traverse { request =>
            topologyStateForInitializationService
              .initialSnapshotHash(request.member)
              .map(hash =>
                DownloadTopologyStateForInitHashResponse(
                  topologyStateHash = hash.getCryptographicEvidence
                )
              )
              .onShutdown(throw AbortedDueToShutdown.Error().asGrpcError)
          }
      )
    )
  }
}
