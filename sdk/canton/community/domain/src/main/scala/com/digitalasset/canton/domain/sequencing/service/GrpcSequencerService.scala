// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.ProtoDeserializationError.ProtoDeserializationFailure
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeNumeric, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v30
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.domain.sequencing.config.SequencerParameters
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.{Sequencer, SequencerValidations}
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerService.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.GrpcErrors.AbortedDueToShutdown
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.DynamicDomainParametersLookup
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactions,
  TopologyStateForInitializationService,
}
import com.digitalasset.canton.tracing.{
  SerializableTraceContext,
  TraceContext,
  TraceContextGrpc,
  Traced,
}
import com.digitalasset.canton.util.{EitherTUtil, RateLimiter}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import io.grpc.Status
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.pekko.stream.Materializer

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Authenticate the current user can perform an operation on behalf of the given member */
private[sequencing] trait AuthenticationCheck {

  /** Can the authenticated member perform an action on behalf of the provided member.
    * Return a left with a user presentable error message if not.
    * Right if the operation can continue.
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
    ): Either[String, Unit] = Right(())

    override def lookupCurrentMember(): Option[Member] = None
  }
}

object GrpcSequencerService {

  def apply(
      sequencer: Sequencer,
      metrics: SequencerMetrics,
      authenticationCheck: AuthenticationCheck,
      clock: Clock,
      domainParamsLookup: DynamicDomainParametersLookup[SequencerDomainParameters],
      parameters: SequencerParameters,
      protocolVersion: ProtocolVersion,
      topologyStateForInitializationService: TopologyStateForInitializationService,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): GrpcSequencerService =
    new GrpcSequencerService(
      sequencer,
      metrics,
      loggerFactory,
      authenticationCheck,
      new SubscriptionPool[GrpcManagedSubscription[_]](
        clock,
        metrics,
        parameters.processingTimeouts,
        loggerFactory,
      ),
      new DirectSequencerSubscriptionFactory(
        sequencer,
        parameters.processingTimeouts,
        loggerFactory,
      ),
      domainParamsLookup,
      parameters,
      topologyStateForInitializationService,
      protocolVersion,
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
  * @param sequencer The underlying sequencer implementation
  */
class GrpcSequencerService(
    sequencer: Sequencer,
    metrics: SequencerMetrics,
    protected val loggerFactory: NamedLoggerFactory,
    authenticationCheck: AuthenticationCheck,
    subscriptionPool: SubscriptionPool[GrpcManagedSubscription[_]],
    directSequencerSubscriptionFactory: DirectSequencerSubscriptionFactory,
    domainParamsLookup: DynamicDomainParametersLookup[SequencerDomainParameters],
    parameters: SequencerParameters,
    topologyStateForInitializationService: TopologyStateForInitializationService,
    protocolVersion: ProtocolVersion,
    maxItemsInTopologyResponse: PositiveInt = PositiveInt.tryCreate(100),
)(implicit ec: ExecutionContext)
    extends v30.SequencerServiceGrpc.SequencerService
    with NamedLogging
    with FlagCloseable {

  override protected val timeouts: ProcessingTimeout = parameters.processingTimeouts

  private val rates = new TrieMap[ParticipantId, RateLimiter]()

  def membersWithActiveSubscriptions: Seq[Member] =
    subscriptionPool.activeSubscriptions().map(_.member)
  def disconnectMember(member: Member)(implicit traceContext: TraceContext): Unit =
    subscriptionPool.closeSubscriptions(member)
  def disconnectAllMembers()(implicit traceContext: TraceContext): Unit =
    subscriptionPool.closeAllSubscriptions()

  override def sendAsyncVersioned(
      requestP: v30.SendAsyncVersionedRequest
  ): Future[v30.SendAsyncVersionedResponse] =
    validateAndSend(requestP).map(_.toProtoV30)

  private def validateAndSend(
      proto: v30.SendAsyncVersionedRequest
  ): Future[SendAsyncVersionedResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // This has to run at the beginning, because it reads from a thread-local.
    val senderFromMetadata = authenticationCheck.lookupCurrentMember()

    def parseAndValidate(
        maxRequestSize: MaxRequestSize
    ): Either[SendAsyncError, SignedContent[SubmissionRequest]] = for {
      signedContent <- SignedContent
        .fromByteString(protocolVersion)(proto.signedSubmissionRequest)
        .leftMap(requestDeserializationError(_, maxRequestSize))
      signedSubmissionRequest <- signedContent
        .deserializeContent(
          SubmissionRequest
            .fromByteString(protocolVersion)(
              MaxRequestSizeToDeserialize.Limit(maxRequestSize.value)
            )
        )
        .leftMap(requestDeserializationError(_, maxRequestSize))
      _ <- validateSubmissionRequest(
        proto.serializedSize,
        signedSubmissionRequest.content,
        senderFromMetadata,
      )
    } yield signedSubmissionRequest

    lazy val sendET = for {
      domainParameters <- EitherT
        .right[SendAsyncError](
          domainParamsLookup.getApproximateOrDefaultValue(warnOnUsingDefaults(senderFromMetadata))
        )
        .mapK(FutureUnlessShutdown.outcomeK)
      request <- EitherT.fromEither[FutureUnlessShutdown](
        parseAndValidate(domainParameters.maxRequestSize)
      )
      _ <- checkRate(request.content).mapK(FutureUnlessShutdown.outcomeK)
      _ <- sequencer.sendAsyncSigned(request)
    } yield ()

    performUnlessClosingUSF(functionFullName)(sendET.value.map { res =>
      res.left.foreach { err =>
        logger.info(s"Rejecting submission request by $senderFromMetadata with $err")
      }
      toSendAsyncVersionedResponse(res)
    })
      .onShutdown(
        SendAsyncVersionedResponse(error = Some(SendAsyncError.ShuttingDown()))
      )
  }

  private def toSendAsyncVersionedResponse(
      result: Either[SendAsyncError, Unit]
  ): SendAsyncVersionedResponse =
    SendAsyncVersionedResponse(result.swap.toOption)

  private def requestDeserializationError(
      error: ProtoDeserializationError,
      maxRequestSize: MaxRequestSize,
  )(implicit traceContext: TraceContext): SendAsyncError = {
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
    SendAsyncError.RequestInvalid(message)
  }

  private def validateSubmissionRequest(
      requestSize: Int,
      request: SubmissionRequest,
      memberFromMetadata: Option[Member],
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = {
    val messageId = request.messageId

    def refuseUnless(
        sender: Member
    )(condition: Boolean, message: => String): Either[SendAsyncError, Unit] =
      Either.cond(condition, (), refuse(messageId.toProtoPrimitive, sender)(message))

    def invalidUnless(
        sender: Member
    )(condition: Boolean, message: => String): Either[SendAsyncError, Unit] =
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
        SequencerValidations.checkToAtMostOneMediator(request),
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
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] =
    SequencerValidations
      .wellformedAggregationRule(sender, aggregationRule)
      .leftMap(message => invalid(messageId.toProtoPrimitive, sender)(message))

  private def invalid(messageIdP: String, sender: Member)(
      message: String
  )(implicit traceContext: TraceContext): SendAsyncError = {
    logger.warn(s"Request '$messageIdP' from '$sender' is invalid: $message")
    SendAsyncError.RequestInvalid(message)
  }

  private def refuse(messageIdP: String, sender: Member)(
      message: String
  )(implicit traceContext: TraceContext): SendAsyncError = {
    logger.warn(s"Request '$messageIdP' from '$sender' refused: $message")
    SendAsyncError.RequestRefused(message)
  }

  private def checkRate(
      request: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit] = {
    val sender = request.sender
    def checkRate(
        participantId: ParticipantId,
        confirmationRequestsMaxRate: NonNegativeInt,
    ): Either[SendAsyncError, Unit] = {
      val limiter = getOrUpdateRateLimiter(participantId, confirmationRequestsMaxRate)
      Either.cond(
        limiter.checkAndUpdateRate(),
        (), {
          val message = f"Submission rate exceeds rate limit of $confirmationRequestsMaxRate/s."
          logger.info(
            f"Request '${request.messageId}' from '$sender' refused: $message"
          )
          SendAsyncError.Overloaded(message)
        },
      )
    }

    sender match {
      // Rate limiting only if participants send to participants.
      case participantId: ParticipantId if request.isConfirmationRequest =>
        for {
          confirmationRequestsMaxRate <- EitherT
            .right(domainParamsLookup.getApproximateOrDefaultValue())
            .map(_.confirmationRequestsMaxRate)
          _ <- EitherT.fromEither[Future](checkRate(participantId, confirmationRequestsMaxRate))
        } yield ()
      case _ =>
        EitherT.rightT[Future, SendAsyncError](())
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

  private def toVersionSubscriptionResponseV0(event: OrdinarySerializedEvent) =
    v30.VersionedSubscriptionResponse(
      signedSequencedEvent = event.signedEvent.toByteString,
      Some(SerializableTraceContext(event.traceContext).toProtoV30),
    )

  override def subscribeVersioned(
      request: v30.SubscriptionRequest,
      responseObserver: StreamObserver[v30.VersionedSubscriptionResponse],
  ): Unit =
    subscribeInternal[v30.VersionedSubscriptionResponse](
      request,
      responseObserver,
      toVersionSubscriptionResponseV0,
    )

  private def subscribeInternal[T](
      request: v30.SubscriptionRequest,
      responseObserver: StreamObserver[T],
      toSubscriptionResponse: OrdinarySerializedEvent => T,
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    withServerCallStreamObserver(responseObserver) { observer =>
      val result = for {
        subscriptionRequest <- SubscriptionRequest
          .fromProtoV30(request)
          .left
          .map(err => invalidRequest(err.toString))
        SubscriptionRequest(member, offset) = subscriptionRequest
        _ = logger.debug(s"Received subscription request from $member for offset $offset")
        _ <- Either.cond(
          !isClosing,
          (),
          Status.UNAVAILABLE.withDescription("Domain is being shutdown."),
        )
        _ <- checkSubscriptionMemberPermission(member)
        authenticationTokenO = IdentityContextHelper.getCurrentStoredAuthenticationToken
        _ <- subscriptionPool
          .create(
            () =>
              createSubscription[T](
                member,
                authenticationTokenO.map(_.expireAt),
                offset,
                observer,
                toSubscriptionResponse,
              ),
            member,
          )
          .leftMap { case SubscriptionPool.PoolClosed =>
            Status.UNAVAILABLE.withDescription("Subscription pool is closed.")
          }
      } yield ()
      result.fold(err => responseObserver.onError(err.asException()), identity)
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
      .fromByteString(protocolVersion)(request.signedAcknowledgeRequest)
      .flatMap(_.deserializeContent(AcknowledgeRequest.fromByteString(protocolVersion)))
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
    (for {
      request <- validatedRequestE.toEitherT[Future]
      _ <- (request match {
        case s: SignedAcknowledgeRequest =>
          sequencer
            .acknowledgeSigned(s.signedRequest)
      }).leftMap(e =>
        Status.INVALID_ARGUMENT.withDescription(s"Could not acknowledge $e").asException()
      )
    } yield ()).foldF[v30.AcknowledgeSignedResponse](
      Future.failed,
      _ => Future.successful(v30.AcknowledgeSignedResponse()),
    )
  }

  private def createSubscription[T](
      member: Member,
      expireAt: Option[CantonTimestamp],
      counter: SequencerCounter,
      observer: ServerCallStreamObserver[T],
      toSubscriptionResponse: OrdinarySerializedEvent => T,
  )(implicit traceContext: TraceContext): GrpcManagedSubscription[T] = {

    logger.info(s"$member subscribes from counter=$counter")
    new GrpcManagedSubscription(
      handler => directSequencerSubscriptionFactory.create(counter, member, handler),
      observer,
      member,
      expireAt,
      timeouts,
      loggerFactory,
      toSubscriptionResponse,
    )
  }

  /** Ensure observer is a ServerCalLStreamObserver
    *
    * @param observer underlying observer
    * @param handler  handler requiring a ServerCallStreamObserver
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

  override def downloadTopologyStateForInit(
      requestP: v30.DownloadTopologyStateForInitRequest,
      responseObserver: StreamObserver[v30.DownloadTopologyStateForInitResponse],
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    TopologyStateForInitRequest
      .fromProtoV30(requestP)
      .traverse(request =>
        topologyStateForInitializationService
          .initialSnapshot(request.member)
      )
      .onComplete {
        case Success(Left(parsingError)) =>
          responseObserver.onError(ProtoDeserializationFailure.Wrap(parsingError).asGrpcError)

        case Success(Right(initialSnapshot)) =>
          initialSnapshot.result.grouped(maxItemsInTopologyResponse.value).foreach { batch =>
            val response =
              TopologyStateForInitResponse(Traced(StoredTopologyTransactions(batch)))
            responseObserver.onNext(response.toProtoV30)
          }
          responseObserver.onCompleted()

        case Failure(exception) =>
          responseObserver.onError(exception)
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

  override def onClosed(): Unit =
    subscriptionPool.close()

  /** Return the currently known traffic state for a member. Callers must be authorized to request the traffic state.
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
}
