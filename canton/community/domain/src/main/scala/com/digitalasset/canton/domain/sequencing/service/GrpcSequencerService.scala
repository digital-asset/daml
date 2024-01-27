// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import cats.syntax.foldable.*
import com.daml.metrics.api.MetricsContext
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, NonNegativeNumeric}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.domain.metrics.SequencerMetrics
import com.digitalasset.canton.domain.sequencing.SequencerParameters
import com.digitalasset.canton.domain.sequencing.authentication.grpc.IdentityContextHelper
import com.digitalasset.canton.domain.sequencing.sequencer.errors.SequencerError
import com.digitalasset.canton.domain.sequencing.sequencer.{Sequencer, SequencerValidations}
import com.digitalasset.canton.domain.sequencing.service.GrpcSequencerService.*
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.protocol.DomainParametersLookup.SequencerDomainParameters
import com.digitalasset.canton.protocol.{DomainParametersLookup, v0 as protocolV0}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, TraceContextGrpc}
import com.digitalasset.canton.util.{EitherTUtil, RateLimiter}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.empty.Empty
import io.grpc.Status
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}
import org.apache.pekko.stream.Materializer

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

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
      domainParamsLookup: DomainParametersLookup[SequencerDomainParameters],
      parameters: SequencerParameters,
      protocolVersion: ProtocolVersion,
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
      protocolVersion,
    )

  /** Abstracts the steps that are different in processing the submission requests coming from the various sendAsync endpoints
    * @tparam ProtoClass The scalapb generated class of the RPC request message
    */
  private sealed trait SubmissionRequestProcessing[ProtoClass <: scalapb.GeneratedMessage] {

    /** The Scala class to which the `ProtoClass` should deserialize to */
    type ValueClass

    /** Tries to parse the proto class to the value class, erroring if the request exceeds the given limit. */
    def parse(
        requestP: ProtoClass,
        maxRequestSize: MaxRequestSize,
        protocolVersion: ProtocolVersion,
    ): ParsingResult[ValueClass]

    /** Extract the [[SubmissionRequest]] from the value class */
    def unwrap(request: ValueClass): SubmissionRequest

    /** Call the appropriate send method on the [[Sequencer]] */
    def send(request: ValueClass, sequencer: Sequencer)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncError, Unit]
  }

  private object PlainSubmissionRequestProcessing
      extends SubmissionRequestProcessing[protocolV0.SubmissionRequest] {
    override type ValueClass = SubmissionRequest

    override def parse(
        requestP: protocolV0.SubmissionRequest,
        maxRequestSize: MaxRequestSize,
        protocolVersion: ProtocolVersion, // unused; because this parse implementation uses proto version 0 always
    ): ParsingResult[SubmissionRequest] =
      SubmissionRequest.fromProtoV0(
        requestP,
        MaxRequestSizeToDeserialize.Limit(maxRequestSize.value),
      )

    override def unwrap(request: SubmissionRequest): SubmissionRequest = request

    override def send(request: SubmissionRequest, sequencer: Sequencer)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncError, Unit] =
      sequencer.sendAsync(request)
  }

  private object SignedSubmissionRequestProcessing
      extends SubmissionRequestProcessing[protocolV0.SignedContent] {
    override type ValueClass = SignedContent[SubmissionRequest]

    override def parse(
        requestP: protocolV0.SignedContent,
        maxRequestSize: MaxRequestSize,
        protocolVersion: ProtocolVersion,
    ): ParsingResult[SignedContent[SubmissionRequest]] =
      SignedContent
        .fromProtoV0(requestP)
        .flatMap(
          _.deserializeContent(
            SubmissionRequest
              .fromByteString(protocolVersion)(
                MaxRequestSizeToDeserialize.Limit(maxRequestSize.value)
              )
          )
        )

    override def unwrap(request: SignedContent[SubmissionRequest]): SubmissionRequest =
      request.content

    override def send(request: SignedContent[SubmissionRequest], sequencer: Sequencer)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncError, Unit] = sequencer.sendAsyncSigned(request)
  }

  private object VersionedSignedSubmissionRequestProcessing
      extends SubmissionRequestProcessing[v0.SendAsyncVersionedRequest] {
    override type ValueClass = SignedContent[SubmissionRequest]

    override def parse(
        requestP: v0.SendAsyncVersionedRequest,
        maxRequestSize: MaxRequestSize,
        protocolVersion: ProtocolVersion,
    ): ParsingResult[SignedContent[SubmissionRequest]] = {
      for {
        signedContent <- SignedContent.fromByteString(protocolVersion)(
          requestP.signedSubmissionRequest
        )
        signedSubmissionRequest <- signedContent.deserializeContent(
          SubmissionRequest
            .fromByteString(protocolVersion)(
              MaxRequestSizeToDeserialize.Limit(maxRequestSize.value)
            )
        )
      } yield signedSubmissionRequest
    }

    override def unwrap(request: SignedContent[SubmissionRequest]): SubmissionRequest =
      request.content

    override def send(request: SignedContent[SubmissionRequest], sequencer: Sequencer)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncError, Unit] = sequencer.sendAsyncSigned(request)
  }

  private object VersionedUnsignedSubmissionRequestProcessing
      extends SubmissionRequestProcessing[v0.SendAsyncUnauthenticatedVersionedRequest] {
    override type ValueClass = SubmissionRequest

    override def parse(
        requestP: v0.SendAsyncUnauthenticatedVersionedRequest,
        maxRequestSize: MaxRequestSize,
        protocolVersion: ProtocolVersion,
    ): ParsingResult[SubmissionRequest] =
      SubmissionRequest.fromByteString(protocolVersion)(
        MaxRequestSizeToDeserialize.Limit(maxRequestSize.value)
      )(
        requestP.submissionRequest
      )

    override def unwrap(request: SubmissionRequest): SubmissionRequest = request

    override def send(request: SubmissionRequest, sequencer: Sequencer)(implicit
        traceContext: TraceContext
    ): EitherT[Future, SendAsyncError, Unit] = sequencer.sendAsync(request)
  }

  private sealed trait WrappedAcknowledgeRequest extends Product with Serializable {
    def unwrap: AcknowledgeRequest
  }
  private final case class PlainAcknowledgeRequest(request: AcknowledgeRequest)
      extends WrappedAcknowledgeRequest {
    override def unwrap: AcknowledgeRequest = request
  }
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
    domainParamsLookup: DomainParametersLookup[SequencerDomainParameters],
    parameters: SequencerParameters,
    protocolVersion: ProtocolVersion,
)(implicit ec: ExecutionContext)
    extends v0.SequencerServiceGrpc.SequencerService
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

  override def sendAsyncSigned(
      requestP: protocolV0.SignedContent
  ): Future[v0.SendAsyncSignedResponse] =
    if (!SubmissionRequest.usingSignedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The unsigned send endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else if (SubmissionRequest.usingVersionedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The versioned send endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else {
      validateAndSend(
        requestP,
        SignedSubmissionRequestProcessing,
        isUsingAuthenticatedEndpoint = true,
      ).map(_.toSendAsyncSignedResponseProto)
    }

  override def sendAsync(requestP: protocolV0.SubmissionRequest): Future[v0.SendAsyncResponse] =
    if (SubmissionRequest.usingSignedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The signed send endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else {
      validateAndSend(
        requestP,
        PlainSubmissionRequestProcessing,
        isUsingAuthenticatedEndpoint = true,
      ).map(_.toSendAsyncResponseProto)
    }

  override def sendAsyncUnauthenticated(
      requestP: protocolV0.SubmissionRequest
  ): Future[v0.SendAsyncResponse] =
    if (SubmissionRequest.usingVersionedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The versioned send endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else {
      validateAndSend(
        requestP,
        PlainSubmissionRequestProcessing,
        isUsingAuthenticatedEndpoint = false,
      ).map(_.toSendAsyncResponseProto)
    }

  override def sendAsyncVersioned(
      requestP: v0.SendAsyncVersionedRequest
  ): Future[v0.SendAsyncSignedResponse] =
    if (!SubmissionRequest.usingVersionedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The unversioned send endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else {
      validateAndSend(
        requestP,
        VersionedSignedSubmissionRequestProcessing,
        isUsingAuthenticatedEndpoint = true,
      ).map(_.toSendAsyncSignedResponseProto)
    }

  override def sendAsyncUnauthenticatedVersioned(
      requestP: v0.SendAsyncUnauthenticatedVersionedRequest
  ): Future[v0.SendAsyncResponse] =
    if (!SubmissionRequest.usingVersionedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The unversioned send endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else {
      validateAndSend(
        requestP,
        VersionedUnsignedSubmissionRequestProcessing,
        isUsingAuthenticatedEndpoint = false,
      ).map(_.toSendAsyncResponseProto)
    }

  private def validateAndSend[ProtoClass <: scalapb.GeneratedMessage](
      proto: ProtoClass,
      processing: SubmissionRequestProcessing[ProtoClass],
      isUsingAuthenticatedEndpoint: Boolean,
  ): Future[SendAsyncResponse] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext

    // This has to run at the beginning, because it reads from a thread-local.
    val senderFromMetadata = authenticationCheck.lookupCurrentMember()

    def parseAndValidate(
        maxRequestSize: MaxRequestSize
    ): Either[SendAsyncError, processing.ValueClass] = for {
      request <- processing
        .parse(proto, maxRequestSize, protocolVersion)
        .leftMap(requestDeserializationError(_, maxRequestSize))
      _ <- validateSubmissionRequest(
        proto.serializedSize,
        processing.unwrap(request),
        senderFromMetadata,
      )
      _ <- checkSenderPermission(processing.unwrap(request), isUsingAuthenticatedEndpoint)
    } yield request

    lazy val sendET = for {
      domainParameters <- EitherT.right[SendAsyncError](
        domainParamsLookup.getApproximateOrDefaultValue(warnOnUsingDefaults(senderFromMetadata))
      )
      request <- EitherT.fromEither[Future](parseAndValidate(domainParameters.maxRequestSize))
      _ <- checkRate(processing.unwrap(request))
      _ <- processing.send(request, sequencer)
    } yield ()

    performUnlessClosingF(functionFullName)(sendET.value.map { res =>
      res.left.foreach { err =>
        logger.info(s"Rejecting submission request by $senderFromMetadata with ${err}")
      }
      toSendAsyncResponse(res)
    })
      .onShutdown(SendAsyncResponse(error = Some(SendAsyncError.ShuttingDown())))

  }

  private def toSendAsyncResponse(result: Either[SendAsyncError, Unit]): SendAsyncResponse =
    SendAsyncResponse(result.swap.toOption)

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

  private def checkSenderPermission(
      submissionRequest: SubmissionRequest,
      isUsingAuthenticatedEndpoint: Boolean,
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = {
    val sender = submissionRequest.sender
    for {
      _ <- Either.cond(
        sender.isAuthenticated == isUsingAuthenticatedEndpoint,
        (),
        refuse(submissionRequest.messageId.toProtoPrimitive, sender)(
          s"Sender $sender needs to use ${if (isUsingAuthenticatedEndpoint) "unauthenticated"
            else "authenticated"} send operation"
        ),
      )
      _ <- sender match {
        case authMember: AuthenticatedMember =>
          checkAuthenticatedSendPermission(submissionRequest, authMember)
        case unauthMember: UnauthenticatedMemberId =>
          checkUnauthenticatedSendPermission(submissionRequest, unauthMember)
      }
    } yield ()
  }

  private def validateSubmissionRequest(
      requestSize: Int,
      request: SubmissionRequest,
      memberFromMetadata: Option[Member],
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = {
    val messageId = request.messageId

    // TODO(i2741) properly deal with malicious behaviour
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
        SequencerValidations.checkFromParticipantToAtMostOneMediator(request),
        "Batch from participant contains multiple mediators as recipients.",
      )
      _ <- refuseUnless(sender)(
        noSigningTimestampIfUnauthenticated(
          sender,
          request.timestampOfSigningKey,
          request.batch.envelopes,
        ),
        "Requests sent from or to unauthenticated members must not specify the timestamp of the signing key",
      )
    } yield {
      metrics.bytesProcessed.mark(requestSize.toLong)(MetricsContext.Empty)
      metrics.messagesProcessed.mark()
      if (TimeProof.isTimeProofSubmission(request)) metrics.timeRequests.mark()

      ()
    }
  }

  /** Reject requests that involve unauthenticated members and specify the timestamp of the signing key.
    * This is because the unauthenticated member typically does not know the domain topology state
    * and therefore cannot validate that the requested timestamp is within the signing tolerance.
    */
  private def noSigningTimestampIfUnauthenticated(
      sender: Member,
      timestampOfSigningKey: Option[CantonTimestamp],
      envelopes: Seq[ClosedEnvelope],
  ): Boolean =
    timestampOfSigningKey.isEmpty || (sender.isAuthenticated && envelopes.forall(
      _.recipients.allRecipients.forall(_.member.isAuthenticated)
    ))

  private def invalid(messageIdP: String, senderPO: String)(
      message: String
  )(implicit traceContext: TraceContext): SendAsyncError = {
    val senderText = if (senderPO.isEmpty) "[sender-not-set]" else senderPO
    logger.warn(s"Request '$messageIdP' from '$senderText' is invalid: $message")
    SendAsyncError.RequestInvalid(message)
  }

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

  private def extractSender(messageId: MessageId, senderP: String)(implicit
      traceContext: TraceContext
  ): Either[SendAsyncError, Member] =
    Member
      .fromProtoPrimitive(senderP, "member")
      .leftMap(err => invalid(messageId.toProtoPrimitive, senderP)(s"Unable to parse sender: $err"))

  private def checkAuthenticatedSendPermission(
      request: SubmissionRequest,
      sender: AuthenticatedMember,
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = sender match {
    case _: DomainTopologyManagerId => Right(())
    case _ =>
      val unauthRecipients = request.batch.envelopes
        .toSet[ClosedEnvelope]
        .flatMap(_.recipients.allRecipients)
        .collect { case Recipient(unauthMember: UnauthenticatedMemberId) =>
          unauthMember
        }
      Either.cond(
        unauthRecipients.isEmpty,
        (),
        refuse(request.messageId.toProtoPrimitive, sender)(
          s"Member is trying to send message to unauthenticated ${unauthRecipients.mkString(" ,")}. Only domain manager can do that."
        ),
      )
  }

  private def checkUnauthenticatedSendPermission(
      request: SubmissionRequest,
      unauthenticatedMember: UnauthenticatedMemberId,
  )(implicit traceContext: TraceContext): Either[SendAsyncError, Unit] = {
    // unauthenticated member can only send messages to IDM
    val nonIdmRecipients = request.batch.envelopes
      .flatMap(_.recipients.allRecipients)
      .filter {
        case Recipient(_: DomainTopologyManagerId) => false
        case _ => true
      }
    Either.cond(
      nonIdmRecipients.isEmpty,
      (),
      refuse(request.messageId.toProtoPrimitive, unauthenticatedMember)(
        s"Unauthenticated member is trying to send message to members other than the domain manager: ${nonIdmRecipients.toSet
            .mkString(" ,")}."
      ),
    )
  }

  private def checkRate(
      request: SubmissionRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit] = {
    val sender = request.sender
    def checkRate(
        participantId: ParticipantId,
        maxRatePerParticipant: NonNegativeInt,
    ): Either[SendAsyncError, Unit] = {
      val limiter = getOrUpdateRateLimiter(participantId, maxRatePerParticipant)
      Either.cond(
        limiter.checkAndUpdateRate(),
        (), {
          val message = f"Submission rate exceeds rate limit of $maxRatePerParticipant/s."
          logger.info(
            f"Request '${request.messageId}' from '$sender' refused: $message"
          )
          SendAsyncError.Overloaded(message)
        },
      )
    }

    sender match {
      case participantId: ParticipantId if request.isRequest =>
        for {
          maxRatePerParticipant <- EitherTUtil
            .fromFuture(
              domainParamsLookup.getApproximateOrDefaultValue(),
              e => SendAsyncError.Internal(s"Unable to retrieve domain parameters: ${e.getMessage}"),
            )
            .map(_.maxRatePerParticipant)
          _ <- EitherT.fromEither[Future](checkRate(participantId, maxRatePerParticipant))
        } yield ()
      case _ =>
        // No rate limitation for domain entities and non-requests
        // TODO(i2898): verify that the sender is not lying about the request nature to bypass the rate limitation
        EitherT.rightT[Future, SendAsyncError](())
    }
  }
  private def getOrUpdateRateLimiter(
      participantId: ParticipantId,
      maxRatePerParticipant: NonNegativeInt,
  ): RateLimiter = {
    def rateAsNumeric = NonNegativeNumeric.tryCreate(maxRatePerParticipant.value.toDouble)
    rates.get(participantId) match {
      case Some(rateLimiter) =>
        if (
          Math.abs(
            rateLimiter.maxTasksPerSecond.value - maxRatePerParticipant.value.toDouble
          ) < 1.0e-6
        )
          rateLimiter
        else {
          val newRateLimiter = new RateLimiter(rateAsNumeric, parameters.maxBurstFactor)
          rates.update(participantId, newRateLimiter)
          newRateLimiter
        }
      case None =>
        rates.getOrElseUpdate(
          participantId,
          new RateLimiter(rateAsNumeric, parameters.maxBurstFactor),
        )
    }
  }

  private def toSubscriptionResponseV0(event: OrdinarySerializedEvent) =
    v0.SubscriptionResponse(
      signedSequencedEvent = Some(event.signedEvent.toProtoV0),
      Some(SerializableTraceContext(event.traceContext).toProtoV0),
    )

  private def toVersionSubscriptionResponseV0(event: OrdinarySerializedEvent) =
    v0.VersionedSubscriptionResponse(
      signedSequencedEvent = event.signedEvent.toByteString,
      Some(SerializableTraceContext(event.traceContext).toProtoV0),
      event.trafficState.map(_.toProtoV0),
    )

  override def subscribe(
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[v0.SubscriptionResponse],
  ): Unit =
    if (usingVersionedSubscription(protocolVersion))
      responseObserver.onError(
        wrongProtocolVersion(
          s"The versioned subscribe endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    else
      subscribeInternal[v0.SubscriptionResponse](
        request,
        responseObserver,
        requiresAuthentication = true,
        toSubscriptionResponseV0,
      )

  def usingVersionedSubscription(protocolVersion: ProtocolVersion) =
    protocolVersion >= ProtocolVersion.v5

  override def subscribeUnauthenticated(
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[v0.SubscriptionResponse],
  ): Unit =
    if (usingVersionedSubscription(protocolVersion)) {
      responseObserver.onError(
        wrongProtocolVersion(
          s"The versioned subscribe endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else
      subscribeInternal[v0.SubscriptionResponse](
        request,
        responseObserver,
        requiresAuthentication = false,
        toSubscriptionResponseV0,
      )

  override def subscribeVersioned(
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[v0.VersionedSubscriptionResponse],
  ): Unit =
    if (!usingVersionedSubscription(protocolVersion)) {
      responseObserver.onError(
        wrongProtocolVersion(
          s"The unversioned subscribe endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else
      subscribeInternal[v0.VersionedSubscriptionResponse](
        request,
        responseObserver,
        requiresAuthentication = true,
        toVersionSubscriptionResponseV0,
      )

  override def subscribeUnauthenticatedVersioned(
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[v0.VersionedSubscriptionResponse],
  ): Unit =
    if (!usingVersionedSubscription(protocolVersion)) {
      responseObserver.onError(
        wrongProtocolVersion(
          s"The unversioned subscribe endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else
      subscribeInternal[v0.VersionedSubscriptionResponse](
        request,
        responseObserver,
        requiresAuthentication = false,
        toVersionSubscriptionResponseV0,
      )

  private def subscribeInternal[T](
      request: v0.SubscriptionRequest,
      responseObserver: StreamObserver[T],
      requiresAuthentication: Boolean,
      toSubscriptionResponse: OrdinarySerializedEvent => T,
  ): Unit = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    withServerCallStreamObserver(responseObserver) { observer =>
      val result = for {
        subscriptionRequest <- SubscriptionRequest
          .fromProtoV0(request)
          .left
          .map(err => invalidRequest(err.toString))
        SubscriptionRequest(member, offset) = subscriptionRequest
        _ = logger.debug(s"Received subscription request from $member for offset $offset")
        _ <- Either.cond(
          !isClosing,
          (),
          Status.UNAVAILABLE.withDescription("Domain is being shutdown."),
        )
        _ <- checkSubscriptionMemberPermission(member, requiresAuthentication)
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

  private def checkSubscriptionMemberPermission(member: Member, requiresAuthentication: Boolean)(
      implicit traceContext: TraceContext
  ): Either[Status, Unit] =
    (member, requiresAuthentication) match {
      case (authMember: AuthenticatedMember, true) =>
        checkAuthenticatedMemberPermission(authMember)
      case (authMember: AuthenticatedMember, false) =>
        Left(
          Status.PERMISSION_DENIED.withDescription(
            s"Member $authMember needs to use authenticated subscribe operation"
          )
        )
      case (_: UnauthenticatedMemberId, false) =>
        Right(())
      case (unauthMember: UnauthenticatedMemberId, true) =>
        Left(
          Status.PERMISSION_DENIED.withDescription(
            s"Member $unauthMember cannot use authenticated subscribe operation"
          )
        )
    }

  override def acknowledge(requestP: v0.AcknowledgeRequest): Future[Empty] =
    if (SubmissionRequest.usingSignedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The signed acknowledgement endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else
      performAcknowledge(
        AcknowledgeRequest
          .fromProtoV0Unmemoized(requestP)
          .map(ack => PlainAcknowledgeRequest(ack))
      )

  override def acknowledgeSigned(request: protocolV0.SignedContent): Future[Empty] = {
    if (!SubmissionRequest.usingSignedSubmissionRequest(protocolVersion)) {
      Future.failed(
        wrongProtocolVersion(
          s"The unsigned acknowledgement endpoints must be used with protocol version $protocolVersion"
        ).asException
      )
    } else {
      val acknowledgeRequestE = SignedContent
        .fromProtoV0(request)
        .flatMap(
          _.deserializeContent(AcknowledgeRequest.fromByteString(protocolVersion))
        )
      performAcknowledge(acknowledgeRequestE.map(SignedAcknowledgeRequest))
    }
  }

  private def performAcknowledge(
      acknowledgeRequestE: Either[
        ProtoDeserializationError,
        WrappedAcknowledgeRequest,
      ]
  ): Future[Empty] = {
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
        case p: PlainAcknowledgeRequest =>
          EitherT.right(sequencer.acknowledge(p.unwrap.member, p.unwrap.timestamp))
        case s: SignedAcknowledgeRequest =>
          sequencer
            .acknowledgeSigned(s.signedRequest)
      }).leftMap(e =>
        Status.INVALID_ARGUMENT.withDescription(s"Could not acknowledge $e").asException()
      )
    } yield ()).foldF[Empty](Future.failed, _ => Future.successful(Empty()))
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
      handler => directSequencerSubscriptionFactory.create(counter, "direct", member, handler),
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
        val statusException = internalError("Unknown stream observer request").asException()
        logger.warn(statusException.getMessage)
        observer.onError(statusException)
    }

  private def checkAuthenticatedMemberPermission(
      member: Member
  )(implicit traceContext: TraceContext): Either[Status, Unit] =
    authenticationCheck
      .authenticate(
        member,
        authenticationCheck.lookupCurrentMember(),
      ) // This has to run at the beginning, because it reads from a thread-local.
      .leftMap { message =>
        logger.warn(s"Authentication check failed: $message")
        permissionDenied(message)
      }

  private def invalidRequest(message: String): Status =
    Status.INVALID_ARGUMENT.withDescription(message)

  private def internalError(message: String): Status = Status.INTERNAL.withDescription(message)

  private def permissionDenied(message: String): Status =
    Status.PERMISSION_DENIED.withDescription(message)

  private def wrongProtocolVersion(message: String): Status =
    Status.UNIMPLEMENTED.withDescription(message)

  // avoid emitting a warning during the first sequencing of the topology snapshot
  private def warnOnUsingDefaults(sender: Option[Member]): Boolean = sender match {
    case Some(_: ParticipantId) => true
    case _ => false
  }

  override def onClosed(): Unit = {
    subscriptionPool.close()
  }

}
