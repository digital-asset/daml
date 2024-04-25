// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.parallel.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.domain.sequencing.sequencer.errors.*
import com.digitalasset.canton.health.admin.data.SequencerHealthStatus
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SendAsyncError,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.time.EnrichedDurations.*
import com.digitalasset.canton.time.{Clock, PeriodicAction}
import com.digitalasset.canton.topology.{
  DomainMember,
  DomainTopologyManagerId,
  Member,
  UnauthenticatedMemberId,
}
import com.digitalasset.canton.tracing.Spanning.SpanWrapper
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.EitherTUtil.ifThenET
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** Provides additional functionality that is common between sequencer implementations:
  *  - auto registers unknown recipients addressed in envelopes from the domain topology manager
  *    (avoids explicit registration from the domain node -> sequencer which will be useful when separate processes)
  */
abstract class BaseSequencer(
    domainManagerId: DomainTopologyManagerId,
    protected val loggerFactory: NamedLoggerFactory,
    healthConfig: Option[SequencerHealthConfig],
    clock: Clock,
    signatureVerifier: SignatureVerifier,
)(implicit executionContext: ExecutionContext, trace: Tracer)
    extends Sequencer
    with NamedLogging
    with Spanning {

  val periodicHealthCheck: Option[PeriodicAction] = healthConfig.map(conf =>
    // periodically calling the sequencer's health check in order to continuously notify
    // listeners in case the health status has changed.
    new PeriodicAction(
      clock,
      conf.backendCheckPeriod.toInternal,
      loggerFactory,
      timeouts,
      "health-check",
    )(tc => healthInternal(tc).map(reportHealthState(_)(tc)))
  )

  /** The domain manager is responsible for identities within the domain.
    * If they decide to address a message to a member then we can be confident that they want the member available on the sequencer.
    * No other member gets such privileges.
    */
  private def autoRegisterNewMembersMentionedByIdentityManager(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, WriteRequestRefused, Unit] = {
    ifThenET(submission.sender == domainManagerId)(
      submission.batch.allMembers.toList
        .parTraverse_(ensureMemberRegistered)
    )
  }

  private def ensureMemberRegistered(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, WriteRequestRefused, Unit] =
    ensureRegistered(member).leftFlatMap {
      // due to the way ensureRegistered executes it is unlikely (I think impossible) for the already registered error
      // to be returned, however in this circumstance it's actually fine as we want them registered regardless.
      case OperationError(RegisterMemberError.AlreadyRegisteredError(member)) =>
        logger.debug(
          s"Went to auto register member but found they were already registered: $member"
        )
        EitherT.pure[Future, WriteRequestRefused](())
      case OperationError(RegisterMemberError.UnexpectedError(member, message)) =>
        // TODO(#11062) consider whether to propagate these errors further
        logger.error(s"An unexpected error occurred whilst registering member $member: $message")
        EitherT.pure[Future, WriteRequestRefused](())
      case error: WriteRequestRefused => EitherT.leftT(error)
    }

  override def sendAsyncSigned(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit] = withSpan("Sequencer.sendAsyncSigned") {
    implicit traceContext => span =>
      val submission = signedSubmission.content
      span.setAttribute("sender", submission.sender.toString)
      span.setAttribute("message_id", submission.messageId.unwrap)
      for {
        signedSubmissionWithFixedTs <- signatureVerifier
          .verifySignature[SubmissionRequest](
            signedSubmission,
            HashPurpose.SubmissionRequestSignature,
            _.sender,
          )
          .leftMap(e => SendAsyncError.RequestRefused(e))
        _ <- checkMemberRegistration(submission)
        _ <- sendAsyncSignedInternal(signedSubmissionWithFixedTs)
      } yield ()
  }

  override def acknowledgeSigned(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(
      implicit traceContext: TraceContext
  ): EitherT[Future, String, Unit] = for {
    signedAcknowledgeRequestWithFixedTs <- signatureVerifier
      .verifySignature[AcknowledgeRequest](
        signedAcknowledgeRequest,
        HashPurpose.AcknowledgementSignature,
        _.member,
      )
    _ <- EitherT.right(acknowledgeSignedInternal(signedAcknowledgeRequestWithFixedTs))
  } yield ()

  protected def acknowledgeSignedInternal(
      signedAcknowledgeRequest: SignedContent[AcknowledgeRequest]
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  override def sendAsync(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    withSpan("Sequencer.sendAsync") { implicit traceContext => span =>
      setSpanAttributes(span, submission)
      for {
        _ <- checkMemberRegistration(submission)
        _ <- sendAsyncInternal(submission)
      } yield ()
    }

  private def setSpanAttributes(span: SpanWrapper, submission: SubmissionRequest): Unit = {
    span.setAttribute("sender", submission.sender.toString)
    span.setAttribute("message_id", submission.messageId.unwrap)
  }

  private def checkMemberRegistration(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    (for {
      _ <- autoRegisterNewMembersMentionedByIdentityManager(submission)
      _ <- submission.sender match {
        case member: UnauthenticatedMemberId =>
          ensureMemberRegistered(member)
        case _ => EitherT.pure[Future, WriteRequestRefused](())
      }
    } yield ()).leftSemiflatMap { registrationError =>
      logger.error(s"Failed to auto-register members: $registrationError")
      // this error won't exist once sendAsync is fully implemented, so temporarily we'll just return a failed future
      Future.failed(
        new RuntimeException(s"Failed to auto-register members: $registrationError")
      )
    }

  protected def localSequencerMember: DomainMember
  protected def disableMemberInternal(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def disableMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerAdministrationError, Unit] = {
    logger.info(show"Disabling member at the sequencer: $member")
    for {
      _ <- EitherT
        .cond[Future](
          localSequencerMember != member,
          (),
          SequencerAdministrationError.CannotDisableLocalSequencerMember
            .Error(localSequencerMember),
        )

      _ <- EitherT.right(disableMemberInternal(member))
    } yield ()
  }

  protected def healthInternal(implicit traceContext: TraceContext): Future[SequencerHealthStatus]

  protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  protected def sendAsyncSignedInternal(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Unit]

  override def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] =
    for {
      _ <- member match {
        case _: UnauthenticatedMemberId =>
          ensureMemberRegistered(member)
            .leftMap(CreateSubscriptionError.RegisterUnauthenticatedMemberError)
        case _ =>
          EitherT.pure[Future, CreateSubscriptionError](())
      }
      source <- readInternal(member, offset)
    } yield source

  protected def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource]

  override def onClosed(): Unit =
    periodicHealthCheck.foreach(Lifecycle.close(_)(logger))

}
