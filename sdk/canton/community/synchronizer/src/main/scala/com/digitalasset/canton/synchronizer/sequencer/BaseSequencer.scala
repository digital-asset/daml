// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.data.EitherT
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.crypto.HashPurpose
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.protocol.SequencerErrors.SubmissionRequestRefused
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SequencerDeliverError,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.sequencer.admin.data.SequencerHealthStatus
import com.digitalasset.canton.synchronizer.sequencer.errors.{
  CreateSubscriptionError,
  SequencerAdministrationError,
}
import com.digitalasset.canton.time.{Clock, PeriodicAction}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util.ShowUtil.*
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext

/** Implements parts of [[Sequencer]] interface, common to all sequencers. Adds `*Internal` methods
  * without implementation for variance among specific sequencer subclasses.
  */
abstract class BaseSequencer(
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

  override def sendAsyncSigned(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit] =
    withSpan("Sequencer.sendAsyncSigned") { implicit traceContext => span =>
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
          .leftMap(e => SubmissionRequestRefused(e))
        isMemberEnabled <- EitherT.right[SequencerDeliverError](
          isEnabled(submission.sender)
        )
        _ <- EitherT.cond[FutureUnlessShutdown](
          isMemberEnabled,
          (),
          SubmissionRequestRefused(
            s"Member ${submission.sender} is disabled at the sequencer"
          ),
        )
        _ <- sendAsyncSignedInternal(signedSubmissionWithFixedTs)
      } yield ()
    }

  override def acknowledgeSigned(signedAcknowledgeRequest: SignedContent[AcknowledgeRequest])(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = for {
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
  ): FutureUnlessShutdown[Unit]

  protected def localSequencerMember: Member
  protected def disableMemberInternal(member: Member)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit]

  def disableMember(member: Member)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerAdministrationError, Unit] = {
    logger.info(show"Disabling member at the sequencer: $member")
    for {
      _ <- EitherT
        .cond[FutureUnlessShutdown](
          localSequencerMember != member,
          (),
          SequencerAdministrationError.CannotDisableLocalSequencerMember
            .Error(localSequencerMember),
        )

      _ <- EitherT.right(disableMemberInternal(member))
    } yield ()
  }

  protected def healthInternal(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SequencerHealthStatus]

  protected def sendAsyncInternal(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit]

  protected def sendAsyncSignedInternal(signedSubmission: SignedContent[SubmissionRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SequencerDeliverError, Unit]

  override def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource] =
    readInternal(member, offset)

  override def readV2(member: Member, timestamp: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource] =
    readInternalV2(member, timestamp)

  protected def readInternal(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource]

  protected def readInternalV2(member: Member, timestamp: Option[CantonTimestamp])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, CreateSubscriptionError, Sequencer.SequencedEventSource]

  override def onClosed(): Unit =
    periodicHealthCheck.foreach(LifeCycle.close(_)(logger))

}
