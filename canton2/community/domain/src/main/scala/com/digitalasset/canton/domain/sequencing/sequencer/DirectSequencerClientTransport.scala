// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.sequencing.service.DirectSequencerSubscriptionFactory
import com.digitalasset.canton.lifecycle.SyncCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.client.transports.SequencerClientTransport
import com.digitalasset.canton.sequencing.handshake.HandshakeRequestError
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  HandshakeRequest,
  HandshakeResponse,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, FutureUtil}
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** This transport is meant to be used to create a sequencer client that connects directly to an in-process sequencer.
  * Needed for cases when the sequencer node itself needs to listen to specific events such as identity events.
  */
class DirectSequencerClientTransport(
    sequencer: Sequencer,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer, ec: ExecutionContext)
    extends SequencerClientTransport
    with NamedLogging {

  private val subscriptionFactory =
    new DirectSequencerSubscriptionFactory(sequencer, timeouts, loggerFactory)

  override def sendAsync(
      request: SubmissionRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] =
    sequencer
      .sendAsync(request)
      .leftMap(SendAsyncClientError.RequestRefused)

  override def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] =
    sequencer
      .sendAsyncSigned(request)
      .leftMap(SendAsyncClientError.RequestRefused)

  override def sendAsyncUnauthenticated(
      request: SubmissionRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] =
    EitherT.leftT(
      SendAsyncClientError.RequestInvalid("Direct client does not support unauthenticated sends")
    )

  override def acknowledge(request: AcknowledgeRequest)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    sequencer.acknowledge(request.member, request.timestamp)

  override def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    sequencer.acknowledgeSigned(request)

  override def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E] = new SequencerSubscription[E] {

    override protected def timeouts: ProcessingTimeout =
      DirectSequencerClientTransport.this.timeouts

    override protected val loggerFactory: NamedLoggerFactory =
      DirectSequencerClientTransport.this.loggerFactory

    private val subscriptionRef = new AtomicReference[Option[SequencerSubscription[E]]](None)

    {
      val subscriptionET =
        subscriptionFactory
          .create(
            request.counter,
            "direct",
            request.member,
            {
              case Right(event) => handler(event)
              case Left(error) =>
                ErrorUtil.invalidState(
                  s"Direct transport subscriptions must not trigger subscription errors such as ${error}"
                )
            },
          )
          .thereafter {
            case Success(Right(subscription)) =>
              closeReasonPromise.completeWith(subscription.closeReason)

              performUnlessClosing(functionFullName) {
                subscriptionRef.set(Some(subscription))
              } onShutdown {
                subscription.close()
              }
            case Success(Left(value)) =>
              closeReasonPromise.trySuccess(Fatal(value.toString)).discard[Boolean]
            case Failure(exception) =>
              closeReasonPromise.tryFailure(exception).discard[Boolean]
          }
      FutureUtil.doNotAwait(
        subscriptionET.value,
        s"creating the direct sequencer subscription for $request",
      )
    }

    override protected def closeAsync(): Seq[SyncCloseable] = Seq(
      SyncCloseable(
        "direct-sequencer-client-transport",
        subscriptionRef.get().foreach(_.close()),
      )
    )

    override private[canton] def complete(
        reason: SubscriptionCloseReason[E]
    )(implicit traceContext: TraceContext): Unit = {
      subscriptionRef.get().foreach(_.complete(reason))
      close()
    }
  }

  override def subscribeUnauthenticated[E](
      request: SubscriptionRequest,
      handler: SerializedEventHandler[E],
  )(implicit traceContext: TraceContext): SequencerSubscription[E] =
    ErrorUtil.internalError(
      new UnsupportedOperationException(
        "Direct client does not support unauthenticated subscriptions"
      )
    )

  override def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy =
    new SubscriptionErrorRetryPolicy {
      override def retryOnError(
          subscriptionError: SubscriptionCloseReason.SubscriptionError,
          receivedItems: Boolean,
      )(implicit traceContext: TraceContext): Boolean =
        false // unlikely there will be any errors with this direct transport implementation
    }

  override def handshake(request: HandshakeRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HandshakeRequestError, HandshakeResponse] =
    // never called - throwing an exception so tests fail if this ever changes
    throw new UnsupportedOperationException(
      "handshake is not implemented for DirectSequencerClientTransport"
    )

  override def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, TopologyStateForInitResponse] =
    throw new UnsupportedOperationException(
      "downloadTopologyStateForInit is not implemented for DirectSequencerClientTransport"
    )
}
