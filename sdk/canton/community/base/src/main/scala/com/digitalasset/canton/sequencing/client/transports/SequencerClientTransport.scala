// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.{
  SendAcknowledgementError,
  SendAsyncClientError,
  SequencerSubscription,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.handshake.SupportsHandshake
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** Implementation dependent operations for a client to write to a domain sequencer. */
trait SequencerClientTransportCommon extends FlagCloseable with SupportsHandshake {

  /** Sends a submission request to the sequencer.
    * If we failed to make the request, an error will be returned.
    * If the sequencer accepted (or may have accepted) the request this call will return successfully.
    * This is about to be deprecated in favor of [[sendAsyncSigned]]
    */
  def sendAsync(request: SubmissionRequest, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit]

  /** Sends a signed submission request to the sequencer.
    * If we failed to make the request, an error will be returned.
    * If the sequencer accepted (or may have accepted) the request this call will return successfully.
    * This is replacing [[sendAsync]]
    */
  def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit]

  def sendAsyncUnauthenticated(
      request: SubmissionRequest,
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit]

  def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAcknowledgementError, Unit]
}

/** Implementation dependent operations for a client to read and write to a domain sequencer. */
trait SequencerClientTransport extends SequencerClientTransportCommon {

  /** Create a single subscription to read events from the Sequencer for this member starting from the counter defined in the request.
    * Transports are currently responsible for calling the supplied handler.
    * The handler must not be called concurrently and must receive events in-order.
    * If the handler fails with an exception the subscription should close with a [[com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.HandlerError]].
    * If the subscription fails for a technical reason it should close with a [[com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.SubscriptionError]].
    * The transport is not expected to provide retries of subscriptions.
    */
  def subscribe[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(implicit
      traceContext: TraceContext
  ): SequencerSubscription[E]

  def subscribeUnauthenticated[E](request: SubscriptionRequest, handler: SerializedEventHandler[E])(
      implicit traceContext: TraceContext
  ): SequencerSubscription[E]

  /** The transport can decide which errors will cause the sequencer client to not try to reestablish a subscription */
  def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy
}
