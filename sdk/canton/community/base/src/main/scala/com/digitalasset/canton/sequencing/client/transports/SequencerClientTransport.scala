// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import cats.data.EitherT
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.sequencing.SerializedEventHandler
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.{
  SequencerSubscription,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** Implementation dependent operations for a client to write to a domain sequencer. */
trait SequencerClientTransportCommon extends FlagCloseable {

  /** Revoke all the authentication tokens on this sequencer and close the connection.
    */
  def logout(): EitherT[FutureUnlessShutdown, Status, Unit]

  /** Sends a signed submission request to the sequencer.
    * If we failed to make the request, an error will be returned.
    * If the sequencer accepted (or may have accepted) the request this call will return successfully.
    */
  def sendAsyncSigned(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit]

  /** Acknowledge that we have successfully processed all events up to and including the given timestamp.
    * The client should then never subscribe for events from before this point.
    *
    * @return True if acknowledgement succeeded, false if sequencer was unavailable
    */
  def acknowledgeSigned(request: SignedContent[AcknowledgeRequest])(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean]

  def getTrafficStateForMember(request: GetTrafficStateForMemberRequest)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse]

  def downloadTopologyStateForInit(request: TopologyStateForInitRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, TopologyStateForInitResponse]
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

  /** The transport can decide which errors will cause the sequencer client to not try to reestablish a subscription */
  def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy
}
