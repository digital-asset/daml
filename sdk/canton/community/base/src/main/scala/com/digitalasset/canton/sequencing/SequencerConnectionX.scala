// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import cats.data.EitherT
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.sequencing.ConnectionX.ConnectionXConfig
import com.digitalasset.canton.sequencing.InternalSequencerConnectionX.{
  ConnectionAttributes,
  SequencerConnectionXHealth,
}
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.SendAsyncClientResponseError
import com.digitalasset.canton.sequencing.client.{
  SequencerSubscription,
  SubscriptionErrorRetryPolicy,
}
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  GetTrafficStateForMemberRequest,
  GetTrafficStateForMemberResponse,
  SignedContent,
  SubmissionRequest,
  SubscriptionRequest,
  TopologyStateForInitHashResponse,
  TopologyStateForInitRequest,
  TopologyStateForInitResponse,
}
import com.digitalasset.canton.tracing.TraceContext
import io.grpc.Status

import scala.concurrent.duration.Duration

/** A connection to a sequencer. This trait attempts to be independent of the underlying transport.
  *
  * NOTE: We currently make only a minimal effort to keep transport independence, and there are
  * obvious leaks. This will be extended when we need it.
  */
trait SequencerConnectionX extends FlagCloseable with NamedLogging {

  def name: String

  def health: SequencerConnectionXHealth

  def config: ConnectionXConfig

  def attributes: ConnectionAttributes

  def fail(reason: String)(implicit traceContext: TraceContext): Unit

  def fatal(reason: String)(implicit traceContext: TraceContext): Unit

  def sendAsync(
      request: SignedContent[SubmissionRequest],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SendAsyncClientResponseError, Unit]

  def acknowledgeSigned(signedRequest: SignedContent[AcknowledgeRequest], timeout: Duration)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Boolean]

  def getTrafficStateForMember(request: GetTrafficStateForMemberRequest, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GetTrafficStateForMemberResponse]

  def logout()(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, Status, Unit]

  /** Fetches the "current" sequencing time */
  def getTime(timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Option[CantonTimestamp]]

  def downloadTopologyStateForInit(request: TopologyStateForInitRequest, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitResponse]

  def downloadTopologyStateForInitHash(request: TopologyStateForInitRequest, timeout: Duration)(
      implicit traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, TopologyStateForInitHashResponse]

  def subscribe[E](
      request: SubscriptionRequest,
      handler: SequencedEventHandler[E],
      timeout: Duration,
  )(implicit
      traceContext: TraceContext
  ): Either[String, SequencerSubscription[E]]

  /** Determine which errors will cause the sequencer client to not try to reestablish a
    * subscription
    */
  def subscriptionRetryPolicy: SubscriptionErrorRetryPolicy
}
