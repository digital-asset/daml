// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.sequencing.client.{
  SequencerSubscriptionAkka,
  SubscriptionErrorRetryPolicyAkka,
}
import com.digitalasset.canton.sequencing.protocol.SubscriptionRequest
import com.digitalasset.canton.tracing.TraceContext

/** Implementation dependent operations for a client to read and write to a domain sequencer. */
trait SequencerClientTransportAkka extends SequencerClientTransportCommon {

  type SubscriptionError

  /** Create a single subscription to read events from the Sequencer for this member starting from the counter defined in the request.
    * The transport is not expected to provide retries of subscriptions.
    */
  def subscribe(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionAkka[SubscriptionError]

  def subscribeUnauthenticated(request: SubscriptionRequest)(implicit
      traceContext: TraceContext
  ): SequencerSubscriptionAkka[SubscriptionError]

  /** The transport can decide which errors will cause the sequencer client to not try to reestablish a subscription */
  def subscriptionRetryPolicyAkka: SubscriptionErrorRetryPolicyAkka[SubscriptionError]

}
