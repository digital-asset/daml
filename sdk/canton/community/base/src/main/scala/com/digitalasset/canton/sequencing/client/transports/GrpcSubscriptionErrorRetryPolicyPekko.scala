// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.client.SubscriptionErrorRetryPolicyPekko
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientTransportPekko.GrpcSequencerSubscriptionError

class GrpcSubscriptionErrorRetryPolicyPekko
    extends SubscriptionErrorRetryPolicyPekko[GrpcSequencerSubscriptionError] {
  override def retryOnError(
      subscriptionError: GrpcSequencerSubscriptionError,
      receivedItems: Boolean,
  )(implicit loggingContext: ErrorLoggingContext): Boolean = {
    subscriptionError match {
      case GrpcSequencerClientTransportPekko.ExpectedGrpcFailure(error) =>
        GrpcSubscriptionErrorRetryPolicy.logAndDetermineRetry(error, receivedItems)
      case GrpcSequencerClientTransportPekko.UnexpectedGrpcFailure(ex) =>
        loggingContext.error(s"Unexpected error type: $ex")
        false
      case GrpcSequencerClientTransportPekko.ResponseParseError(error) =>
        loggingContext.error(s"Failed to parse sequenced event: $error")
        false
    }
  }

  override def retryOnException(ex: Throwable)(implicit
      loggingContext: ErrorLoggingContext
  ): Boolean = false
}
