// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.transports

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.client.SubscriptionErrorRetryPolicyAkka
import com.digitalasset.canton.sequencing.client.transports.GrpcSequencerClientTransportAkka.GrpcSequencerSubscriptionError

class GrpcSubscriptionErrorRetryPolicyAkka
    extends SubscriptionErrorRetryPolicyAkka[GrpcSequencerSubscriptionError] {
  override def retryOnError(
      subscriptionError: GrpcSequencerSubscriptionError,
      receivedItems: Boolean,
  )(implicit loggingContext: ErrorLoggingContext): Boolean = {
    subscriptionError match {
      case GrpcSequencerClientTransportAkka.ExpectedGrpcFailure(error) =>
        GrpcSubscriptionErrorRetryPolicy.logAndDetermineRetry(error, receivedItems)
      case GrpcSequencerClientTransportAkka.UnexpectedGrpcFailure(ex) =>
        loggingContext.error(s"Unexpected error type: $ex")
        false
      case GrpcSequencerClientTransportAkka.ResponseParseError(error) =>
        loggingContext.error(s"Failed to parse sequenced event: $error")
        false
    }
  }

  override def retryOnException(ex: Throwable)(implicit
      loggingContext: ErrorLoggingContext
  ): Boolean = false
}
