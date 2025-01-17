// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.logging.ErrorLoggingContext

/** Policy for what errors are considered retryable.
  * Each [[transports.SequencerClientTransport]] is expected to supply
  * their own policy which can consider error types they have defined.
  */
trait SubscriptionErrorRetryPolicyPekko[-E] {
  def retryOnError(subscriptionError: E, receivedItems: Boolean)(implicit
      loggingContext: ErrorLoggingContext
  ): Boolean

  def retryOnException(ex: Throwable)(implicit
      loggingContext: ErrorLoggingContext
  ): Boolean
}

object SubscriptionErrorRetryPolicyPekko {
  case object never extends SubscriptionErrorRetryPolicyPekko[Any] {
    override def retryOnError(subscriptionError: Any, receivedItems: Boolean)(implicit
        loggingContext: ErrorLoggingContext
    ): Boolean = false

    override def retryOnException(ex: Throwable)(implicit
        loggingContext: ErrorLoggingContext
    ): Boolean = false
  }
}
