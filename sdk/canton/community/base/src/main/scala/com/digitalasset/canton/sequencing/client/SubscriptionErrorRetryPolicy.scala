// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason.SubscriptionError
import com.digitalasset.canton.tracing.TraceContext

import scala.reflect.ClassTag

/** Policy for what errors are considered retryable.
  * Each [[transports.SequencerClientTransport]] is expected to supply
  * their own policy which can consider error types they have defined.
  */
trait SubscriptionErrorRetryPolicy {
  def retryOnError(subscriptionError: SubscriptionError, receivedItems: Boolean)(implicit
      traceContext: TraceContext
  ): Boolean

  def retryOnException(ex: Throwable, Logger: TracedLogger)(implicit
      traceContext: TraceContext
  ): Boolean = false
}

/** Allows implementors to only specify policy for an error hierarchy they've defined.
  * Avoids adding type parameters to all sequencer client components.
  * TODO(11067): work out if type parameters are really required and if so are they that bad
  */
abstract class CheckedSubscriptionErrorRetryPolicy[SE <: SubscriptionError](implicit
    classTag: ClassTag[SE]
) extends SubscriptionErrorRetryPolicy {
  override def retryOnError(error: SubscriptionError, receivedItems: Boolean)(implicit
      traceContext: TraceContext
  ): Boolean =
    error match {
      case expectedError: SE => retryInternal(expectedError, receivedItems)
      case unexpectedType => sys.error(s"Unexpected error type: $unexpectedType")
    }

  protected def retryInternal(error: SE, receivedItems: Boolean)(implicit
      traceContext: TraceContext
  ): Boolean
}

object SubscriptionErrorRetryPolicy {

  /** Never retry on any error */
  def never: SubscriptionErrorRetryPolicy = new SubscriptionErrorRetryPolicy {
    override def retryOnError(subscriptionError: SubscriptionError, receivedItems: Boolean)(implicit
        traceContext: TraceContext
    ): Boolean = false
  }
}
