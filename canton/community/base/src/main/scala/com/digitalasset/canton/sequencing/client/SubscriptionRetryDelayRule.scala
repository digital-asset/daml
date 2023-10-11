// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import scala.concurrent.duration.FiniteDuration

/** Calculator for how to select the next retry duration and specifies what duration is enough to log a warning. */
trait SubscriptionRetryDelayRule {

  /** What should the first delay be */
  val initialDelay: FiniteDuration

  /** If we retry for a duration greater than this value then a warning will be logged */
  val warnDelayDuration: FiniteDuration

  /** Calculate the next retry delay given the prior and knowing whether an event has been received on the last subscription (suggesting that it did successfully connect and read). */
  def nextDelay(previousDelay: FiniteDuration, hasReceivedEvent: Boolean): FiniteDuration
}

object SubscriptionRetryDelayRule {
  def apply(
      initialRetryDelay: FiniteDuration,
      warnDelay: FiniteDuration,
      maxRetryDelay: FiniteDuration,
  ): SubscriptionRetryDelayRule =
    new SubscriptionRetryDelayRule {

      override val initialDelay: FiniteDuration = initialRetryDelay
      override val warnDelayDuration: FiniteDuration = warnDelay

      override def nextDelay(
          previousDelay: FiniteDuration,
          hasReceivedEvent: Boolean,
      ): FiniteDuration =
        // reset delay
        if (hasReceivedEvent) initialRetryDelay
        else {
          // increase delay
          (previousDelay * 2) min maxRetryDelay
        }
    }
}
