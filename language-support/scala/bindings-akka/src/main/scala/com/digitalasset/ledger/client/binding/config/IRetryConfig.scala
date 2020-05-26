// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding.config

import scala.concurrent.duration._

trait IRetryConfig {

  /**
    * @return The interval between retries.
    */
  def intervalMs: Positive[Long]
  def interval: FiniteDuration = intervalMs.value.millis

  /**
    * @return The total timeout we allow for the operation to succeed.
    */
  def timeoutMs: Positive[Long]
  def timeout: FiniteDuration = timeoutMs.value.millis
}
