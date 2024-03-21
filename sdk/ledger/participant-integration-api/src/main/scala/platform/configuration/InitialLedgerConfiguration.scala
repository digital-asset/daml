// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.configuration

import java.time.Duration
import com.daml.ledger.configuration.{Configuration, LedgerTimeModel}

/** Instructions on how to initialize an empty ledger, without a configuration.
  *
  * A configuation is only submitted if one is not detected on the ledger.
  *
  * @param generation                The configuration generation. Monotonically increasing.
  * @param maxDeduplicationDuration  The maximum time window during which commands can be deduplicated.
  * @param avgTransactionLatency  The expected average latency of a transaction, i.e., the average
  *                               time from submitting the transaction to a write service and the
  *                               transaction being assigned a record time.
  * @param minSkew                The minimimum skew between ledger time and record time:
  *                               lt_TX >= rt_TX - minSkew
  * @param maxSkew                The maximum skew between ledger time and record time:
  *                               lt_TX <= rt_TX + maxSkew
  * @param delayBeforeSubmitting The delay until the participant tries to submit a configuration.
  */
final case class InitialLedgerConfiguration(
    maxDeduplicationDuration: Duration,
    avgTransactionLatency: Duration,
    minSkew: Duration,
    maxSkew: Duration,
    delayBeforeSubmitting: Duration,
) {
  def toConfiguration = Configuration(
    generation = 1L,
    timeModel = LedgerTimeModel.apply(avgTransactionLatency, minSkew, maxSkew).get,
    maxDeduplicationDuration = maxDeduplicationDuration,
  )
}

object InitialLedgerConfiguration {}
