// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.configuration

import java.time.Duration

object LedgerTimeModel {

  object maximumToleranceTimeModel {
    // The expected average latency of a transaction, i.e., the average
    // time from submitting the transaction to a write service and the
    // transaction being assigned a record time.
    val avgTransactionLatency = Duration.ZERO
  }

}
