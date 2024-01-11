// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import java.time.Duration

final case class LedgerEffectiveTimeTolerance(transactionLatency: Duration, skew: Duration)
    extends ToleranceWindow {

  override val toleranceInPast: Duration = transactionLatency.plus(skew)

  override val toleranceInFuture: Duration = skew

}
