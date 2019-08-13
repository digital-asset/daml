// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.api.util

import java.time.Duration

final case class LedgerEffectiveTimeTolerance(transactionLatency: Duration, skew: Duration)
    extends ToleranceWindow {

  override val toleranceInPast: Duration = transactionLatency.plus(skew)

  override val toleranceInFuture: Duration = skew

}
