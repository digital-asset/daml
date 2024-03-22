// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.runner

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object Defaults {

  val LedgerClockGranularity: FiniteDuration = 1.second

  val TimeoutScaleFactor: Double = 1.0

  // Neither ledgers nor participants scale perfectly with the number of processors.
  // We therefore limit the maximum number of concurrent tests, to avoid overwhelming the ledger.
  val ConcurrentRuns: Int = sys.runtime.availableProcessors min 4

}
