// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.daml.ledger.api.testtool.runner

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object Defaults {

  val LedgerClockGranularity: FiniteDuration = 1.second

  val TimeoutScaleFactor: Double = 1.0

  // Neither ledgers nor participants scale perfectly with the number of processors.
  // We therefore limit the maximum number of concurrent tests, to avoid overwhelming the ledger.
  val ConcurrentRuns: Int = sys.runtime.availableProcessors min 4

}
