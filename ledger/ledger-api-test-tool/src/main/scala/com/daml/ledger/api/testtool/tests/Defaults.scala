// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.tests

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object Defaults {

  val LedgerClockGranularity: FiniteDuration = 1.second

  val TimeoutScaleFactor: Double = 1.0

}
