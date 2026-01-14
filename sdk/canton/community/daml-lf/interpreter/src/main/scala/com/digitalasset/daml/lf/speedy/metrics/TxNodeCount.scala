// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.metrics

import scala.annotation.unused

final class TxNodeCount extends MetricPlugin {
  type Result = Long

  private[this] var txNodeCount: Long = 0

  override def incrCount(@unused ctx: MetricPlugin.Ctx*): Unit = {
    txNodeCount += 1
  }

  override def totalCount: Result = {
    txNodeCount
  }
}
