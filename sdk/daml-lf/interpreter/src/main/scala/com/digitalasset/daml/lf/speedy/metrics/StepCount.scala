// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.metrics

final class StepCount(val batchSize: Long) extends MetricPlugin {
  type Result = Long

  // Speedy evaluates in steps which are grouped into batches of batchSize
  private[this] var stepBatchCount: Long = 0
  private[this] var stepCount: Long = 0

  override def incrCount(ctx: MetricPlugin.Ctx*): Unit = ctx match {
    case Seq(StepCount.StepCtx) =>
      stepCount += 1
    case Seq(StepCount.BatchCtx) =>
      stepBatchCount += 1
      stepCount = 0
    case _ =>
  }

  override def totalCount: Result = stepBatchCount * batchSize + stepCount
}

object StepCount {
  sealed trait Ctx extends MetricPlugin.Ctx

  case object BatchCtx extends Ctx

  case object StepCtx extends Ctx
}
