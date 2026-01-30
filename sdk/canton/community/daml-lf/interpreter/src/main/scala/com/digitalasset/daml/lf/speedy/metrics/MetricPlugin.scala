// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.speedy.metrics

trait MetricPlugin {
  type Result

  def incrCount(ctx: MetricPlugin.Ctx*): Unit

  def totalCount: Result
}

object MetricPlugin {
  trait Ctx
}
