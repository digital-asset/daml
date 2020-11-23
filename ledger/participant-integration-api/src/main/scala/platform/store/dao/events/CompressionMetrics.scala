// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.codahale.metrics.Histogram
import com.daml.metrics.Metrics

final class CompressionMetrics(
    val createArgumentCompressionRatio: Histogram,
    val createKeyValueCompressionRatio: Histogram,
    val exerciseArgumentCompressionRatio: Histogram,
    val exerciseResultCompressionRatio: Histogram,
)

object CompressionMetrics {

  def apply(metrics: Metrics): CompressionMetrics = {
    new CompressionMetrics(
      createArgumentCompressionRatio = metrics.daml.index.db.compression.ratio.createArgument,
      createKeyValueCompressionRatio = metrics.daml.index.db.compression.ratio.createKeyValue,
      exerciseArgumentCompressionRatio = metrics.daml.index.db.compression.ratio.exerciseArgument,
      exerciseResultCompressionRatio = metrics.daml.index.db.compression.ratio.exerciseResult,
    )
  }

}
