// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.daml.metrics.Metrics
import com.daml.metrics.api.MetricHandle.Histogram

object CompressionMetrics {

  final class Field(val compressed: Histogram, val uncompressed: Histogram)

  def createArgument(metrics: Metrics): CompressionMetrics.Field =
    new Field(
      compressed = metrics.daml.index.db.main.compression.createArgumentCompressed,
      uncompressed = metrics.daml.index.db.main.compression.createArgumentUncompressed,
    )

  def createKeyValue(metrics: Metrics) =
    new Field(
      compressed = metrics.daml.index.db.main.compression.createKeyValueCompressed,
      uncompressed = metrics.daml.index.db.main.compression.createKeyValueUncompressed,
    )

  def exerciseArgument(metrics: Metrics) =
    new Field(
      compressed = metrics.daml.index.db.main.compression.exerciseArgumentCompressed,
      uncompressed = metrics.daml.index.db.main.compression.exerciseArgumentUncompressed,
    )

  def exerciseResult(metrics: Metrics) =
    new Field(
      compressed = metrics.daml.index.db.main.compression.exerciseResultCompressed,
      uncompressed = metrics.daml.index.db.main.compression.exerciseResultUncompressed,
    )
}
