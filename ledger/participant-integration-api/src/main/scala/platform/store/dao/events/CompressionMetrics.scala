// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.codahale.metrics.Histogram
import com.daml.metrics.Metrics

object CompressionMetrics {

  final class Field(val compressed: Histogram, val uncompressed: Histogram)

  def createArgument(metrics: Metrics): CompressionMetrics.Field =
    new Field(
      compressed = metrics.daml.index.db.compression.createArgumentCompressed,
      uncompressed = metrics.daml.index.db.compression.createArgumentUncompressed,
    )

  def createKeyValue(metrics: Metrics) =
    new Field(
      compressed = metrics.daml.index.db.compression.createKeyValueCompressed,
      uncompressed = metrics.daml.index.db.compression.createKeyValueUncompressed,
    )

  def exerciseArgument(metrics: Metrics) =
    new Field(
      compressed = metrics.daml.index.db.compression.exerciseArgumentCompressed,
      uncompressed = metrics.daml.index.db.compression.exerciseArgumentUncompressed,
    )

  def exerciseResult(metrics: Metrics) =
    new Field(
      compressed = metrics.daml.index.db.compression.exerciseResultCompressed,
      uncompressed = metrics.daml.index.db.compression.exerciseResultUncompressed,
    )
}
