// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao.events

import com.codahale.metrics.Histogram
import com.daml.metrics.Metrics

final class CompressionMetrics(
    val createArgument: CompressionMetrics.Field,
    val createKeyValue: CompressionMetrics.Field,
    val exerciseArgument: CompressionMetrics.Field,
    val exerciseResult: CompressionMetrics.Field,
)

object CompressionMetrics {

  final class Field(val compressed: Histogram, val uncompressed: Histogram)

  def apply(metrics: Metrics): CompressionMetrics = {
    new CompressionMetrics(
      createArgument = new Field(
        compressed = metrics.daml.index.db.compression.createArgumentCompressed,
        uncompressed = metrics.daml.index.db.compression.createArgumentUncompressed,
      ),
      createKeyValue = new Field(
        compressed = metrics.daml.index.db.compression.createKeyValueCompressed,
        uncompressed = metrics.daml.index.db.compression.createKeyValueUncompressed,
      ),
      exerciseArgument = new Field(
        compressed = metrics.daml.index.db.compression.exerciseArgumentCompressed,
        uncompressed = metrics.daml.index.db.compression.exerciseArgumentUncompressed,
      ),
      exerciseResult = new Field(
        compressed = metrics.daml.index.db.compression.exerciseResultCompressed,
        uncompressed = metrics.daml.index.db.compression.exerciseResultUncompressed,
      ),
    )
  }

}
