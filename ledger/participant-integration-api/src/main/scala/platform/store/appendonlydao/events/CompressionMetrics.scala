// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import com.codahale.metrics.Histogram
import com.daml.metrics.ParticipantMetrics

object CompressionMetrics {

  final class Field(val compressed: Histogram, val uncompressed: Histogram)

  def createArgument(metrics: ParticipantMetrics): CompressionMetrics.Field =
    new Field(
      compressed = metrics.daml.index.db.compression.createArgumentCompressed,
      uncompressed = metrics.daml.index.db.compression.createArgumentUncompressed,
    )

  def createKeyValue(metrics: ParticipantMetrics) =
    new Field(
      compressed = metrics.daml.index.db.compression.createKeyValueCompressed,
      uncompressed = metrics.daml.index.db.compression.createKeyValueUncompressed,
    )

  def exerciseArgument(metrics: ParticipantMetrics) =
    new Field(
      compressed = metrics.daml.index.db.compression.exerciseArgumentCompressed,
      uncompressed = metrics.daml.index.db.compression.exerciseArgumentUncompressed,
    )

  def exerciseResult(metrics: ParticipantMetrics) =
    new Field(
      compressed = metrics.daml.index.db.compression.exerciseResultCompressed,
      uncompressed = metrics.daml.index.db.compression.exerciseResultUncompressed,
    )
}
