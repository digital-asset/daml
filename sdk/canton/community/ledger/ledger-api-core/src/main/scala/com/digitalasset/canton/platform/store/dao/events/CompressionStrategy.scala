// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.serialization.Compression

import java.io.ByteArrayOutputStream

final case class CompressionStrategy(
    createArgumentCompression: FieldCompressionStrategy,
    createKeyValueCompression: FieldCompressionStrategy,
    exerciseArgumentCompression: FieldCompressionStrategy,
    exerciseResultCompression: FieldCompressionStrategy,
)

object CompressionStrategy {

  def none(metrics: LedgerApiServerMetrics): CompressionStrategy =
    buildUniform(Compression.Algorithm.None, metrics)

  def allGZIP(metrics: LedgerApiServerMetrics): CompressionStrategy =
    buildUniform(Compression.Algorithm.GZIP, metrics)

  def buildUniform(
      algorithm: Compression.Algorithm,
      metrics: LedgerApiServerMetrics,
  ): CompressionStrategy =
    build(algorithm, algorithm, algorithm, algorithm, metrics)

  def build(
      createArgumentAlgorithm: Compression.Algorithm,
      createKeyValueAlgorithm: Compression.Algorithm,
      exerciseArgumentAlgorithm: Compression.Algorithm,
      exerciseResultAlgorithm: Compression.Algorithm,
      metrics: LedgerApiServerMetrics,
  ): CompressionStrategy = CompressionStrategy(
    createArgumentCompression =
      FieldCompressionStrategy(createArgumentAlgorithm, CompressionMetrics.createArgument(metrics)),
    createKeyValueCompression =
      FieldCompressionStrategy(createKeyValueAlgorithm, CompressionMetrics.createKeyValue(metrics)),
    exerciseArgumentCompression = FieldCompressionStrategy(
      exerciseArgumentAlgorithm,
      CompressionMetrics.exerciseArgument(metrics),
    ),
    exerciseResultCompression =
      FieldCompressionStrategy(exerciseResultAlgorithm, CompressionMetrics.exerciseResult(metrics)),
  )
}

final case class FieldCompressionStrategy(id: Option[Int], compress: Array[Byte] => Array[Byte])

object FieldCompressionStrategy {
  def apply(a: Compression.Algorithm, metric: CompressionMetrics.Field): FieldCompressionStrategy =
    FieldCompressionStrategy(
      a.id,
      uncompressed => {
        val output = new ByteArrayOutputStream(uncompressed.length)
        val gzip = a.compress(output)
        try {
          gzip.write(uncompressed)
        } finally {
          gzip.close()
        }
        val compressed = output.toByteArray
        output.close()
        metric.compressed.update(compressed.length)(MetricsContext.Empty)
        metric.uncompressed.update(uncompressed.length)(MetricsContext.Empty)
        compressed
      },
    )

}
