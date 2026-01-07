// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.store.serialization.Compression

import java.io.ByteArrayOutputStream

final case class CompressionStrategy(
    consumingExerciseArgumentCompression: FieldCompressionStrategy,
    consumingExerciseResultCompression: FieldCompressionStrategy,
    nonConsumingExerciseArgumentCompression: FieldCompressionStrategy,
    nonConsumingExerciseResultCompression: FieldCompressionStrategy,
)

object CompressionStrategy {

  def none(metrics: LedgerApiServerMetrics): CompressionStrategy =
    buildUniform(Compression.Algorithm.None, metrics)

  def allGZIP(metrics: LedgerApiServerMetrics): CompressionStrategy =
    buildUniform(Compression.Algorithm.GZIP, metrics)

  def buildFromConfig(
      metrics: LedgerApiServerMetrics
  )(consumingExercise: Boolean, nonConsumingExercise: Boolean): CompressionStrategy = {
    val consumingAlgorithm: Compression.Algorithm =
      if (consumingExercise) Compression.Algorithm.GZIP else Compression.Algorithm.None
    val nonConsumingAlgorithm: Compression.Algorithm =
      if (nonConsumingExercise) Compression.Algorithm.GZIP else Compression.Algorithm.None
    build(
      consumingAlgorithm,
      consumingAlgorithm,
      nonConsumingAlgorithm,
      nonConsumingAlgorithm,
      metrics,
    )
  }

  def buildUniform(
      algorithm: Compression.Algorithm,
      metrics: LedgerApiServerMetrics,
  ): CompressionStrategy =
    build(algorithm, algorithm, algorithm, algorithm, metrics)

  def build(
      consumingExerciseArgumentAlgorithm: Compression.Algorithm,
      consumingExerciseResultAlgorithm: Compression.Algorithm,
      nonConsumingExerciseArgumentAlgorithm: Compression.Algorithm,
      nonConsumingExerciseResultAlgorithm: Compression.Algorithm,
      metrics: LedgerApiServerMetrics,
  ): CompressionStrategy = CompressionStrategy(
    consumingExerciseArgumentCompression = FieldCompressionStrategy(
      consumingExerciseArgumentAlgorithm,
      CompressionMetrics.exerciseArgument(metrics),
    ),
    consumingExerciseResultCompression = FieldCompressionStrategy(
      consumingExerciseResultAlgorithm,
      CompressionMetrics.exerciseResult(metrics),
    ),
    nonConsumingExerciseArgumentCompression = FieldCompressionStrategy(
      nonConsumingExerciseArgumentAlgorithm,
      CompressionMetrics.exerciseArgument(metrics),
    ),
    nonConsumingExerciseResultCompression = FieldCompressionStrategy(
      nonConsumingExerciseResultAlgorithm,
      CompressionMetrics.exerciseResult(metrics),
    ),
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
