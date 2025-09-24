// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.syntax.option.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  BatchAggregatorConfig,
  CantonConfigValidator,
  PositiveFiniteDuration,
  UniformCantonConfigValidation,
}

/** Configuration for the mediator.
  *
  * @param pruning
  *   mediator pruning configuration
  */
final case class MediatorConfig(
    pruning: MediatorPruningConfig = MediatorPruningConfig(),
    deduplicationStore: DeduplicationStoreConfig = DeduplicationStoreConfig(),
) extends UniformCantonConfigValidation

object MediatorConfig {
  implicit val mediatorConfigCantonConfigValidator: CantonConfigValidator[MediatorConfig] =
    CantonConfigValidatorDerivation[MediatorConfig]
}

/** Configuration for mediator pruning
  *
  * @param maxPruningBatchSize
  *   Maximum number of events to prune from a mediator at a time, used to break up batches
  *   internally
  * @param pruningMetricUpdateInterval
  *   How frequently to update the `max-event-age` pruning progress metric in the background. A
  *   setting of None disables background metric updating.
  */
final case class MediatorPruningConfig(
    maxPruningBatchSize: PositiveInt =
      PositiveInt.tryCreate(50000), // Large default for database-range-delete based pruning
    pruningMetricUpdateInterval: Option[config.PositiveDurationSeconds] =
      config.PositiveDurationSeconds.ofHours(1L).some,
) extends UniformCantonConfigValidation

object MediatorPruningConfig {
  implicit val mediatorPruningConfigCantonConfigValidator
      : CantonConfigValidator[MediatorPruningConfig] = {
    import com.digitalasset.canton.config.CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[MediatorPruningConfig]
  }
}

/** Configuration for deduplication store
  * @param pruneAtMostEvery
  *   An interval to throttle pruning operations to at most once per this duration
  * @param persistBatching
  *   Configuration for batching of persist operation in the deduplication store
  */
final case class DeduplicationStoreConfig(
    pruneAtMostEvery: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(10),
    persistBatching: BatchAggregatorConfig = BatchAggregatorConfig(),
) extends UniformCantonConfigValidation

object DeduplicationStoreConfig {
  implicit val deduplicationStoreConfigCantonConfigValidator
      : CantonConfigValidator[DeduplicationStoreConfig] =
    CantonConfigValidatorDerivation[DeduplicationStoreConfig]
}
