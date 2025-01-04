// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.mediator

import cats.syntax.option.*
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt

/** Configuration for the mediator.
  *
  * @param pruning mediator pruning configuration
  */
final case class MediatorConfig(
    pruning: MediatorPruningConfig = MediatorPruningConfig()
)

/** Configuration for mediator pruning
  *
  * @param maxPruningBatchSize         Maximum number of events to prune from a mediator at a time, used to break up batches internally
  * @param pruningMetricUpdateInterval How frequently to update the `max-event-age` pruning progress metric in the background.
  *                                    A setting of None disables background metric updating.
  */
final case class MediatorPruningConfig(
    maxPruningBatchSize: PositiveInt =
      PositiveInt.tryCreate(50000), // Large default for database-range-delete based pruning
    pruningMetricUpdateInterval: Option[config.PositiveDurationSeconds] =
      config.PositiveDurationSeconds.ofHours(1L).some,
)
