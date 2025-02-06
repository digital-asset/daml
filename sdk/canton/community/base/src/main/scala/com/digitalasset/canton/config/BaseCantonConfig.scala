// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import pureconfig.{ConfigReader, ConfigWriter}

/** Base config readers and writers used in different places
  *
  * The main bulk of config readers and writers is in CantonConfig. However, some
  * are shared with the sequencer driver. In order to avoid duplication, we
  * create them here so they can be imported from both places.
  */
object BaseCantonConfig {

  import pureconfig.generic.semiauto.*

  object Readers {

    lazy implicit val batchAggregatorConfigReader: ConfigReader[BatchAggregatorConfig] = {
      implicit val batching: ConfigReader[BatchAggregatorConfig.Batching] =
        deriveReader[BatchAggregatorConfig.Batching]
      implicit val noBatching: ConfigReader[BatchAggregatorConfig.NoBatching.type] =
        deriveReader[BatchAggregatorConfig.NoBatching.type]
      deriveReader[BatchAggregatorConfig]
    }

    lazy implicit final val batchingReader: ConfigReader[BatchingConfig] =
      deriveReader[BatchingConfig]

    lazy implicit final val connectionAllocationReader: ConfigReader[ConnectionAllocation] =
      deriveReader[ConnectionAllocation]

    lazy implicit final val dbParamsReader: ConfigReader[DbParametersConfig] =
      deriveReader[DbParametersConfig]

    lazy implicit val queryCostMonitoringConfigReader: ConfigReader[QueryCostMonitoringConfig] = {
      lazy implicit val queryCostSortByConfigReader: ConfigReader[QueryCostSortBy] =
        deriveEnumerationReader[QueryCostSortBy]
      deriveReader[QueryCostMonitoringConfig]
    }
  }

  object Writers {

    lazy implicit val batchAggregatorConfigWriter: ConfigWriter[BatchAggregatorConfig] = {
      implicit val batching: ConfigWriter[BatchAggregatorConfig.Batching] =
        deriveWriter[BatchAggregatorConfig.Batching]
      implicit val noBatching: ConfigWriter[BatchAggregatorConfig.NoBatching.type] =
        deriveWriter[BatchAggregatorConfig.NoBatching.type]
      deriveWriter[BatchAggregatorConfig]
    }

    lazy implicit final val batchingWriter: ConfigWriter[BatchingConfig] =
      deriveWriter[BatchingConfig]

    lazy implicit final val connectionAllocationWriter: ConfigWriter[ConnectionAllocation] =
      deriveWriter[ConnectionAllocation]

    lazy implicit final val dbParamsWriter: ConfigWriter[DbParametersConfig] =
      deriveWriter[DbParametersConfig]

    lazy implicit final val queryCostMonitoringConfig: ConfigWriter[QueryCostMonitoringConfig] = {
      implicit val queryCostSortByWriter: ConfigWriter[QueryCostSortBy] =
        deriveEnumerationWriter[QueryCostSortBy]
      deriveWriter[QueryCostMonitoringConfig]
    }

  }

}
