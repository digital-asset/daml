// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.platform.indexer.PackageMetadataViewConfig.{
  DefaultInitLoadParallelism,
  DefaultInitProcessParallelism,
}

case class PackageMetadataViewConfig(
    initLoadParallelism: Int = DefaultInitLoadParallelism,
    initProcessParallelism: Int = DefaultInitProcessParallelism,
)

object PackageMetadataViewConfig {
  val DefaultInitLoadParallelism = 16
  val DefaultInitProcessParallelism = 16

  val Default = PackageMetadataViewConfig()
}
