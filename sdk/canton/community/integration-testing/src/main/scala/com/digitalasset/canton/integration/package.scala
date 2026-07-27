// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.{CantonConfig, StorageConfig}
import com.digitalasset.canton.console.ConsoleEnvironment
import com.digitalasset.canton.integration.ConfigTransforms.ConfigNodeType

package object integration {
  type TestConsoleEnvironment = ConsoleEnvironment & TestEnvironment
  type ConfigTransform = CantonConfig => CantonConfig
  type StorageConfigTransform =
    (ConfigNodeType, String, StorageConfig) => StorageConfig
}
