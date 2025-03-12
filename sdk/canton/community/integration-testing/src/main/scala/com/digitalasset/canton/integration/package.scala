// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.config.{CantonConfig, StorageConfig}
import com.digitalasset.canton.console.ConsoleEnvironment
import com.digitalasset.canton.integration.ConfigTransforms.ConfigNodeType

package object integration {

  /** This type takes the console type used at runtime for the environment and then augments it with
    * a type supporting our typical integration test extensions.
    */
  type TestConsoleEnvironment = ConsoleEnvironment with TestEnvironment

  type ConfigTransform = CantonConfig => CantonConfig
  type StorageConfigTransform =
    (ConfigNodeType, String, StorageConfig) => StorageConfig
}
