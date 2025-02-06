// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.KmsConfig.RetryConfig
import com.typesafe.config.ConfigValue

/** KMS configuration for the community edition.
  * Only supports KMS drivers.
  */
sealed trait CommunityKmsConfig extends KmsConfig

object CommunityKmsConfig {

  /** A KMS configuration for an external KMS driver.
    *
    * @param name The name of the external KMS driver
    * @param config The driver specific raw config section
    * @param healthCheckPeriod How long to wait between health checks of the KMS driver
    * @param retries retry configuration for KMS operations
    */
  final case class Driver(
      name: String,
      config: ConfigValue,
      healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(10),
      retries: RetryConfig = RetryConfig(),
  ) extends CommunityKmsConfig
      with KmsConfig.Driver
}
