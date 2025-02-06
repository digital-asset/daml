// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config
import com.digitalasset.canton.config.KmsConfig.RetryConfig
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.util.retry
import com.typesafe.config.ConfigValue

trait KmsConfig {

  /** Retry configuration for KMS operations */
  def retries: RetryConfig
}

object KmsConfig {

  /** Exponential backoff configuration for retries of network failures
    *
    * @param initialDelay initial delay before the first retry
    * @param maxDelay     max delay between retries
    * @param maxRetries   max number of retries
    */
  final case class ExponentialBackoffConfig(
      initialDelay: config.NonNegativeFiniteDuration,
      maxDelay: config.NonNegativeDuration,
      maxRetries: Int,
  ) extends UniformCantonConfigValidation

  object ExponentialBackoffConfig {
    implicit val exponentialBackoffConfigCantonConfigValidator
        : CantonConfigValidator[ExponentialBackoffConfig] =
      CantonConfigValidatorDerivation[ExponentialBackoffConfig]
  }

  /** Retry configuration for KMS operations
    *
    * @param failures       exponential backoff retry configuration for retryable failures
    * @param createKeyCheck exponential backoff retry configuration when waiting for a newly created key to exist in the KMS
    */
  final case class RetryConfig(
      failures: ExponentialBackoffConfig = ExponentialBackoffConfig(
        initialDelay = config.NonNegativeFiniteDuration.ofMillis(500),
        maxDelay = config.NonNegativeDuration.ofSeconds(10),
        maxRetries = retry.Forever,
      ),
      createKeyCheck: ExponentialBackoffConfig = ExponentialBackoffConfig(
        initialDelay = config.NonNegativeFiniteDuration.ofMillis(100),
        maxDelay = config.NonNegativeDuration.ofSeconds(10),
        maxRetries = 20,
      ),
  ) extends UniformCantonConfigValidation

  object RetryConfig {
    implicit val retryConfigCantonConfigValidator: CantonConfigValidator[RetryConfig] =
      CantonConfigValidatorDerivation[RetryConfig]
  }

  trait Driver extends KmsConfig {
    def name: String
    def config: ConfigValue
    def healthCheckPeriod: PositiveFiniteDuration
  }

}
