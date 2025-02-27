// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config
import com.digitalasset.canton.config.KmsConfig.RetryConfig
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.util.retry
import com.typesafe.config.ConfigValue

sealed trait KmsConfig {

  /** Retry configuration for KMS operations */
  def retries: RetryConfig
}

object KmsConfig {

  implicit val kmsConfigCantonConfigValidator: CantonConfigValidator[KmsConfig] =
    CantonConfigValidatorDerivation[KmsConfig]

  /** Exponential backoff configuration for retries of network failures
    *
    * @param initialDelay
    *   initial delay before the first retry
    * @param maxDelay
    *   max delay between retries
    * @param maxRetries
    *   max number of retries
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
    * @param failures
    *   exponential backoff retry configuration for retryable failures
    * @param createKeyCheck
    *   exponential backoff retry configuration when waiting for a newly created key to exist in the
    *   KMS
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

  /** A KMS configuration for an external KMS driver.
    *
    * @param name
    *   The name of the external KMS driver
    * @param config
    *   The driver specific raw config section
    * @param healthCheckPeriod
    *   How long to wait between health checks of the KMS driver
    * @param retries
    *   retry configuration for KMS operations
    */
  final case class Driver(
      name: String,
      config: ConfigValue,
      healthCheckPeriod: PositiveFiniteDuration = PositiveFiniteDuration.ofSeconds(10),
      retries: RetryConfig = RetryConfig(),
  ) extends KmsConfig
      with UniformCantonConfigValidation

  object Driver {
    // Don't try to validate anything inside the config value of the driver
    private implicit def configValueCantonConfigValidator: CantonConfigValidator[ConfigValue] =
      CantonConfigValidator.validateAll
    implicit val driverCantonConfigValidator: CantonConfigValidator[Driver] =
      CantonConfigValidatorDerivation[Driver]
  }

  /** Stores the configuration for AWS KMS. This configuration is mandatory if we want to protect
    * Canton's private keys using an AWS KMS.
    *
    * @param region
    *   defines the AWS region to be use (e.g. us-east-1)
    * @param multiRegionKey
    *   flag to enable multiRegion keys (Canton will generate single region keys by default)
    * @param auditLogging
    *   when enabled, all calls to KMS will be logged. Defaults to false.
    * @param retries
    *   retry configuration
    * @param disableSslVerification
    *   When set to true, SSL verification is disabled. Mostly for testing purposes. Can only be
    *   used if non-standard-config is enabled.
    * @param endpointOverride
    *   the [optional] endpoint for a proxy to be used by the KMS client.
    */
  final case class Aws(
      region: String,
      multiRegionKey: Boolean = false,
      auditLogging: Boolean = false,
      override val retries: RetryConfig = RetryConfig(),
      disableSslVerification: Boolean = false,
      endpointOverride: Option[String] = None,
  ) extends KmsConfig
      with EnterpriseOnlyCantonConfigValidation
  object Aws {
    val defaultTestConfig: Aws = Aws(region = "us-east-1")
    val testConfigWithAudit: Aws = defaultTestConfig.copy(auditLogging = true)
    val multiRegionTestConfig: Aws = defaultTestConfig.copy(multiRegionKey = true)
  }

  /** Stores the configuration for GCP KMS. This configuration is mandatory if we want to protect
    * Canton's private keys using a GCP KMS.
    *
    * @param locationId
    *   defines the GCP region to use (e.g. us-east1)
    * @param projectId
    *   defines a GCP project to use (e.g. gcp-kms-testing)
    * @param keyRingId
    *   defines a key-ring to where keys will be added. This can be created as a multi-region
    *   key-ring, which enables multi-region keys
    * @param auditLogging
    *   when enabled, all calls to KMS will be logged. Defaults to false.
    * @param retries
    *   retry configuration
    * @param endpointOverride
    *   the [optional] endpoint for a proxy to be used by the KMS client.
    */
  final case class Gcp(
      locationId: String,
      projectId: String,
      keyRingId: String,
      auditLogging: Boolean = false,
      override val retries: RetryConfig = RetryConfig(),
      endpointOverride: Option[String] = None,
  ) extends KmsConfig
      with EnterpriseOnlyCantonConfigValidation

  object Gcp {
    val defaultTestConfig: Gcp = Gcp(
      locationId = "us-east1",
      projectId = "gcp-kms-testing",
      keyRingId = "canton-test-keys-2023",
    )
    val multiRegionTestConfig: Gcp = Gcp(
      locationId = "global",
      projectId = "gcp-kms-testing",
      keyRingId = "canton-test-keys-multi-region",
    )
  }

}
