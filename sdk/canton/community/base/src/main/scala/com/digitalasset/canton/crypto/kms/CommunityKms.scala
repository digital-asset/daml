// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{
  EnterpriseOnlyCantonConfigValidation,
  KmsConfig,
  ProcessingTimeout,
}
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock

import scala.concurrent.ExecutionContext

/** Factory to create a KMS client for the community edition. */
object CommunityKms {

  def create(
      config: KmsConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      executionContext: ExecutionContext,
  ): Either[KmsError, Kms] =
    config match {
      case driverKmsConfig: KmsConfig.Driver =>
        DriverKms.create(
          driverKmsConfig,
          futureSupervisor,
          clock,
          timeouts,
          loggerFactory,
          executionContext,
        )
      case other: EnterpriseOnlyCantonConfigValidation =>
        throw new IllegalArgumentException(
          s"Unsupported KMS configuration in community edition: $other"
        )
    }
}
