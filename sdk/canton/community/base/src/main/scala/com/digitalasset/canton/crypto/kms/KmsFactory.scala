// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.aws.AwsKms
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.crypto.kms.gcp.GcpKms
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TracerProvider

import scala.concurrent.ExecutionContext

object KmsFactory {

  def create(
      config: KmsConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      tracerProvider: TracerProvider,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      executionContext: ExecutionContext,
  ): Either[KmsError, Kms] =
    config match {
      case awsKmsConfig: KmsConfig.Aws =>
        AwsKms.create(awsKmsConfig, timeouts, loggerFactory, tracerProvider)
      case gcpKmsConfig: KmsConfig.Gcp =>
        GcpKms.create(gcpKmsConfig, timeouts, loggerFactory)
      case driverKmsConfig: KmsConfig.Driver =>
        DriverKms.create(
          driverKmsConfig,
          futureSupervisor,
          clock,
          timeouts,
          loggerFactory,
          executionContext,
        )
    }

}
