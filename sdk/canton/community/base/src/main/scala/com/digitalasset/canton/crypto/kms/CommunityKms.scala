// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import cats.syntax.functor.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{CommunityKmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock

import scala.concurrent.ExecutionContext

/** Factory to create a KMS client for the community edition. */
object CommunityKms {

  def create(
      config: CommunityKmsConfig,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      executionContext: ExecutionContext,
  ): Either[KmsError, Kms] =
    config match {
      case driverKmsConfig: CommunityKmsConfig.Driver =>
        DriverKms
          .create(
            driverKmsConfig,
            futureSupervisor,
            clock,
            timeouts,
            loggerFactory,
            executionContext,
          )
          .widen[Kms]
    }

}
