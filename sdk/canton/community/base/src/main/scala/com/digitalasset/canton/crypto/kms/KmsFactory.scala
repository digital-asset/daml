// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TracerProvider

import scala.concurrent.ExecutionContext

/** Factory to create a KMS. */
trait KmsFactory {

  def create(
      config: KmsConfig,
      nonStandardConfig: Boolean,
      timeouts: ProcessingTimeout,
      futureSupervisor: FutureSupervisor,
      tracerProvider: TracerProvider,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      executionContext: ExecutionContext,
  ): Either[KmsError, Kms]
}
