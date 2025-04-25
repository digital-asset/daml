// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.plugins

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.kms.{CommunityKmsFactory, Kms, KmsError}
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.tracing.NoReportingTracerProvider

import scala.concurrent.ExecutionContext

trait UseCommunityKms extends UseKms {
  override protected def createKms()(implicit ec: ExecutionContext): Either[KmsError, Kms] =
    CommunityKmsFactory
      .create(
        kmsConfig,
        nonStandardConfig = false,
        timeouts,
        FutureSupervisor.Noop,
        NoReportingTracerProvider,
        new WallClock(timeouts, loggerFactory),
        loggerFactory,
        ec,
      )
}
