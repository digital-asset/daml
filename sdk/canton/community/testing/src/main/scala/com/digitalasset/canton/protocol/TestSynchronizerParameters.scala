// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize

/** Synchronizer parameters used for unit testing with sane default values. */
object TestSynchronizerParameters {

  val defaultDynamic: DynamicSynchronizerParameters =
    DynamicSynchronizerParameters.initialValues(
      BaseTest.testedProtocolVersion
    )

  def defaultDynamic(
      confirmationRequestsMaxRate: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
  ): DynamicSynchronizerParameters =
    DynamicSynchronizerParameters.tryInitialValues(
      protocolVersion = BaseTest.testedProtocolVersion,
      confirmationRequestsMaxRate = confirmationRequestsMaxRate,
      maxRequestSize = maxRequestSize,
    )
}
