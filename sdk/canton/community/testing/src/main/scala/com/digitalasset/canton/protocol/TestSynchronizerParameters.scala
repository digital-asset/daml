// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.time.NonNegativeFiniteDuration

/** Synchronizer parameters used for unit testing with sane default values. */
object TestSynchronizerParameters {

  val defaultDynamic: DynamicSynchronizerParameters =
    DynamicSynchronizerParameters.initialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
      BaseTest.testedProtocolVersion,
    )

  def defaultDynamic(
      confirmationRequestsMaxRate: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
  ): DynamicSynchronizerParameters =
    DynamicSynchronizerParameters.tryInitialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
      protocolVersion = BaseTest.testedProtocolVersion,
      confirmationRequestsMaxRate = confirmationRequestsMaxRate,
      maxRequestSize = maxRequestSize,
    )
}
