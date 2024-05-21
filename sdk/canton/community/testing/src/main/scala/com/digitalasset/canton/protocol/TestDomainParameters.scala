// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.time.NonNegativeFiniteDuration

/** Domain parameters used for unit testing with sane default values. */
object TestDomainParameters {

  val defaultDynamic: DynamicDomainParameters =
    DynamicDomainParameters.initialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
      BaseTest.testedProtocolVersion,
    )

  def defaultDynamic(
      confirmationRequestsMaxRate: NonNegativeInt,
      maxRequestSize: MaxRequestSize,
  ): DynamicDomainParameters =
    DynamicDomainParameters.tryInitialValues(
      topologyChangeDelay = NonNegativeFiniteDuration.tryOfMillis(250),
      protocolVersion = BaseTest.testedProtocolVersion,
      confirmationRequestsMaxRate = confirmationRequestsMaxRate,
      maxRequestSize = maxRequestSize,
    )
}
