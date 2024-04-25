// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.domain.sequencing.CantonNodeWithSequencerParameters
import com.digitalasset.canton.environment.{
  CantonNodeParameters,
  HasGeneralCantonNodeParameters,
  HasProtocolCantonNodeParameters,
}

final case class SequencerNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
    maxBurstFactor: PositiveDouble,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters
    with CantonNodeWithSequencerParameters
