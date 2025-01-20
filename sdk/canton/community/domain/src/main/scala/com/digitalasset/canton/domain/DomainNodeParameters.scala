// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain

import com.digitalasset.canton.config.RequireTypes.{PositiveDouble, PositiveInt}
import com.digitalasset.canton.domain.sequencing.CantonNodeWithSequencerParameters
import com.digitalasset.canton.environment.{
  CantonNodeParameters,
  HasGeneralCantonNodeParameters,
  HasProtocolCantonNodeParameters,
}

/** Parameters used by domain nodes
  *
  * We "merge" parameters that are valid for all nodes (i.e. canton.parameters) and
  * the node specific parameters together into this class.
  * @param general the node parameters required by the base class
  * @param protocol related parameters which are configured differently (not all nodes have the same set of parameters)
  * @param dispatcherBatchSize How many topology transactions we process in one batch max
  */
final case class DomainNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
    maxBurstFactor: PositiveDouble,
    dispatcherBatchSize: PositiveInt,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters
    with CantonNodeWithSequencerParameters
