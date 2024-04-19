// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.config

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveDouble
import com.digitalasset.canton.environment.{
  CantonNodeParameters,
  HasGeneralCantonNodeParameters,
  HasProtocolCantonNodeParameters,
}

trait SequencerParameters {
  def maxConfirmationRequestsBurstFactor: PositiveDouble
  def processingTimeouts: ProcessingTimeout
}

/** Parameters for a SequencerNode.
  * We "merge" parameters that are valid for all nodes (i.e. canton.parameters) and
  * the node specific parameters together into this class.
  * @param general the node parameters required by the base class
  * @param protocol related parameters which are configured differently (not all nodes have the same set of parameters)
  * @param maxConfirmationRequestsBurstFactor How many confirmation requests can be sent in a burst before the rate limit kicks in.
  */
final case class SequencerNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
    maxConfirmationRequestsBurstFactor: PositiveDouble,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters
    with SequencerParameters
