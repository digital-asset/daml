// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble}
import com.digitalasset.canton.config.{ActiveRequestLimitsConfig, ProcessingTimeout}
import com.digitalasset.canton.environment.{
  CantonNodeParameters,
  HasGeneralCantonNodeParameters,
  HasProtocolCantonNodeParameters,
}
import com.digitalasset.canton.synchronizer.block.AsyncWriterParameters

trait SequencerParameters {
  def maxConfirmationRequestsBurstFactor: PositiveDouble
  def processingTimeouts: ProcessingTimeout
}

/** Parameters for a SequencerNode. We "merge" parameters that are valid for all nodes (i.e.
  * canton.parameters) and the node specific parameters together into this class.
  * @param general
  *   the node parameters required by the base class
  * @param protocol
  *   related parameters which are configured differently (not all nodes have the same set of
  *   parameters)
  * @param maxConfirmationRequestsBurstFactor
  *   How many confirmation requests can be sent in a burst before the rate limit kicks in.
  * @param asyncWriter
  *   Whether the sequencer writes are async or sync
  * @param unsafeEnableOnlinePartyReplication
  *   Whether to enable online party replication sequencer channels. Unsafe as still under
  *   development.
  * @param streamLimits
  *   optional stream limit configs
  */
final case class SequencerNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
    maxConfirmationRequestsBurstFactor: PositiveDouble,
    asyncWriter: AsyncWriterParameters,
    unsafeEnableOnlinePartyReplication: Boolean = false,
    sequencerApiLimits: Map[String, NonNegativeInt] = Map.empty,
    warnOnUndefinedLimits: Boolean = true,
    requestLimits: Option[ActiveRequestLimitsConfig] = None,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters
    with SequencerParameters
