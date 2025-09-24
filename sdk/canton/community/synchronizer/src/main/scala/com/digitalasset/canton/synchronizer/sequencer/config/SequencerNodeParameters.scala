// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble}
import com.digitalasset.canton.environment.{
  CantonNodeParameters,
  HasGeneralCantonNodeParameters,
  HasProtocolCantonNodeParameters,
}
import com.digitalasset.canton.synchronizer.block.AsyncWriterParameters
import com.digitalasset.canton.synchronizer.sequencer.ProgressSupervisorConfig

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
  * @param sequencerApiLimits
  *   map of service name to maximum number of parallel open streams
  * @param warnOnUndefinedLimits
  *   emit warning if a limit is not configured for a stream
  */
final case class SequencerNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
    maxConfirmationRequestsBurstFactor: PositiveDouble,
    asyncWriter: AsyncWriterParameters,
    unsafeEnableOnlinePartyReplication: Boolean = false,
    sequencerApiLimits: Map[String, NonNegativeInt] = Map.empty,
    warnOnUndefinedLimits: Boolean = true,
    progressSupervisor: Option[ProgressSupervisorConfig] = None,
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters
    with SequencerParameters
