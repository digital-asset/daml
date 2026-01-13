// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.config.{ActiveRequestLimitsConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.{
  CantonNodeParameters,
  HasGeneralCantonNodeParameters,
  HasProtocolCantonNodeParameters,
}
import com.digitalasset.canton.synchronizer.block.AsyncWriterParameters

trait SequencerParameters {
  def maxConfirmationRequestsBurstFactor: PositiveDouble
  def processingTimeouts: ProcessingTimeout
  def maxSubscriptionsPerMember: PositiveInt
}

/** Parameters for a SequencerNode. We "merge" parameters that are valid for all nodes (i.e.
  * canton.parameters) and the node specific parameters together into this class.
  *
  * @param general
  *   the node parameters required by the base class
  * @param protocol
  *   related parameters which are configured differently (not all nodes have the same set of
  *   parameters)
  * @param maxConfirmationRequestsBurstFactor
  *   How many confirmation requests can be sent in a burst before the rate limit kicks in.
  * @param sequencingTimeLowerBoundExclusive
  *   if defined, the sequencer will only send events with sequencing time strictly greater than
  *   sequencingTimeLowerBoundExclusive
  * @param asyncWriter
  *   Whether the sequencer writes are async or sync
  * @param timeAdvancingTopology
  *   How the sequencer should send time advancing broadcasts after topology transactions
  * @param unsafeEnableOnlinePartyReplication
  *   Whether to enable online party replication sequencer channels. Unsafe as still under
  *   development.
  * @param requestLimits
  *   optional stream limit for the number of active requests or streams
  * @param maxAuthTokensPerMember
  *   maximum number of auth tokens and nonces per member
  * @param maxSubscriptionsPerMember
  *   maximum number of subscriptions per member
  */
final case class SequencerNodeParameters(
    general: CantonNodeParameters.General,
    protocol: CantonNodeParameters.Protocol,
    maxConfirmationRequestsBurstFactor: PositiveDouble,
    sequencingTimeLowerBoundExclusive: Option[CantonTimestamp],
    asyncWriter: AsyncWriterParameters,
    timeAdvancingTopology: TimeAdvancingTopologyConfig,
    unsafeEnableOnlinePartyReplication: Boolean = false,
    sequencerApiLimits: Map[String, NonNegativeInt] = Map.empty,
    warnOnUndefinedLimits: Boolean = true,
    requestLimits: Option[ActiveRequestLimitsConfig] = None,
    maxAuthTokensPerMember: PositiveInt = PositiveInt.tryCreate(25),
    maxSubscriptionsPerMember: PositiveInt = PositiveInt.tryCreate(5),
) extends CantonNodeParameters
    with HasGeneralCantonNodeParameters
    with HasProtocolCantonNodeParameters
    with SequencerParameters
