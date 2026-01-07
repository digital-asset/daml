// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{PositiveDouble, PositiveInt}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.block.AsyncWriterParameters
import io.scalaland.chimney.dsl.*

/** Async block sequencer writer control parameters
  *
  * @param enabled
  *   if true (default) then the async writer is enabled
  * @param trafficBatchSize
  *   the maximum number of traffic events to batch in a single write
  * @param aggregationBatchSize
  *   the maximum number of inflight aggregations to batch in a single write
  * @param blockInfoBatchSize
  *   the maximum number of block info updates to batch in a single write
  */
final case class AsyncWriterConfig(
    enabled: Boolean = true,
    trafficBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    aggregationBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    blockInfoBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
) {
  def toParameters: AsyncWriterParameters = this.transformInto[AsyncWriterParameters]
}

/** Various parameters for non-standard sequencer settings
  *
  * @param alphaVersionSupport
  *   if true, then dev version will be turned on, but we will brick this sequencer node if it is
  *   used for production.
  * @param dontWarnOnDeprecatedPV
  *   if true, then this sequencer will not emit a warning when configured to use protocol version
  *   2.0.0.
  * @param maxConfirmationRequestsBurstFactor
  *   how forgiving the rate limit is in case of bursts (so rate limit starts after observing an
  *   initial burst of factor * max_rate commands)
  * @param sequencingTimeLowerBoundExclusive
  *   if defined, the sequencer will only send events with sequencing time strictly greater than
  *   sequencingTimeLowerBoundExclusive
  * @param asyncWriter
  *   controls the async writer
  * @param producePostOrderingTopologyTicks
  *   temporary flag to enable topology ticks produced post-ordering by sequencers (feature in
  *   development)
  */
final case class SequencerNodeParameterConfig(
    override val alphaVersionSupport: Boolean = false,
    override val betaVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    maxConfirmationRequestsBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.5),
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val watchdog: Option[WatchdogConfig] = None,
    unsafeEnableOnlinePartyReplication: Boolean = false,
    sequencingTimeLowerBoundExclusive: Option[CantonTimestamp] =
      SequencerNodeParameterConfig.DefaultSequencingTimeLowerBoundExclusive,
    asyncWriter: AsyncWriterConfig = AsyncWriterConfig(),
    timeAdvancingTopology: TimeAdvancingTopologyConfig = TimeAdvancingTopologyConfig(),
    // TODO(#29314) remove this flag once the feature is complete
    producePostOrderingTopologyTicks: Boolean = false,
) extends ProtocolConfig
    with LocalNodeParametersConfig

object SequencerNodeParameterConfig {

  val DefaultSequencingTimeLowerBoundExclusive: Option[CantonTimestamp] = None
}
