// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.config

import cats.data.Chain
import com.digitalasset.canton.config.*
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.synchronizer.block.AsyncWriterParameters
import com.digitalasset.canton.synchronizer.sequencer.ProgressSupervisorConfig
import io.scalaland.chimney.dsl.*

/** Async block sequencer writer control parameters
  *
  * @param enabled
  *   if true then the async writer is enabled
  * @param trafficBatchSize
  *   the maximum number of traffic events to batch in a single write
  * @param aggregationBatchSize
  *   the maximum number of inflight aggregations to batch in a single write
  * @param blockInfoBatchSize
  *   the maximum number of block info updates to batch in a single write
  */
final case class AsyncWriterConfig(
    enabled: Boolean = false,
    trafficBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    aggregationBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
    blockInfoBatchSize: PositiveInt = PositiveInt.tryCreate(1000),
) {

  def toParameters: AsyncWriterParameters = this.transformInto[AsyncWriterParameters]

}

object AsyncWriterConfig {

  implicit val asyncWriterConfigCantonConfigValidator: CantonConfigValidator[AsyncWriterConfig] =
    new CantonConfigValidator[AsyncWriterConfig] {
      override def validate(
          edition: CantonEdition,
          config: AsyncWriterConfig,
      ): Chain[CantonConfigValidationError] = Chain.empty
    }

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
  * @param sequencerApiLimits
  *   map of service name to maximum number of parallel open streams
  * @param warnOnUndefinedLimits
  *   if true, then this sequencer will emit a warning once if there is no limit configured for a
  *   particular stream
  * @param progressSupervisor
  *   if defined, enables the progress supervisor to monitor if the sequencer stops progressing and
  *   to collect diagnostic data
  * @param asyncWriter
  *   controls the async writer
  */
final case class SequencerNodeParameterConfig(
    override val sessionSigningKeys: SessionSigningKeysConfig = SessionSigningKeysConfig.disabled,
    override val alphaVersionSupport: Boolean = false,
    override val betaVersionSupport: Boolean = false,
    override val dontWarnOnDeprecatedPV: Boolean = false,
    maxConfirmationRequestsBurstFactor: PositiveDouble = PositiveDouble.tryCreate(0.5),
    override val batching: BatchingConfig = BatchingConfig(),
    override val caching: CachingConfigs = CachingConfigs(),
    override val watchdog: Option[WatchdogConfig] = None,
    progressSupervisor: Option[ProgressSupervisorConfig] = None,
    unsafeEnableOnlinePartyReplication: Boolean = false,
    sequencerApiLimits: Map[String, NonNegativeInt] = Map.empty,
    warnOnUndefinedLimits: Boolean = true,
    asyncWriter: AsyncWriterConfig = AsyncWriterConfig(),
) extends ProtocolConfig
    with LocalNodeParametersConfig
    with UniformCantonConfigValidation

object SequencerNodeParameterConfig {

  implicit val sequencerNodeParameterConfigCantonConfigValidator
      : CantonConfigValidator[SequencerNodeParameterConfig] = {
    import CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[SequencerNodeParameterConfig]
  }
}
