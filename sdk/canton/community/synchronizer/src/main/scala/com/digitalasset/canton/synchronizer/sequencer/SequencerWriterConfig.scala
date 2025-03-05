// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  NonNegativeFiniteDuration,
  UniformCantonConfigValidation,
}
import com.digitalasset.canton.util.BytesUnit

sealed trait CommitMode {
  private[sequencer] val postgresSettings: NonEmpty[Seq[String]]
}

object CommitMode {

  implicit val commitModeCantonConfigValidator: CantonConfigValidator[CommitMode] =
    CantonConfigValidatorDerivation[CommitMode]

  /** Synchronously commit to local and replicas (in psql this means synchronous_commit='on' or
    * 'remote_write' and that synchronous_standby_names have been appropriately set)
    */
  case object Synchronous extends CommitMode with UniformCantonConfigValidation {
    override private[sequencer] val postgresSettings = NonEmpty(Seq, "on", "remote_write")
  }

  /** Synchronously commit to the local database alone (in psql this means
    * synchronous_commit='local')
    */
  case object Local extends CommitMode with UniformCantonConfigValidation {
    override private[sequencer] val postgresSettings = NonEmpty(Seq, "local")
  }

  /** The default commit mode we expect a sequencer to be run in. */
  val Default = CommitMode.Synchronous
}

/** Configuration for the database based sequencer writer
  * @param payloadQueueSize
  *   how many payloads should be held in memory while waiting for them to be flushed to the db. if
  *   new deliver events with payloads are requested when this queue is full the send will return a
  *   overloaded error and reject the request.
  * @param payloadWriteBatchMaxSize
  *   max payload batch size to flush to the database. will trigger a write when this batch size is
  *   reached.
  * @param payloadWriteMaxConcurrency
  *   limit how many payload batches can be written concurrently.
  * @param eventWriteBatchMaxSize
  *   max event batch size to flush to the database.
  * @param eventWriteMaxConcurrency
  *   limit how many event batches can be written concurrently for block sequencers.
  * @param commitMode
  *   optional commit mode that if set will be validated to ensure that the connection/db settings
  *   have been configured. Defaults to [[CommitMode.Synchronous]].
  * @param commitModeValidation
  *   optional commit mode that if set will be validated to ensure that the connection/db settings
  *   have been configured. Defaults to [[CommitMode.Synchronous]].
  * @param checkpointInterval
  *   an interval at which to generate sequencer counter checkpoints for all members.
  * @param checkpointBackfillParallelism
  *   controls how many checkpoints will be written in parallel during the checkpoint backfill
  *   process at startup. Higher parallelism likely means more IO load on the database, but likely
  *   overall faster progression through the missing checkpoints.
  * @param bufferedEventsMaxMemory
  *   the maximum memory the events buffer will use for caching events
  * @param bufferedEventsPreloadBatchSize
  *   the batch size for load events into the events buffer at the start of the sequencer
  */
sealed trait SequencerWriterConfig {
  this: {
    def copy(
        payloadQueueSize: Int,
        payloadWriteBatchMaxSize: Int,
        payloadWriteMaxConcurrency: Int,
        payloadToEventMargin: NonNegativeFiniteDuration,
        eventWriteBatchMaxSize: Int,
        eventWriteMaxConcurrency: Int,
        commitModeValidation: Option[CommitMode],
        checkpointInterval: NonNegativeFiniteDuration,
        checkpointBackfillParallelism: PositiveInt,
        bufferedEventsMaxMemory: BytesUnit,
        bufferedEventsPreloadBatchSize: PositiveInt,
    ): SequencerWriterConfig
  } =>

  val payloadQueueSize: Int
  val payloadWriteBatchMaxSize: Int
  val payloadWriteMaxConcurrency: Int
  val payloadToEventMargin: NonNegativeFiniteDuration
  val eventWriteBatchMaxSize: Int
  val eventWriteMaxConcurrency: Int
  val commitModeValidation: Option[CommitMode]
  val bufferedEventsMaxMemory: BytesUnit
  val bufferedEventsPreloadBatchSize: PositiveInt

  /** how frequently to generate counter checkpoints for all members */
  val checkpointInterval: NonNegativeFiniteDuration
  val checkpointBackfillParallelism: PositiveInt

  def modify(
      payloadQueueSize: Int = this.payloadQueueSize,
      payloadWriteBatchMaxSize: Int = this.payloadWriteBatchMaxSize,
      payloadWriteMaxConcurrency: Int = this.payloadWriteMaxConcurrency,
      payloadToEventMargin: NonNegativeFiniteDuration = this.payloadToEventMargin,
      eventWriteBatchMaxSize: Int = this.eventWriteBatchMaxSize,
      eventWriteMaxConcurrency: Int = this.eventWriteMaxConcurrency,
      commitModeValidation: Option[CommitMode] = this.commitModeValidation,
      checkpointInterval: NonNegativeFiniteDuration = this.checkpointInterval,
      checkpointBackfillParallelism: PositiveInt = this.checkpointBackfillParallelism,
      bufferedEventsMaxMemory: BytesUnit = this.bufferedEventsMaxMemory,
      bufferedEventsPreloadBatchSize: PositiveInt = this.bufferedEventsPreloadBatchSize,
  ): SequencerWriterConfig =
    copy(
      payloadQueueSize,
      payloadWriteBatchMaxSize,
      payloadWriteMaxConcurrency,
      payloadToEventMargin,
      eventWriteBatchMaxSize,
      eventWriteMaxConcurrency,
      commitModeValidation,
      checkpointInterval,
      checkpointBackfillParallelism,
      bufferedEventsMaxMemory,
      bufferedEventsPreloadBatchSize,
    )
}

/** Expose config as different named versions using different default values to allow easy switching
  * for the different setups we can run in (high-throughput, low-latency). However as each value is
  * only a default so they can also be easily overridden if required.
  */
object SequencerWriterConfig {
  implicit val sequencerWriterConfigCantonConfigValidator
      : CantonConfigValidator[SequencerWriterConfig] = {
    import com.digitalasset.canton.config.CantonConfigValidatorInstances.*
    CantonConfigValidatorDerivation[SequencerWriterConfig]
  }

  val DefaultPayloadTimestampMargin: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(60L)

  val DefaultCheckpointInterval: config.NonNegativeFiniteDuration =
    config.NonNegativeFiniteDuration.ofSeconds(30)

  val DefaultCheckpointBackfillParallelism: PositiveInt = PositiveInt.two

  val DefaultBufferedEventsMaxMemory: BytesUnit = BytesUnit.MB(2L)
  val DefaultBufferedEventsPreloadBatchSize: PositiveInt = PositiveInt.tryCreate(50)

  /** Use to have events immediately flushed to the database. Useful for decreasing latency however
    * at a high throughput a large number of writes will be detrimental for performance.
    */
  final case class LowLatency(
      override val payloadQueueSize: Int = 1000,
      override val payloadWriteBatchMaxSize: Int = 1,
      override val payloadWriteMaxConcurrency: Int = 2,
      override val payloadToEventMargin: NonNegativeFiniteDuration = DefaultPayloadTimestampMargin,
      override val eventWriteBatchMaxSize: Int = 1,
      override val eventWriteMaxConcurrency: Int = 1,
      override val commitModeValidation: Option[CommitMode] = CommitMode.Default.some,
      override val checkpointInterval: NonNegativeFiniteDuration = DefaultCheckpointInterval,
      override val checkpointBackfillParallelism: PositiveInt =
        DefaultCheckpointBackfillParallelism,
      override val bufferedEventsMaxMemory: BytesUnit = DefaultBufferedEventsMaxMemory,
      override val bufferedEventsPreloadBatchSize: PositiveInt =
        DefaultBufferedEventsPreloadBatchSize,
  ) extends SequencerWriterConfig
      with UniformCantonConfigValidation

  /** Creates batches of incoming events to minimize the number of writes to the database. Useful
    * for a high throughput usecase when batches will be quickly filled and written. Will be
    * detrimental for latency if used and a lower throughput of events causes writes to always be
    * delayed to the batch max duration.
    */
  final case class HighThroughput(
      override val payloadQueueSize: Int = 1000,
      override val payloadWriteBatchMaxSize: Int = 50,
      override val payloadWriteMaxConcurrency: Int = 4,
      override val payloadToEventMargin: NonNegativeFiniteDuration = DefaultPayloadTimestampMargin,
      override val eventWriteBatchMaxSize: Int = 100,
      override val eventWriteMaxConcurrency: Int = 1,
      override val commitModeValidation: Option[CommitMode] = CommitMode.Default.some,
      override val checkpointInterval: NonNegativeFiniteDuration = DefaultCheckpointInterval,
      override val checkpointBackfillParallelism: PositiveInt =
        DefaultCheckpointBackfillParallelism,
      override val bufferedEventsMaxMemory: BytesUnit = DefaultBufferedEventsMaxMemory,
      override val bufferedEventsPreloadBatchSize: PositiveInt =
        DefaultBufferedEventsPreloadBatchSize,
  ) extends SequencerWriterConfig
      with UniformCantonConfigValidation
}
