// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.option.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric

sealed trait CommitMode {
  private[sequencer] val postgresSettings: NonEmpty[Seq[String]]
}

object CommitMode {

  /** Synchronously commit to local and replicas (in psql this means synchronous_commit='on' or 'remote_write' and that synchronous_standby_names have been appropriately set) */
  case object Synchronous extends CommitMode {
    override private[sequencer] val postgresSettings = NonEmpty(Seq, "on", "remote_write")
  }

  /** Synchronously commit to the local database alone (in psql this means synchronous_commit='local') */
  case object Local extends CommitMode {
    override private[sequencer] val postgresSettings = NonEmpty(Seq, "local")
  }

  /** The default commit mode we expect a sequencer to be run in. */
  val Default = CommitMode.Synchronous
}

/** Configuration for the database based sequencer writer
  * @param payloadQueueSize how many payloads should be held in memory while waiting for them to be flushed to the db.
  *                         if new deliver events with payloads are requested when this queue is full the send will
  *                         return a overloaded error and reject the request.
  * @param payloadWriteBatchMaxSize max payload batch size to flush to the database.
  *                                 will trigger a write when this batch size is reached.
  * @param payloadWriteBatchMaxDuration max duration to collect payloads for a batch before triggering a write if
  *                                     payloadWriteBatchMaxSize is not hit first.
  * @param payloadWriteMaxConcurrency limit how many payload batches can be written concurrently.
  * @param eventWriteBatchMaxSize max event batch size to flush to the database.
  * @param eventWriteBatchMaxDuration max duration to collect events for a batch before triggering a write.
  * @param commitMode optional commit mode that if set will be validated to ensure that the connection/db settings have been configured. Defaults to [[CommitMode.Synchronous]].
  * @param maxSqlInListSize will limit the number of items in a SQL in clause. useful for databases where this may have a low limit (e.g. Oracle).
  */
sealed trait SequencerWriterConfig {
  this: {
    def copy(
        payloadQueueSize: Int,
        payloadWriteBatchMaxSize: Int,
        payloadWriteBatchMaxDuration: NonNegativeFiniteDuration,
        payloadWriteMaxConcurrency: Int,
        payloadToEventMargin: NonNegativeFiniteDuration,
        eventWriteBatchMaxSize: Int,
        eventWriteBatchMaxDuration: NonNegativeFiniteDuration,
        commitModeValidation: Option[CommitMode],
        maxSqlInListSize: PositiveNumeric[Int],
    ): SequencerWriterConfig
  } =>

  val payloadQueueSize: Int
  val payloadWriteBatchMaxSize: Int
  val payloadWriteBatchMaxDuration: NonNegativeFiniteDuration
  val payloadWriteMaxConcurrency: Int
  val payloadToEventMargin: NonNegativeFiniteDuration
  val eventWriteBatchMaxSize: Int
  val eventWriteBatchMaxDuration: NonNegativeFiniteDuration
  val commitModeValidation: Option[CommitMode]
  val maxSqlInListSize: PositiveNumeric[Int]

  def modify(
      payloadQueueSize: Int = this.payloadQueueSize,
      payloadWriteBatchMaxSize: Int = this.payloadWriteBatchMaxSize,
      payloadWriteBatchMaxDuration: NonNegativeFiniteDuration = this.payloadWriteBatchMaxDuration,
      payloadWriteMaxConcurrency: Int = this.payloadWriteMaxConcurrency,
      payloadToEventMargin: NonNegativeFiniteDuration = this.payloadToEventMargin,
      eventWriteBatchMaxSize: Int = this.eventWriteBatchMaxSize,
      eventWriteBatchMaxDuration: NonNegativeFiniteDuration = this.eventWriteBatchMaxDuration,
      commitModeValidation: Option[CommitMode] = this.commitModeValidation,
      maxSqlInListSize: PositiveNumeric[Int] = this.maxSqlInListSize,
  ): SequencerWriterConfig =
    copy(
      payloadQueueSize,
      payloadWriteBatchMaxSize,
      payloadWriteBatchMaxDuration,
      payloadWriteMaxConcurrency,
      payloadToEventMargin,
      eventWriteBatchMaxSize,
      eventWriteBatchMaxDuration,
      commitModeValidation,
      maxSqlInListSize,
    )
}

/** Expose config as different named versions using different default values to allow easy switching for the different
  * setups we can run in (high-throughput, low-latency). However as each value is only a default so they can also be easily
  * overridden if required.
  */
object SequencerWriterConfig {
  val DefaultPayloadTimestampMargin: NonNegativeFiniteDuration =
    NonNegativeFiniteDuration.ofSeconds(60L)
  // the Oracle limit is likely 1000 however this is currently only used for payload lookups on conflicts (savePayloads)
  // so just set a bit above the default max payload batch size (50)
  val DefaultMaxSqlInListSize: PositiveNumeric[Int] = PositiveNumeric.tryCreate(250)

  /** Use to have events immediately flushed to the database. Useful for decreasing latency however at a high throughput
    * a large number of writes will be detrimental for performance.
    */
  final case class LowLatency(
      override val payloadQueueSize: Int = 1000,
      override val payloadWriteBatchMaxSize: Int = 1,
      override val payloadWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(10),
      override val payloadWriteMaxConcurrency: Int = 2,
      override val payloadToEventMargin: NonNegativeFiniteDuration = DefaultPayloadTimestampMargin,
      override val eventWriteBatchMaxSize: Int = 1,
      override val eventWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(20),
      override val commitModeValidation: Option[CommitMode] = CommitMode.Default.some,
      override val maxSqlInListSize: PositiveNumeric[Int] = DefaultMaxSqlInListSize,
  ) extends SequencerWriterConfig

  /** Creates batches of incoming events to minimize the number of writes to the database. Useful for a high throughput
    * usecase when batches will be quickly filled and written. Will be detrimental for latency if used and a lower throughput
    * of events causes writes to always be delayed to the batch max duration.
    */
  final case class HighThroughput(
      override val payloadQueueSize: Int = 1000,
      override val payloadWriteBatchMaxSize: Int = 50,
      override val payloadWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(50),
      override val payloadWriteMaxConcurrency: Int = 4,
      override val payloadToEventMargin: NonNegativeFiniteDuration = DefaultPayloadTimestampMargin,
      override val eventWriteBatchMaxSize: Int = 100,
      override val eventWriteBatchMaxDuration: NonNegativeFiniteDuration =
        NonNegativeFiniteDuration.ofMillis(50),
      override val commitModeValidation: Option[CommitMode] = CommitMode.Default.some,
      override val maxSqlInListSize: PositiveNumeric[Int] = DefaultMaxSqlInListSize,
  ) extends SequencerWriterConfig
}
