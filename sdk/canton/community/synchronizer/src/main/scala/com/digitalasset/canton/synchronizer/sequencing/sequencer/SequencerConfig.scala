// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.sequencer

import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{
  CommunityStorageConfig,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.block.bftordering.core.driver.BftBlockOrderer
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.{
  CommunityReferenceSequencerDriverFactory,
  ReferenceSequencerDriver,
}
import com.digitalasset.canton.time.Clock
import pureconfig.ConfigCursor

import scala.concurrent.ExecutionContext

import DatabaseSequencerConfig.{SequencerPruningConfig, TestingInterceptor}

trait SequencerConfig {
  def supportsReplicas: Boolean
}

/** Unsealed trait so the database sequencer config can be reused between community and enterprise */
trait DatabaseSequencerConfig {
  this: SequencerConfig =>

  val writer: SequencerWriterConfig
  val reader: SequencerReaderConfig
  val testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor]
  val pruning: SequencerPruningConfig
  def highAvailabilityEnabled: Boolean

  override def supportsReplicas: Boolean = highAvailabilityEnabled
}

object DatabaseSequencerConfig {

  /** The Postgres sequencer supports adding a interceptor within the sequencer itself for manipulating sequence behavior during tests.
    * This is used for delaying and/or dropping messages to verify the behavior of transaction processing in abnormal scenarios in a deterministic way.
    * It is not expected to be used at runtime in any capacity and is not possible to set through pureconfig.
    */
  type TestingInterceptor =
    Clock => Sequencer => ExecutionContext => Sequencer

  /** Configuration for database sequencer pruning
    *
    * @param maxPruningBatchSize            Maximum number of events to prune from a sequencer at a time, used to break up batches internally
    * @param pruningMetricUpdateInterval    How frequently to update the `max-event-age` pruning progress metric in the background.
    *                                       A setting of None disables background metric updating.
    * @param trafficPurchasedRetention      Retention duration on how long to retain traffic purchased entry updates for each member
    */
  final case class SequencerPruningConfig(
      maxPruningBatchSize: PositiveInt =
        PositiveInt.tryCreate(50000), // Large default for database-range-delete based pruning
      pruningMetricUpdateInterval: Option[PositiveDurationSeconds] =
        PositiveDurationSeconds.ofHours(1L).some,
      trafficPurchasedRetention: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(1),
  )

}

final case class BlockSequencerConfig(
    writer: SequencerWriterConfig = SequencerWriterConfig.HighThroughput(),
    reader: CommunitySequencerReaderConfig = CommunitySequencerReaderConfig(),
    testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
) { self =>
  def toDatabaseSequencerConfig: DatabaseSequencerConfig = new DatabaseSequencerConfig
    with SequencerConfig {
    override val writer: SequencerWriterConfig = self.writer
    override val reader: SequencerReaderConfig = self.reader
    override val testingInterceptor: Option[TestingInterceptor] = self.testingInterceptor
    // TODO(#15987): Take pruning config from BlockSequencerConfig once block sequencer supports pruning.
    override val pruning: SequencerPruningConfig = SequencerPruningConfig()

    override def highAvailabilityEnabled: Boolean = false
  }
}

sealed trait CommunitySequencerConfig extends SequencerConfig

final case class CommunitySequencerReaderConfig(
    override val readBatchSize: Int = SequencerReaderConfig.defaultReadBatchSize,
    override val checkpointInterval: NonNegativeFiniteDuration =
      SequencerReaderConfig.defaultCheckpointInterval,
    override val payloadBatchSize: Int = SequencerReaderConfig.defaultPayloadBatchSize,
    override val payloadBatchWindow: NonNegativeFiniteDuration =
      SequencerReaderConfig.defaultPayloadBatchWindow,
    override val payloadFetchParallelism: Int =
      SequencerReaderConfig.defaultPayloadFetchParallelism,
    override val eventGenerationParallelism: Int =
      SequencerReaderConfig.defaultEventGenerationParallelism,
) extends SequencerReaderConfig

object CommunitySequencerConfig {

  final case class Database(
      writer: SequencerWriterConfig = SequencerWriterConfig.LowLatency(),
      reader: CommunitySequencerReaderConfig = CommunitySequencerReaderConfig(),
      testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
      pruning: SequencerPruningConfig = SequencerPruningConfig(),
  ) extends CommunitySequencerConfig
      with DatabaseSequencerConfig {
    override def highAvailabilityEnabled: Boolean = false
  }

  final case class External(
      sequencerType: String,
      block: BlockSequencerConfig,
      config: ConfigCursor,
  ) extends CommunitySequencerConfig {
    override def supportsReplicas: Boolean = false
  }

  final case class BftSequencer(
      block: BlockSequencerConfig =
        new BlockSequencerConfig, // To avoid having to include an empty "block" config element if defaults are fine
      config: BftBlockOrderer.Config,
  ) extends CommunitySequencerConfig {
    override def supportsReplicas: Boolean = false
  }

  def default: CommunitySequencerConfig = {
    val driverFactory = new CommunityReferenceSequencerDriverFactory
    External(
      driverFactory.name,
      BlockSequencerConfig(),
      ConfigCursor(
        driverFactory
          .configWriter(confidential = false)
          .to(ReferenceSequencerDriver.Config(storage = CommunityStorageConfig.Memory())),
        List(),
      ),
    )
  }
}

/** Health check related sequencer config
  * @param backendCheckPeriod interval with which the sequencer will poll the health of its backend connection or state.
  */
final case class SequencerHealthConfig(
    backendCheckPeriod: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5)
)
