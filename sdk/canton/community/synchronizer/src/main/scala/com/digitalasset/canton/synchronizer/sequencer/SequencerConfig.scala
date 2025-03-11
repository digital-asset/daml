// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.option.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.manual.CantonConfigValidatorDerivation
import com.digitalasset.canton.config.{
  CantonConfigValidator,
  CantonConfigValidatorInstances,
  DbLockedConnectionPoolConfig,
  EnterpriseOnlyCantonConfigValidation,
  NonNegativeFiniteDuration,
  PositiveDurationSeconds,
  PositiveFiniteDuration,
  StorageConfig,
  UniformCantonConfigValidation,
}
import com.digitalasset.canton.synchronizer.sequencer.DatabaseSequencerConfig.{
  SequencerPruningConfig,
  TestingInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.driver.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencing.sequencer.reference.{
  CommunityReferenceSequencerDriverFactory,
  ReferenceSequencerDriver,
}
import com.digitalasset.canton.time.Clock
import pureconfig.ConfigCursor

import scala.concurrent.ExecutionContext

sealed trait SequencerConfig {
  def supportsReplicas: Boolean
}

object SequencerConfig {

  implicit val sequencerConfigCantonConfigValidator: CantonConfigValidator[SequencerConfig] = {
    implicit val testingInterceptorCantonConfigValidator
        : CantonConfigValidator[TestingInterceptor] =
      CantonConfigValidator.validateAll
    CantonConfigValidatorDerivation[SequencerConfig]
  }

  final case class Database(
      writer: SequencerWriterConfig = SequencerWriterConfig.LowLatency(),
      reader: SequencerReaderConfig = SequencerReaderConfig(),
      highAvailability: Option[SequencerHighAvailabilityConfig] = None,
      testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
      pruning: SequencerPruningConfig = SequencerPruningConfig(),
  ) extends SequencerConfig
      with DatabaseSequencerConfig
      with UniformCantonConfigValidation {
    override def highAvailabilityEnabled: Boolean = highAvailability.exists(_.isEnabled)

    override def supportsReplicas: Boolean = highAvailabilityEnabled
  }

  final case class External(
      sequencerType: String,
      block: BlockSequencerConfig,
      config: ConfigCursor,
  ) extends SequencerConfig
      with UniformCantonConfigValidation {
    override def supportsReplicas: Boolean = false
  }
  object External {
    implicit val externalCantonConfigValidator: CantonConfigValidator[External] = {
      implicit val configCursorCantonConfigValidator: CantonConfigValidator[ConfigCursor] =
        CantonConfigValidator.validateAll // do not look into external configurations
      CantonConfigValidatorDerivation[External]
    }
  }

  final case class BftSequencer(
      block: BlockSequencerConfig =
        // To avoid having to include an empty "block" config element if defaults are fine
        BlockSequencerConfig(),
      config: BftBlockOrdererConfig,
  ) extends SequencerConfig
      with UniformCantonConfigValidation {
    override def supportsReplicas: Boolean = false
  }
  object BftSequencer {
    implicit val bftSequencerCantonConfigValidator: CantonConfigValidator[BftSequencer] =
      CantonConfigValidatorDerivation[BftSequencer]
  }

  def default: SequencerConfig = {
    val driverFactory = new CommunityReferenceSequencerDriverFactory
    External(
      driverFactory.name,
      BlockSequencerConfig(),
      ConfigCursor(
        driverFactory
          .configWriter(confidential = false)
          .to(ReferenceSequencerDriver.Config(storage = StorageConfig.Memory())),
        List(),
      ),
    )
  }

  /** Configuration for how many sequencers are concurrently operating within the synchronizer.
    * @param enabled
    *   Set to Some(true) to enable HA for the sequencer. If None it will be enabled if the storage
    *   supports replication.
    * @param totalNodeCount
    *   how many sequencer writers will there ever be in this synchronizer. recommend setting to a
    *   value larger than the current topology to allow for expansion.
    * @param keepAliveInterval
    *   how frequently will we ensure the sequencer watermark is updated to ensure it still appears
    *   alive
    * @param onlineCheckInterval
    *   how frequently should this sequencer check that nodes are still online
    * @param offlineDuration
    *   how long should a sequencer watermark be lagging for it to be flagged as offline
    * @param connectionPool
    *   configuration of the write connection pool used by an HA sequencer instance.
    */
  final case class SequencerHighAvailabilityConfig(
      // Optional to be able to know if the value has been set in the config file. This is useful to apply
      // default values later on, based on the type of storage used.
      enabled: Option[Boolean] = None,
      totalNodeCount: PositiveInt = PositiveInt.tryCreate(10),
      keepAliveInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(50L),
      onlineCheckInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5L),
      offlineDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(8L),
      connectionPool: DbLockedConnectionPoolConfig = DbLockedConnectionPoolConfig(),
      exclusiveStorage: DatabaseSequencerExclusiveStorageConfig =
        DatabaseSequencerExclusiveStorageConfig(),
  ) extends EnterpriseOnlyCantonConfigValidation {
    lazy val isEnabled: Boolean = enabled.contains(true)
    def toOnlineSequencerCheckConfig: OnlineSequencerCheckConfig =
      OnlineSequencerCheckConfig(onlineCheckInterval, offlineDuration)
  }

  object SequencerHighAvailabilityConfig {
    implicit val sequencerHighAvailabilityConfigCantonConfigValidator
        : CantonConfigValidator[SequencerHighAvailabilityConfig] = {
      import CantonConfigValidatorInstances.*
      CantonConfigValidatorDerivation[SequencerHighAvailabilityConfig]
    }
  }

  /** Configuration for exclusive database sequencer storage
    * @param connectionPool
    *   configuration of the exclusive write connection pool.
    * @param maxConnections
    *   maximum read plus write connections.
    */
  final case class DatabaseSequencerExclusiveStorageConfig(
      connectionPool: DbLockedConnectionPoolConfig =
        DbLockedConnectionPoolConfig(healthCheckPeriod =
          // high default check period to reduce overhead as liveness is less critical for pruning and config changes
          PositiveFiniteDuration.ofSeconds(60)
        ),
      // by default provide one connection each for reads and writes to keep exclusive storage light-weight.
      maxConnections: PositiveInt = PositiveInt.tryCreate(2),
  ) extends EnterpriseOnlyCantonConfigValidation

  object DatabaseSequencerExclusiveStorageConfig {
    implicit val databaseSequencerExclusiveStorageConfigCantonConfigValidator
        : CantonConfigValidator[DatabaseSequencerExclusiveStorageConfig] = {
      import CantonConfigValidatorInstances.*
      CantonConfigValidatorDerivation[DatabaseSequencerExclusiveStorageConfig]
    }
  }

}

/** Unsealed trait so the database sequencer config can be reused between community and enterprise
  */
trait DatabaseSequencerConfig {

  val writer: SequencerWriterConfig
  val reader: SequencerReaderConfig
  val testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor]
  val pruning: SequencerPruningConfig
  def highAvailabilityEnabled: Boolean
}

object DatabaseSequencerConfig {

  /** The Postgres sequencer supports adding a interceptor within the sequencer itself for
    * manipulating sequence behavior during tests. This is used for delaying and/or dropping
    * messages to verify the behavior of transaction processing in abnormal scenarios in a
    * deterministic way. It is not expected to be used at runtime in any capacity and is not
    * possible to set through pureconfig.
    */
  type TestingInterceptor =
    Clock => Sequencer => ExecutionContext => Sequencer

  /** Configuration for database sequencer pruning
    *
    * @param maxPruningBatchSize
    *   Maximum number of events to prune from a sequencer at a time, used to break up batches
    *   internally
    * @param pruningMetricUpdateInterval
    *   How frequently to update the `max-event-age` pruning progress metric in the background. A
    *   setting of None disables background metric updating.
    * @param trafficPurchasedRetention
    *   Retention duration on how long to retain traffic purchased entry updates for each member
    */
  final case class SequencerPruningConfig(
      maxPruningBatchSize: PositiveInt =
        PositiveInt.tryCreate(50000), // Large default for database-range-delete based pruning
      pruningMetricUpdateInterval: Option[PositiveDurationSeconds] =
        PositiveDurationSeconds.ofHours(1L).some,
      trafficPurchasedRetention: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(1),
  ) extends UniformCantonConfigValidation

  object SequencerPruningConfig {
    implicit val sequencerPruningConfigCantonConfigValidator
        : CantonConfigValidator[SequencerPruningConfig] = {
      import com.digitalasset.canton.config.CantonConfigValidatorInstances.*
      CantonConfigValidatorDerivation[SequencerPruningConfig]
    }
  }
}

final case class BlockSequencerConfig(
    writer: SequencerWriterConfig = SequencerWriterConfig.HighThroughput(),
    reader: SequencerReaderConfig = SequencerReaderConfig(),
    testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
) extends UniformCantonConfigValidation { self =>
  def toDatabaseSequencerConfig: DatabaseSequencerConfig = new DatabaseSequencerConfig {
    override val writer: SequencerWriterConfig = self.writer
    override val reader: SequencerReaderConfig = self.reader
    override val testingInterceptor: Option[TestingInterceptor] = self.testingInterceptor
    // TODO(#15987): Take pruning config from BlockSequencerConfig once block sequencer supports pruning.
    override val pruning: SequencerPruningConfig = SequencerPruningConfig()

    override def highAvailabilityEnabled: Boolean = false
  }
}

object BlockSequencerConfig {
  implicit val blockSequencerConfigCantonConfigValidator
      : CantonConfigValidator[BlockSequencerConfig] = {
    implicit val testingInterceptorCantonConfigValidator
        : CantonConfigValidator[TestingInterceptor] =
      CantonConfigValidator.validateAll
    CantonConfigValidatorDerivation[BlockSequencerConfig]
  }
}

/** Health check related sequencer config
  * @param backendCheckPeriod
  *   interval with which the sequencer will poll the health of its backend connection or state.
  */
final case class SequencerHealthConfig(
    backendCheckPeriod: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5)
) extends UniformCantonConfigValidation

object SequencerHealthConfig {
  implicit val sequencerHealthConfigCantonConfigValidator
      : CantonConfigValidator[SequencerHealthConfig] =
    CantonConfigValidatorDerivation[SequencerHealthConfig]
}
