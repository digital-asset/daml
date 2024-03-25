// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.config.{CommunityStorageConfig, NonNegativeFiniteDuration}
import com.digitalasset.canton.domain.sequencing.sequencer.DatabaseSequencerConfig.TestingInterceptor
import com.digitalasset.canton.domain.sequencing.sequencer.reference.{
  CommunityReferenceSequencerDriverFactory,
  ReferenceBlockOrderer,
}
import com.digitalasset.canton.time.Clock
import pureconfig.ConfigCursor

import scala.concurrent.ExecutionContext

trait SequencerConfig {
  def supportsReplicas: Boolean
}

/** Unsealed trait so the database sequencer config can be reused between community and enterprise */
trait DatabaseSequencerConfig {
  this: SequencerConfig =>

  val writer: SequencerWriterConfig
  val reader: SequencerReaderConfig
  val testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor]
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
}

sealed trait CommunitySequencerConfig extends SequencerConfig

final case class CommunitySequencerReaderConfig(
    override val readBatchSize: Int = SequencerReaderConfig.defaultReadBatchSize,
    override val checkpointInterval: NonNegativeFiniteDuration =
      SequencerReaderConfig.defaultCheckpointInterval,
) extends SequencerReaderConfig

object CommunitySequencerConfig {

  final case class Database(
      writer: SequencerWriterConfig = SequencerWriterConfig.LowLatency(),
      reader: CommunitySequencerReaderConfig = CommunitySequencerReaderConfig(),
      testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
  ) extends CommunitySequencerConfig
      with DatabaseSequencerConfig {
    override def highAvailabilityEnabled: Boolean = false
  }

  final case class External(
      sequencerType: String,
      config: ConfigCursor,
      testingInterceptor: Option[TestingInterceptor],
  ) extends CommunitySequencerConfig {
    override def supportsReplicas: Boolean = false
  }

  def default: CommunitySequencerConfig = {
    val driverFactory = new CommunityReferenceSequencerDriverFactory
    External(
      driverFactory.name,
      ConfigCursor(
        driverFactory
          .configWriter(confidential = false)
          .to(ReferenceBlockOrderer.Config(storage = CommunityStorageConfig.Memory())),
        List(),
      ),
      None,
    )
  }
}

/** Health check related sequencer config
  * @param backendCheckPeriod interval with which the sequencer will poll the health of its backend connection or state.
  */
final case class SequencerHealthConfig(
    backendCheckPeriod: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5)
)
