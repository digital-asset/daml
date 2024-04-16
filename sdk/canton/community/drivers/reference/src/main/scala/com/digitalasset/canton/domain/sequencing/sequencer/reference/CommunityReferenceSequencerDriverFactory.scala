// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.domain.sequencing.sequencer.reference

import com.digitalasset.canton.domain.block.{
  BlockOrderingSequencer,
  SequencerDriver,
  SequencerDriverFactory,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.TimeProvider
import org.apache.pekko.stream.Materializer
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

// Straightforward delegate with a zero-argument constructor that can be instantiated by the
//   `ServiceLoader`-based dynamic driver loading mechanism in `BlockSequencerFactory.getFactory`.
class CommunityReferenceSequencerDriverFactory extends SequencerDriverFactory {

  // Must be public because access to its ConfigType member is needed.
  val delegate =
    new BlockOrderingSequencer.Factory(
      blockOrdererFactory = new CommunityReferenceBlockOrdererFactory,
      driverName = "community-reference",
      useTimeProvider = true,
    )

  override def name: String = delegate.name

  override def version: Int = delegate.version

  override type ConfigType = delegate.ConfigType

  override def configParser: ConfigReader[ConfigType] =
    delegate.configParser

  override def configWriter(
      confidential: Boolean
  ): ConfigWriter[ConfigType] = delegate.configWriter(confidential)

  override def create(
      config: ConfigType,
      nonStandardConfig: Boolean,
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): SequencerDriver =
    delegate.create(
      config,
      nonStandardConfig,
      timeProvider,
      firstBlockHeight,
      loggerFactory,
    )

  override def usesTimeProvider: Boolean = delegate.usesTimeProvider
}
