// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.DummyDriverFactory.DummyConfig
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.synchronizer.block.{SequencerDriver, SequencerDriverFactory}
import com.digitalasset.canton.time.TimeProvider
import org.apache.pekko.stream.Materializer
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

class DummyDriverFactory extends SequencerDriverFactory {
  override def name: String = "dummy"

  override def version: Int = SequencerDriver.DriverApiVersion

  override type ConfigType = DummyConfig

  override def configParser: ConfigReader[DummyConfig] = {
    import pureconfig.generic.semiauto.*
    deriveReader[DummyConfig]
  }

  override def configWriter(confidential: Boolean): ConfigWriter[DummyConfig] = {
    import pureconfig.generic.semiauto.*
    implicit val passwordWriter: ConfigWriter[Password] =
      new ConfidentialConfigWriter(confidential)[Password](_.copy(pw = "****"))
    deriveWriter[DummyConfig]
  }

  override def create(
      config: DummyConfig,
      nonStandardConfig: Boolean,
      timeProvider: TimeProvider,
      firstBlockHeight: Option[Long],
      synchronizerId: String,
      sequencerId: String,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, materializer: Materializer): SequencerDriver = ???

  override def usesTimeProvider: Boolean = false
}

object DummyDriverFactory {
  final case class DummyConfig(password: Password, value: String)
}
