// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver

import com.digitalasset.canton.buildinfo.BuildInfo
import org.slf4j.Logger
import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

private[driver] class AnotherTestDriver1Factory extends AnotherTestDriverFactory {

  override def name: String = "test1"

  override def version: Int = 1

  override def buildInfo: Option[String] = Some(BuildInfo.version)

  override type ConfigType = AnotherTestDriver1Config

  override def configReader: ConfigReader[AnotherTestDriver1Config] =
    deriveReader[AnotherTestDriver1Config]

  override def configWriter(confidential: Boolean): ConfigWriter[AnotherTestDriver1Config] =
    deriveWriter[AnotherTestDriver1Config]

  override def create(
      config: AnotherTestDriver1Config,
      loggerFactory: Class[?] => Logger,
      executionContext: ExecutionContext,
  ): AnotherTestDriver =
    new AnotherTestDriver1(config, loggerFactory)
}

class AnotherTestDriver1(config: AnotherTestDriver1Config, loggerFactory: Class[?] => Logger)
    extends AnotherTestDriver {

  private val logger: Logger = loggerFactory(getClass)

  logger.debug(s"Loaded another test driver1 with config: $config")

  override def anotherTest: Int =
    config.testInt

}

private[driver] final case class AnotherTestDriver1Config(testInt: Int)
