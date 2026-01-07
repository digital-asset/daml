// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver.v1

import org.slf4j.Logger
import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

private[driver] class TestDriver2Factory extends TestDriverFactory {

  override def name: String = "test2"

  override def buildInfo: Option[String] = None

  override type ConfigType = TestDriver2Config

  override def configReader: ConfigReader[TestDriver2Config] = deriveReader[TestDriver2Config]

  override def configWriter(confidential: Boolean): ConfigWriter[TestDriver2Config] =
    deriveWriter[TestDriver2Config]

  override def create(
      config: TestDriver2Config,
      loggerFactory: Class[?] => Logger,
      executionContext: ExecutionContext,
  ): TestDriver =
    new TestDriver2(config, loggerFactory)
}

private[driver] class TestDriver2(config: TestDriver2Config, loggerFactory: Class[?] => Logger)
    extends TestDriver {

  private val logger: Logger = loggerFactory(getClass)

  logger.debug(s"Loaded test driver2 with config: $config")

  def test: String =
    // Do something in the driver based on a config value to ensure the right config is passed in and the driver is called
    config.anotherTestString.reverse

}

private[driver] final case class TestDriver2Config(anotherTestString: String)
