// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver.v2

import org.slf4j.Logger
import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

private[driver] class TestDriver1Factory extends TestDriverFactory {

  override def name: String = "test1"

  override def buildInfo: Option[String] = None

  override type ConfigType = TestDriver1Config

  override def configReader: ConfigReader[TestDriver1Config] = deriveReader[TestDriver1Config]

  override def configWriter(confidential: Boolean): ConfigWriter[TestDriver1Config] =
    deriveWriter[TestDriver1Config]

  override def create(
      config: TestDriver1Config,
      loggerFactory: Class[_] => Logger,
      executionContext: ExecutionContext,
  ): TestDriver =
    new TestDriver1(config, loggerFactory)
}

private[driver] class TestDriver1(config: TestDriver1Config, loggerFactory: Class[_] => Logger)
    extends TestDriver {

  private val logger: Logger = loggerFactory(getClass)

  logger.debug(s"Loaded test driver1 with config: $config")

  def test: Int =
    config.testString.toIntOption.getOrElse(42)

}

private[driver] final case class TestDriver1Config(testString: String)
