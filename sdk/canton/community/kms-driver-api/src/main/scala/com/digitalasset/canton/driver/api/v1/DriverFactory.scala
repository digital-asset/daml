// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.driver.api.v1

import com.digitalasset.canton.driver.api
import org.slf4j.Logger
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

/** The corresponding factory for an implementation of a [[Driver]] that can instantiate a new driver. */
trait DriverFactory extends api.DriverFactory {

  /** The name of the driver that is instantiated by an implementation of the driver factory. */
  def name: String

  /** The version of the driver API this factory is implemented against. */
  def version: Int

  /** Optional information for the build of the driver factory, e.g., git commit hash. */
  def buildInfo: Option[String]

  /** The driver-specific configuration type. */
  type ConfigType

  /** The parser to load the driver-specific configuration. */
  def configReader: ConfigReader[ConfigType]

  /** The configuration writer for the driver-specific configuration.
    *
    * @param confidential If the flag is true, the config writer should omit any sensitive configuration items, such as credentials.
    */
  def configWriter(confidential: Boolean): ConfigWriter[ConfigType]

  /** The creation method of a driver by this factory.
    * If the creation of the driver fails this method should throw an exception.
    *
    * @param config The driver-specific configuration.
    * @param loggerFactory A logger factory that should be used by the driver to create a logger for a particular class.
    * @param executionContext The execution context that should be used by the driver.
    *
    * @return A new instance of [[Driver]].
    */
  def create(
      config: ConfigType,
      loggerFactory: Class[_] => Logger,
      executionContext: ExecutionContext,
  ): Driver
}
