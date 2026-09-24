// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver.v1

import cats.syntax.either.*
import com.digitalasset.canton.driver.api
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.util.ErrorUtil
import com.typesafe.config.ConfigValue

import java.util.ServiceLoader
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.reflect.{ClassTag, classTag}

object DriverLoader {

  /** Loads a driver factory based on Java Service Providers and instantiates a driver with the
    * factory.
    *
    * @param driverName
    *   The name of the driver to instantiate.
    * @param driverApiVersion
    *   The version of the driver API.
    * @param rawConfig
    *   The driver-specific raw configuration that will be passed into the driver.
    * @param loggerFactory
    *   The logger factory that is used by the factory and driver to get loggers.
    * @param executionContext
    *   The execution context to be used by the driver to schedule threads.
    *
    * @tparam Factory
    *   A sub-type of a [[driver.api.DriverFactory]] for a particular driver kind and version.
    * @tparam BaseFactory
    *   A sub-type of a [[driver.api.DriverFactory]] for a particular driver kind.
    * @return
    *   Either the instantiated driver or an error.
    */
  def load[Factory <: api.v1.DriverFactory: ClassTag, BaseFactory <: api.DriverFactory: ClassTag](
      driverName: String,
      rawConfig: ConfigValue,
      loggerFactory: NamedLoggerFactory,
      executionContext: ExecutionContext,
  ): Either[String, Factory#Driver] = {
    val logger = loggerFactory.getLogger(DriverLoader.getClass)

    for {
      factory <- DriverFactoryLoader.load[Factory, BaseFactory](driverName)
      _ = factory.buildInfo.foreach { build =>
        logger.info(
          s"Loaded driver $driverName (API v${factory.version}) build $build"
        )
      }
      config <-
        factory.configReader
          .from(rawConfig)
          .leftMap(err => s"Failed to read driver config for $driverName: $err")
      driver <- Either
        .catchNonFatal {
          factory.create(
            config,
            clazz => loggerFactory.getLogger(clazz).underlying,
            executionContext,
          )
        }
        .leftMap { err =>
          s"Failed to load driver $driverName: ${ErrorUtil.messageWithStacktrace(err)}"
        }
    } yield driver
  }

}

object DriverFactoryLoader {

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def loadFactories[Factory <: api.DriverFactory: ClassTag]: List[Factory] =
    ServiceLoader
      .load(classTag[Factory].runtimeClass.asInstanceOf[Class[Factory]])
      .iterator()
      .asScala
      .toList

  /** Load a [[driver.api.DriverFactory]] based on Java Service Providers, with the given driver
    * name and version.
    *
    * @param driverName
    *   The name of the driver that implements the particular driver factory.
    *
    * @tparam Factory
    *   A sub-type of a [[driver.api.DriverFactory]] for a particular driver kind.
    * @return
    *   Either the matching factory or an error.
    */
  def load[Factory <: api.v1.DriverFactory: ClassTag, BaseFactory <: api.DriverFactory: ClassTag](
      driverName: String
  ): Either[String, Factory] =
    loadFactories[Factory].find(_.name == driverName).toRight {
      // Try to load all factories for a particular kind, but not a specific version
      val drivers =
        loadFactories[BaseFactory].map(f => "'" + f.name + " v" + f.version + "'").mkString(", ")
      s"Driver '$driverName' (${classTag[Factory].runtimeClass}) not found. Found drivers: $drivers"
    }

}
