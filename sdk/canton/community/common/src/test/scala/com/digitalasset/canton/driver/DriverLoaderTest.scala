// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver

import com.digitalasset.canton.driver.v1.DriverLoader
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext}
import com.typesafe.config.ConfigValueFactory
import pureconfig.ConfigSource

import scala.jdk.CollectionConverters.*

class DriverLoaderTest extends BaseTestWordSpec with HasExecutionContext {

  private def driverConfig(configString: String): TestConfig.Driver = {
    val config = ConfigSource
      .string(configString)
      .load[TestConfig]
      .valueOrFail("config loading")

    config match {
      case _: TestConfig.Builtin => fail("Config should be for a driver")
      case driverConfig: TestConfig.Driver => driverConfig
    }
  }

  "DriverLoader" can {

    "load and run a test driver" in {

      val config = driverConfig(
        "{ type = driver, name = test1, test-int = 23, config { test-string = foobar } }"
      )

      val driver = DriverLoader
        .load[v1.TestDriverFactory, TestDriverFactory](
          config.name,
          config.config,
          loggerFactory,
          executorService,
        )
        .valueOrFail("load driver")

      driver.test shouldEqual "FOOBAR"

    }

    "load and run second test driver implementation" in {

      val config = driverConfig(
        "{ type = driver, name = test2, test-int = 23, config { another-test-string = foobar } }"
      )

      val driver = DriverLoader
        .load[v1.TestDriverFactory, TestDriverFactory](
          config.name,
          config.config,
          loggerFactory,
          executorService,
        )
        .valueOrFail("load driver")

      driver.test shouldEqual ("foobar".reverse)

    }

    "load and run another test driver with a different factory" in {

      val config = ConfigValueFactory.fromMap(Map("test-int" -> 42).asJava)

      val driver = DriverLoader
        .load[AnotherTestDriverFactory, AnotherTestDriverFactory](
          "test1",
          config,
          loggerFactory,
          executorService,
        )
        .valueOrFail("load another driver")

      driver.anotherTest shouldEqual 42

    }

    "load and run a driver implementation of a different version" in {
      val config = driverConfig(
        "{ type = driver, name = test1, test-int = 5, config { test-string = 23 } }"
      )

      val driver = DriverLoader
        .load[v2.TestDriverFactory, TestDriverFactory](
          config.name,
          config.config,
          loggerFactory,
          executorService,
        )
        .valueOrFail("load driver")

      driver.test shouldEqual 23

    }

    "fail to load driver with invalid driver config" in {

      // Invalid driver config with an unknown key
      val config = driverConfig(
        "{ type = driver, name = test1, test-int = 23, config { random-string = foobar } }"
      )

      DriverLoader
        .load[v1.TestDriverFactory, TestDriverFactory](
          config.name,
          config.config,
          loggerFactory,
          executorService,
        )
        .tapLeft(err => logger.debug(s"Expected error: $err"))
        .left
        .value should include("Failed to read driver config")

    }

    "fail to load unknown driver" in {

      DriverLoader
        .load[v1.TestDriverFactory, TestDriverFactory](
          "foobar",
          // Random config, does not matter
          ConfigValueFactory.fromAnyRef(42),
          loggerFactory,
          executorService,
        )
        .tapLeft(err => logger.debug(s"Expected error: $err"))
        .left
        .value should include(
        "Driver 'foobar' (interface com.digitalasset.canton.driver.v1.TestDriverFactory) not found. Found drivers: 'test1 v1', 'test2 v1', 'test1 v2'"
      )
    }

    "fail to load a driver with wrong driver API version" in {

      DriverLoader
        .load[v3.TestDriverFactory, TestDriverFactory](
          "test1",
          // Random config, does not matter
          ConfigValueFactory.fromAnyRef(42),
          loggerFactory,
          executorService,
        )
        .tapLeft(err => logger.debug(s"Expected error: $err"))
        .left
        .value should include(
        "Driver 'test1' (interface com.digitalasset.canton.driver.v3.TestDriverFactory) not found. Found drivers: 'test1 v1', 'test2 v1', 'test1 v2'"
      )
    }

  }

}
