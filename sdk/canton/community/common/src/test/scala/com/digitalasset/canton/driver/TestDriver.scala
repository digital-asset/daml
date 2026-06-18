// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver

import com.digitalasset.canton.driver.api.v1.DriverFactory
import com.typesafe.config.ConfigObject
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

trait TestDriver

trait TestDriverFactory extends DriverFactory {
  override type Driver <: TestDriver
}

private[driver] sealed trait TestConfig {
  def testInt: Int
}

private[driver] object TestConfig {
  final case class Builtin(testInt: Int) extends TestConfig
  final case class Driver(name: String, testInt: Int, config: ConfigObject) extends TestConfig

  implicit val builtinReader: ConfigReader[Builtin] = deriveReader[Builtin]
  implicit val driverReader: ConfigReader[Driver] = deriveReader[Driver]
  implicit val reader: ConfigReader[TestConfig] = deriveReader[TestConfig]

}
