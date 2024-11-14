// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton.driver.api

trait DriverFactory {

  /** The type of the driver that is instantiated by an implementation of the driver factory. */
  type Driver

  /** The name of the driver that is instantiated by an implementation of the driver factory. */
  def name: String

  /** The version of the driver API this factory is implemented against. */
  def version: Int

}
