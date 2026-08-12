// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver.api

trait DriverFactory {

  /** The type of the driver that is instantiated by an implementation of the driver factory. */
  type Driver

  /** The name of the driver that is instantiated by an implementation of the driver factory. */
  def name: String

  /** The version of the driver API this factory is implemented against. */
  def version: Int

}
