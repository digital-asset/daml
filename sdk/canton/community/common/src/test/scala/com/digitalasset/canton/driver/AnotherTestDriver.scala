// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver

import com.digitalasset.canton.driver.api.v1.DriverFactory

private[driver] trait AnotherTestDriverFactory extends DriverFactory {
  override type Driver = AnotherTestDriver
}

private[driver] trait AnotherTestDriver {
  def anotherTest: Int
}
