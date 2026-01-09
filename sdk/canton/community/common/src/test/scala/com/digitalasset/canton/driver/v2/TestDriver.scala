// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver.v2

import com.digitalasset.canton.driver

private[driver] trait TestDriverFactory extends driver.TestDriverFactory {
  override def version: Int = 2

  override type Driver = TestDriver
}

private[v2] trait TestDriver extends driver.TestDriver {

  // Breaking change from v1 to v2: String -> Int return type
  def test: Int
}
