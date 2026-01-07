// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver.v3

import com.digitalasset.canton.driver

private[driver] trait TestDriverFactory extends driver.TestDriverFactory {
  override def version: Int = 3

  override type Driver = TestDriver
}

private[v3] trait TestDriver extends driver.TestDriver {

  // Breaking change from v2 to v3: Int -> Boolean return type
  def test: Boolean
}
