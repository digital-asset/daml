// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.driver.v1

import com.digitalasset.canton.driver

private[driver] trait TestDriverFactory extends driver.TestDriverFactory {
  override def version: Int = 1

  override type Driver = TestDriver
}

private[v1] trait TestDriver extends driver.TestDriver {
  def test: String
}
