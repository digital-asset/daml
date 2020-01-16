// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.resources

import java.util.concurrent.atomic.AtomicBoolean

final class MockConstructor[T](construct: AtomicBoolean => T) {
  private val acquired = new AtomicBoolean(false)

  def hasBeenAcquired: Boolean = acquired.get

  def apply(): T = construct(acquired)
}
