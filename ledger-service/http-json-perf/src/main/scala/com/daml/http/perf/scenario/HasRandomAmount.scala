// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.perf.scenario

private[scenario] trait HasRandomAmount {
  private val rng = new scala.util.Random(123456789)

  // can be called from two different scenarios, need to synchronize access
  final def randomAmount(): Int = {
    val x = this.synchronized { rng.nextInt(10) }
    x + 5 // [5, 15)
  }
}
