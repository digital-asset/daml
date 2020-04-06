// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.common.metrics

import java.util.concurrent.atomic.AtomicReference
import com.codahale.metrics.Gauge

case class VarGauge[T](initial: T) extends Gauge[T] {
  private val ref = new AtomicReference[T](initial)
  def updateValue(x: T): Unit = ref.set(x)
  override def getValue: T = ref.get()
}
