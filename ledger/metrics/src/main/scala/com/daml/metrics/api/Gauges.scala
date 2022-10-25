// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import java.util.concurrent.atomic.AtomicReference

import com.codahale.metrics.Gauge

object Gauges {

  trait GaugeWithUpdate[T] extends Gauge[T] {

    def updateValue(x: T): Unit
  }

  case class VarGauge[T](initial: T) extends GaugeWithUpdate[T] {
    private val ref = new AtomicReference[T](initial)
    def updateValue(x: T): Unit = ref.set(x)
    def updateValue(up: T => T): Unit = {
      val _ = ref.updateAndGet(up(_))
    }
    override def getValue: T = ref.get()
  }
}
