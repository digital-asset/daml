// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import java.util.concurrent.atomic.AtomicReference

object Gauges {

  trait GaugeWithUpdate[T] {

    def updateValue(x: T): Unit
  }

  case class VarGauge[T](initial: T) extends GaugeWithUpdate[T] {
    private val ref = new AtomicReference[T](initial)

    def updateValue(x: T): Unit = ref.set(x)

    def updateValue(up: T => T): Unit = {
      val _ = ref.updateAndGet { value =>
        up(value)
      }
    }

    def getValue: T = ref.get()

  }
}
