// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics.api

import java.util.concurrent.atomic.AtomicReference

import com.codahale.metrics.Gauge

object Gauges {

  trait GaugeWithUpdate[T] extends Gauge[T] {

    def updateValue(x: T)(implicit
        context: MetricsContext
    ): Unit
  }

  case class VarGauge[T](initial: T)(implicit context: MetricsContext) extends GaugeWithUpdate[T] {
    private val ref = new AtomicReference[(T, MetricsContext)](initial -> context)
    def updateValue(x: T)(implicit
        context: MetricsContext
    ): Unit = ref.set(x -> context)
    def updateValue(up: T => T)(implicit
        context: MetricsContext
    ): Unit = {
      val _ = ref.updateAndGet { case (value, _) =>
        up(value) -> context
      }
    }
    override def getValue: T = ref.get()._1

    def getValueAndContext: (T, MetricsContext) = ref.get()
  }
}
