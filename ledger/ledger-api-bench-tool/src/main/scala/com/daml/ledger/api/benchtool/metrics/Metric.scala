// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import com.daml.ledger.api.benchtool.metrics.objectives.ServiceLevelObjective

import java.time.Duration

trait Metric[Elem] {

  type V <: MetricValue

  type Objective <: ServiceLevelObjective[V]

  def onNext(value: Elem): Metric[Elem]

  def periodicValue(periodDuration: Duration): (Metric[Elem], V)

  def finalValue(totalDuration: Duration): V

  def violatedObjective: Option[(Objective, V)] = None

  def name: String = getClass.getSimpleName

}

object Metric {
  def rounded(value: Double): String = "%.2f".format(value)
}
