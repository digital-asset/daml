// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool.metrics

import java.time.Duration

trait Metric[Elem] {

  type V <: MetricValue

  type Objective <: ServiceLevelObjective[V]

  /** @return an updated version of itself
    */
  def onNext(value: Elem): Metric[Elem]

  /** @return an updated version of itself and the value observed in this period
    *
    * NOTE: Durations of subsequent periods are not guaranteed to be exactly the same.
    */
  def periodicValue(periodDuration: Duration): (Metric[Elem], V)

  def finalValue(totalDuration: Duration): V

  /** @return a list of objective violations, where each element is a pair of
    *         a violated objective and the periodic value that violates it the most.
    */
  def violatedPeriodicObjectives: List[(Objective, V)] = Nil

  /** @return a list of objective violations, where each element is a pair of
    *         a violated objective and the final value that violates it.
    */
  def violatedFinalObjectives(totalDuration: Duration): List[(Objective, V)]

  def name: String = getClass.getSimpleName

}

object Metric {
  def rounded(value: Double): String = "%.2f".format(value)
}
