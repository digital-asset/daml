// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.speedy.Speedy.{Machine, Control}
import scala.collection.mutable.Map

private[speedy] object Classify { // classify the machine state w.r.t what step occurs next

  final class Counts(
      var ctrlExpr: Int = 0,
      var ctrlValue: Int = 0,
      var exprs: Map[String, Int] = Map.empty,
      var konts: Map[String, Int] = Map.empty,
  ) {
    def steps = ctrlExpr + ctrlValue
    def pp: String = {
      val lines =
        (("CtrlExpr:", ctrlExpr) :: exprs.toList.map { case (expr, n) => ("- " + expr, n) }) ++
          (("CtrlValue:", ctrlValue) :: konts.toList.map { case (kont, n) => (" -" + kont, n) })
      lines.map { case (tag, n) => s"$tag : $n" }.mkString("\n")
    }
  }

  def classifyMachine(machine: Machine, counts: Counts): Unit = {
    machine.control match {
      case Control.Value(_) =>
        // classify a value by the continution it is about to return to
        counts.ctrlValue += 1
        val kont = machine.kontStack.get(machine.kontStack.size - 1).getClass.getSimpleName
        val _ = counts.konts += kont -> (counts.konts.get(kont).getOrElse(0) + 1)
      case Control.Expression(exp) =>
        counts.ctrlExpr += 1
        val expr = exp.getClass.getSimpleName
        val _ = counts.exprs += expr -> (counts.exprs.get(expr).getOrElse(0) + 1)
      case _ => ()
    }
  }
}
