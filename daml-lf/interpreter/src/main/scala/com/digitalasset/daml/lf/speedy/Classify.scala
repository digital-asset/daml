// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.speedy.Speedy.{Control, Machine}
import com.daml.scalautil.Statement.discard

import scala.collection.mutable

private[speedy] object Classify { // classify the machine state w.r.t what step occurs next

  final class Counts() {
    private[this] var ctrlExpr: Int = 0

    private[this] val exprs: mutable.Map[String, Int] = mutable.Map.empty
    private[this] val konts: mutable.Map[String, Int] = mutable.Map.empty

    def addKont(kont: String): Unit = {
      incrCtrl()
      discard(konts += kont -> (konts.getOrElse(kont, 0) + 1))
    }

    def addExpr(expr: String): Unit = {
      incrCtrl()
      discard(exprs += expr -> (exprs.getOrElse(expr, 0) + 1))
    }

    def steps: Int = ctrlExpr

    def pp: String = {
      val lines =
        (("CtrlExpr:", ctrlExpr) :: exprs.toList.map { case (expr, n) => ("- " + expr, n) }) ++
          (("CtrlValue:", 0) :: konts.toList.map { case (kont, n) => (" -" + kont, n) })
      lines.map { case (tag, n) => s"$tag : $n" }.mkString("\n")
    }

    private def incrCtrl(): Unit = ctrlExpr += 1
  }

  def classifyMachine(machine: Machine, counts: Counts): Unit = {
    machine.currentControl match {
      case Control.Value(_) =>
        // classify a value by the continuation it is about to return to
        val kont = machine.peekKontStackEnd().getClass.getSimpleName
        counts.addKont(kont)
      case Control.Expression(exp) =>
        val expr = exp.getClass.getSimpleName
        counts.addExpr(expr)
      case _ => ()
    }
  }
}
