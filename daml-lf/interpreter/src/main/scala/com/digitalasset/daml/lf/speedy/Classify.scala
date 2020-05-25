// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.speedy.Speedy._
import com.daml.lf.speedy.SExpr._

object Classify { // classify the machine state w.r.t what step occurs next

  final class Counts(
      var ctrlExpr: Int = 0,
      var ctrlValue: Int = 0,
      // expression classification (ctrlExpr)
      var evalue: Int = 0,
      var evarS: Int = 0,
      var evarA: Int = 0,
      var evarF: Int = 0,
      var eapp: Int = 0,
      var eclose: Int = 0,
      var ebuiltin: Int = 0,
      var eval: Int = 0,
      var elocation: Int = 0,
      var elet: Int = 0,
      var ecase: Int = 0,
      var erecdef: Int = 0,
      var ecatch: Int = 0,
      var eimportvalue: Int = 0,
      var ewrongcid: Int = 0,
      // kont classification (ctrlValue)
      var kfinished: Int = 0,
      var karg: Int = 0,
      var kfun: Int = 0,
      var kpushto: Int = 0,
      var kcacheval: Int = 0,
      var klocation: Int = 0,
      var kmatch: Int = 0,
      var kcatch: Int = 0,
  ) {
    def steps = ctrlExpr + ctrlValue
    def pp: String = {
      List(
        ("CtrlExpr:", ctrlExpr),
        ("- evalue", evalue),
        ("- evarS", evarS),
        ("- evarA", evarA),
        ("- evarF", evarF),
        ("- eapp", eapp),
        ("- eclose", eclose),
        ("- ebuiltin", ebuiltin),
        ("- eval", eval),
        ("- elocation", elocation),
        ("- elet", elet),
        ("- ecase", ecase),
        ("- erecdef", erecdef),
        ("- ecatch", ecatch),
        ("- eimportvalue", eimportvalue),
        ("CtrlValue:", ctrlValue),
        ("- kfinished", kfinished),
        ("- karg", karg),
        ("- kfun", kfun),
        ("- kpushto", kpushto),
        ("- kcacheval", kcacheval),
        ("- klocation", klocation),
        ("- kmatch", kmatch),
        ("- kcatch", kcatch),
      ).map { case (tag, n) => s"$tag : $n" }.mkString("\n")
    }
  }

  def classifyMachine(machine: Machine, counts: Counts): Unit = {
    if (machine.returnValue != null) {
      // classify a value by the continution it is about to return to
      counts.ctrlValue += 1
      val kont = machine.kontStack.get(machine.kontStack.size - 1)
      classifyKont(kont, counts)
    } else {
      counts.ctrlExpr += 1
      classifyExpr(machine.ctrl, counts)
    }
  }

  def classifyExpr(exp: SExpr, counts: Counts): Unit = {
    exp match {
      case _: SEVar => //not expected at runtime
      case _: SEAbs => //not expected at runtime
      case _: SEValue => counts.evalue += 1
      case _: SELocS => counts.evarS += 1
      case _: SELocA => counts.evarA += 1
      case _: SELocF => counts.evarF += 1
      case _: SEApp => counts.eapp += 1
      case _: SEMakeClo => counts.eclose += 1
      case _: SEBuiltin => counts.ebuiltin += 1
      case _: SEVal => counts.eval += 1
      case _: SELocation => counts.elocation += 1
      case _: SELet => counts.elet += 1
      case _: SECase => counts.ecase += 1
      case _: SEBuiltinRecursiveDefinition => counts.erecdef += 1
      case _: SECatch => counts.ecatch += 1
      case _: SELabelClosure => ()
      case _: SEImportValue => counts.eimportvalue += 1
      case _: SEWronglyTypeContractId => counts.ewrongcid += 1
    }
  }

  def classifyKont(kont: Kont, counts: Counts): Unit = {
    kont match {
      case KFinished => counts.kfinished += 1
      case _: KArg => counts.karg += 1
      case _: KFun => counts.kfun += 1
      case _: KPushTo => counts.kpushto += 1
      case _: KCacheVal => counts.kcacheval += 1
      case _: KLocation => counts.klocation += 1
      case _: KMatch => counts.kmatch += 1
      case _: KCatch => counts.kcatch += 1
      case _: KLabelClosure => ()
      case _: KLeaveClosure => ()
    }
  }

}
