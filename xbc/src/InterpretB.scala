// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

import scala.annotation.tailrec

object InterpretB { // interpreter for Lang, producing boxed values
  import Lang._

  type Value = BoxedValue

  def run(program: Program): Value = {
    program match {
      case Program(defs, main) =>
        def nested_evalFrame = evalFrame _

        @tailrec
        def evalFrame(actuals: Vector[Value], body: Exp): Value = {

          def eval(exp: Exp): Value =
            exp match {
              case Num(x) => BoxedValue.Number(x)
              case Builtin(binOp, e1, e2) => applyBinOp(binOp, eval(e1), eval(e2))
              case IfNeg(e1, e2, e3) =>
                if (BoxedValue.isNeg(eval(e1))) eval(e2) else eval(e3)
              case Arg(i) => actuals(i)
              case FnCall(fnName, args) =>
                val arity = args.length
                defs.get(fnName, arity) match {
                  case None => sys.error(s"FnCall: $fnName/$arity")
                  case Some(body) =>
                    val vs = args.map(eval(_)).toVector
                    nested_evalFrame(vs, body)
                }
            }

          body match {
            case IfNeg(e1, e2, e3) =>
              if (BoxedValue.isNeg(eval(e1))) evalFrame(actuals, e2) else evalFrame(actuals, e3)
            case FnCall(fnName, args) =>
              val arity = args.length
              defs.get(fnName, arity) match {
                case None => sys.error(s"FnCall: $fnName/$arity")
                case Some(body) =>
                  val vs = args.map(eval(_)).toVector
                  evalFrame(vs, body) //tailcall
              }
            case _ =>
              eval(body)
          }
        }

        evalFrame(Vector(), main)
    }
  }

  def applyBinOp(binOp: BinOp, v1: Value, v2: Value): Value = {
    binOp match {
      case MulOp => BoxedValue.mul(v1, v2)
      case AddOp => BoxedValue.add(v1, v2)
      case SubOp => BoxedValue.sub(v1, v2)
      case CmpOp => BoxedValue.cmp(v1, v2)
    }
  }

}
