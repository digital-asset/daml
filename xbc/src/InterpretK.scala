// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package xbc

import scala.annotation.tailrec

object InterpretK { // cps-style interpreter for Lang (for stack-safety)

  import Lang._

  type Value = Long

  sealed abstract trait Trav
  final case class Down(actuals: Vector[Value], exp: Exp) extends Trav
  final case class Up(res: Value) extends Trav

  sealed abstract trait K
  final case object KRet extends K
  final case class KBin1(b: BinOp, actuals: Vector[Value], e2: Exp, k: K) extends K
  final case class KBin2(b: BinOp, res1: Value, k: K) extends K
  final case class KIf(actuals: Vector[Value], e2: Exp, e3: Exp, k: K) extends K
  final case class KMoreArgs(
      actuals: Vector[Value],
      results: List[Value],
      exps: List[Exp],
      body: Exp,
      k: K,
  ) extends K

  def cps(program: Program): Value = {
    program match {
      case Program(defs, main) =>
        val _ = defs

        @tailrec
        def loop(trav: Trav, k: K): Value = {
          trav match {
            case Down(actuals, exp) =>
              exp match {
                case Num(x) => loop(Up(x), k)
                case Builtin(b, e1, e2) => loop(Down(actuals, e1), KBin1(b, actuals, e2, k))
                case Arg(i) => loop(Up(actuals(i)), k)
                case IfNeg(e1, e2, e3) => loop(Down(actuals, e1), KIf(actuals, e2, e3, k))
                case FnCall(fnName, args) =>
                  val arity = args.length
                  defs.get(fnName, arity) match {
                    case None => sys.error(s"FnCall: $fnName/$arity")
                    case Some(body) =>
                      args match {
                        case Nil =>
                          loop(Down(Vector(), body), k)
                        case arg :: args =>
                          loop(Down(actuals, arg), KMoreArgs(actuals, Nil, args, body, k))
                      }
                  }
              }
            case Up(res) =>
              k match {
                case KRet => res
                case KBin1(b, actuals, e2, k) => loop(Down(actuals, e2), KBin2(b, res, k))
                case KBin2(b, res1, k) => loop(Up(applyBinOp(b, res1, res)), k)
                case KIf(actuals, e2, e3, k) =>
                  if (res < 0) loop(Down(actuals, e2), k) else loop(Down(actuals, e3), k)
                case KMoreArgs(actuals, results0, args, body, k) =>
                  val results = res :: results0
                  args match {
                    case Nil =>
                      val vs = results.reverse.toVector
                      loop(Down(vs, body), k)
                    case arg :: args =>
                      loop(Down(actuals, arg), KMoreArgs(actuals, results, args, body, k))
                  }
              }
          }
        }

        loop(Down(Vector(), main), KRet)
    }
  }

  def applyBinOp(binOp: BinOp, v1: Value, v2: Value): Value = {
    binOp match {
      case MulOp => v1 * v2
      case AddOp => v1 + v2
      case SubOp => v1 - v2
      case CmpOp => if (v1 > v2) 1 else if (v1 == v2) 0 else -1
    }
  }

}
