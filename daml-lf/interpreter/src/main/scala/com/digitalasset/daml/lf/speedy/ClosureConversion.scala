// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/**  Closure Conversion (Phase of the speedy compiler pipeline)
  *
  *  This compilation phase transforms from SExpr0 to SExpr0.
  *    SExpr0 contains expression forms which exist during the speedy compilation pipeline.
  *
  * TODO: introduces new expression type (SExpr1) for the result of this phase, and input to the
  * following ANF transformation phase.
  */

import com.daml.lf.speedy.{SExpr0 => source}
import com.daml.lf.speedy.{SExpr1 => target}

private[speedy] object ClosureConversion {

  case class CompilationError(error: String) extends RuntimeException(error, null, true, false)

  /** Convert abstractions in a speedy expression into
    * explicit closure creations.
    * This step computes the free variables in an abstraction
    * body, then translates the references in the body into
    * references to the immediate top of the argument stack,
    * and changes the abstraction into a closure creation node
    * describing the free variables that need to be captured.
    *
    * For example:
    *   SELet(..two-bindings..) in
    *     SEAbs(2,
    *       SEVar(4) ..             [reference to first let-bound variable]
    *       SEVar(2))               [reference to first function-arg]
    * =>
    *   SELet(..two-bindings..) in
    *     SEMakeClo(
    *       Array(SELocS(2)),       [capture the first let-bound variable, from the stack]
    *       2,
    *       SELocF(0) ..            [reference the first let-bound variable via the closure]
    *       SELocA(0))              [reference the first function arg]
    */

  // TODO: Introduce a new type expression for the result of closure conversion
  private[speedy] def closureConvert(expr: source.SExpr): target.SExpr = {
    closureConvert(Map.empty, expr)
  }

  private def closureConvert(remaps: Map[Int, target.SELoc], expr: source.SExpr): target.SExpr = {

    // remaps is a function which maps the relative offset from variables (SEVar) to their runtime location
    // The Map must contain a binding for every variable referenced.
    // The Map is consulted when translating variable references (SEVar) and free variables of an abstraction (SEAbs)
    def remap(i: Int): target.SELoc =
      remaps.get(i) match {
        case Some(loc) => loc
        case None =>
          throw CompilationError(s"remap($i),remaps=$remaps")
      }
    expr match {
      case source.SEVar(i) => remap(i)
      case source.SEVal(ref) => target.SEVal(ref)
      case source.SEBuiltin(b) => target.SEBuiltin(b)
      case source.SEValue(v) => target.SEValue(v)
      case source.SEBuiltinRecursiveDefinition(f) => target.SEBuiltinRecursiveDefinition(f)
      case source.SELocation(loc, body) =>
        target.SELocation(loc, closureConvert(remaps, body))

      case source.SEAbs(0, _) =>
        throw CompilationError("empty SEAbs")

      case source.SEAbs(arity, body) =>
        val fvs = freeVars(body, arity).toList.sorted
        val newRemapsF: Map[Int, target.SELoc] = fvs.zipWithIndex.map { case (orig, i) =>
          (orig + arity) -> target.SELocF(i)
        }.toMap
        val newRemapsA = (1 to arity).map { case i =>
          i -> target.SELocA(arity - i)
        }
        // The keys in newRemapsF and newRemapsA are disjoint
        val newBody = closureConvert(newRemapsF ++ newRemapsA, body)
        target.SEMakeClo(fvs.map(remap).toArray, arity, newBody)

      case source.SEAppGeneral(fun, args) =>
        val newFun = closureConvert(remaps, fun)
        val newArgs = args.map(closureConvert(remaps, _))
        target.SEApp(newFun, newArgs)

      case source.SECase(scrut, alts) =>
        target.SECase(
          closureConvert(remaps, scrut),
          alts.map { case source.SCaseAlt(pat, body) =>
            val n = pat.numArgs
            target.SCaseAlt(
              pat,
              closureConvert(shift(remaps, n), body),
            )
          },
        )

      case source.SELet(bounds, body) =>
        target.SELet(
          bounds.zipWithIndex.map { case (b, i) =>
            closureConvert(shift(remaps, i), b)
          },
          closureConvert(shift(remaps, bounds.length), body),
        )

      case source.SETryCatch(body, handler) =>
        target.SETryCatch(
          closureConvert(remaps, body),
          closureConvert(shift(remaps, 1), handler),
        )

      case source.SEScopeExercise(body) =>
        target.SEScopeExercise(closureConvert(remaps, body))

      case source.SELabelClosure(label, expr) =>
        target.SELabelClosure(label, closureConvert(remaps, expr))

      case source.SELet1General(bound, body) =>
        target.SELet1General(closureConvert(remaps, bound), closureConvert(shift(remaps, 1), body))

      case _: source.SELoc | _: source.SEMakeClo | _: source.SEDamlException |
          _: source.SEImportValue =>
        throw CompilationError(s"closureConvert: unexpected $expr")
    }
  }

  // Modify/extend `remaps` to reflect when new values are pushed on the stack.  This
  // happens as we traverse into SELet and SECase bodies which have bindings which at
  // runtime will appear on the stack.
  // We must modify `remaps` because it is keyed by indexes relative to the end of the stack.
  // And any values in the map which are of the form SELocS must also be _shifted_
  // because SELocS indexes are also relative to the end of the stack.
  private[this] def shift(remaps: Map[Int, target.SELoc], n: Int): Map[Int, target.SELoc] = {

    // We must update both the keys of the map (the relative-indexes from the original SEVar)
    // And also any values in the map which are stack located (SELocS), which are also indexed relatively
    val m1 = remaps.map { case (k, loc) => (n + k, shiftLoc(loc, n)) }

    // And create mappings for the `n` new stack items
    val m2 = (1 to n).map(i => (i, target.SELocS(i)))

    m1 ++ m2
  }

  private[this] def shiftLoc(loc: target.SELoc, n: Int): target.SELoc = loc match {
    case target.SELocS(i) => target.SELocS(i + n)
    case target.SELocA(_) | target.SELocF(_) => loc
  }

  /** Compute the free variables in a speedy expression.
    * The returned free variables are de bruijn indices
    * adjusted to the stack of the caller.
    */
  private[this] def freeVars(expr: source.SExpr, initiallyBound: Int): Set[Int] = {
    def go(expr: source.SExpr, bound: Int, free: Set[Int]): Set[Int] =
      expr match {
        case source.SEVar(i) =>
          if (i > bound) free + (i - bound) else free /* adjust to caller's environment */
        case _: source.SEVal => free
        case _: source.SEBuiltin => free
        case _: source.SEValue => free
        case _: source.SEBuiltinRecursiveDefinition => free
        case source.SELocation(_, body) =>
          go(body, bound, free)
        case source.SEAppGeneral(fun, args) =>
          args.foldLeft(go(fun, bound, free))((acc, arg) => go(arg, bound, acc))
        case source.SEAbs(n, body) =>
          go(body, bound + n, free)
        case source.SECase(scrut, alts) =>
          alts.foldLeft(go(scrut, bound, free)) { case (acc, source.SCaseAlt(pat, body)) =>
            val n = pat.numArgs
            go(body, bound + n, acc)
          }
        case source.SELet(bounds, body) =>
          bounds.zipWithIndex.foldLeft(go(body, bound + bounds.length, free)) {
            case (acc, (expr, idx)) => go(expr, bound + idx, acc)
          }
        case source.SELabelClosure(_, expr) =>
          go(expr, bound, free)
        case source.SETryCatch(body, handler) =>
          go(body, bound, go(handler, 1 + bound, free))
        case source.SEScopeExercise(body) =>
          go(body, bound, free)

        case _: source.SELoc | _: source.SEMakeClo | _: source.SEDamlException |
            _: source.SEImportValue | _: source.SELet1General =>
          throw CompilationError(s"freeVars: unexpected $expr")
      }

    go(expr, initiallyBound, Set.empty)
  }

}
