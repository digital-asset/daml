// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/** Closure Conversion (Phase of the speedy compiler pipeline)
  *
  * This compilation phase transforms from SExpr0 to SExpr1.
  */

import com.daml.lf.data.Ref

import com.daml.lf.speedy.SExpr.SCasePat
import com.daml.lf.speedy.{SExpr0 => source}
import com.daml.lf.speedy.{SExpr1 => target}

import scala.annotation.tailrec

private[speedy] object ClosureConversion {

  private[speedy] def closureConvert(source0: source.SExpr): target.SExpr = {

    case class Abs(a: Int) // absolute variable index, determined by tracking sourceDepth

    case class Env(sourceDepth: Int, mapping: Map[Abs, target.SELoc], targetDepth: Int) {

      def lookup(abs: Abs): target.SELoc = {
        mapping.get(abs) match {
          case Some(loc) => loc
          case None =>
            throw sys.error(s"lookup($abs),in:$mapping")
        }
      }

      def extend(n: Int): Env = {
        // Create mappings for `n` new stack items, and combine with the (unshifted!) existing mapping.
        val m2 = (0 until n).view.map { i =>
          val abs = Abs(sourceDepth + i)
          (abs, target.SELocAbsoluteS(targetDepth + i))
        }
        Env(sourceDepth + n, mapping ++ m2, targetDepth + n)
      }

      def absBody(arity: Int, fvs: List[Abs]): Env = {
        val newRemapsF: Map[Abs, target.SELoc] = fvs.view.zipWithIndex.map { case (abs, i) =>
          abs -> target.SELocF(i)
        }.toMap
        val newRemapsA = (0 until arity).view.map { case i =>
          val abs = Abs(sourceDepth + i)
          abs -> target.SELocA(i)
        }
        // The keys in newRemapsF and newRemapsA are disjoint
        val m1 = newRemapsF ++ newRemapsA
        // Only targetDepth is reset to 0 in an abstraction body
        Env(sourceDepth + arity, m1, 0)
      }
    }

    object Env {
      def apply(): Env = {
        Env(0, Map.empty, 0)
      }
    }

    /** Closure-conversion, Traversal:
      *
      *   To ensure stack-safety, the input expression is traversed by a single tail-recursive 'loop'.
      *   During the 'Traversal', we are either:
      *   - going 'Down' a source expression (subtree), with an 'Env' for context, or
      *   - coming 'Up' with a target expression (result for a subtree)
      *
      *   In both cases we have a continuation ('List[Cont]') argument which says how to proceed.
      */
    sealed abstract class Traversal
    object Traversal {
      final case class Down(exp: source.SExpr, env: Env) extends Traversal
      final case class Up(exp: target.SExpr) extends Traversal
    }
    import Traversal._

    /** Closure Conversion, Cont:
      *
      *   The multiple forms for a continuation describe how the result of transforming a
      *   sub-expression should be embedded in the continuing traversal. The continuation
      *   forms correspond to the source expression forms: specifically, the location of
      *   recursive expression instances (values of type SExpr).
      *
      *   For expression forms with no recursive instance (i.e. SEVar, SEVal), there are
      *   no corresponding continuation forms.
      *
      *   For expression forms with a single recursive instance (i.e. SELocation), there
      *   is a single continuation form: (Cont.Location).
      *
      *   For expression forms with two recursive instances (i.e. SETryCatch), there are
      *   two corresponding continuation forms: (Cont.TryCatch1, Cont.TryCatch2).
      *
      *   For the more complex expression forms containing a list of recursive instances
      *   (i.e. SEAppGeneral), the corresponding continuation forms are also more complex,
      *   but will generally have two cases (i.e. Cont.App1, Cont.App2), corresponding to
      *   the Nil/Cons cases of the list of recursive instances.
      *
      *   And so on. In effect, 'Cont' is a zipper type for expressions.
      *
      *   Another way to understand the continuation forms is by observing the presence of
      *   an 'env: Env' component indicates more source-expression processing to be done
      *   (generally with the components following the 'env'). Any components before the
      *   'env' (or all components if there is no 'env') represent transform-(sub)-results
      *   which need combining into the final result.
      */
    sealed abstract class Cont
    object Cont {

      final case class Location(loc: Ref.Location) extends Cont

      final case class Abs(arity: Int, fvs: List[target.SELoc]) extends Cont

      final case class App1(env: Env, args: List[source.SExpr]) extends Cont

      final case class App2(
          funDone: target.SExpr,
          argsDone: List[target.SExpr],
          env: Env,
          args: List[source.SExpr],
      ) extends Cont

      final case class Case1(env: Env, alts: List[source.SCaseAlt]) extends Cont

      final case class Case2(
          scrut: target.SExpr,
          altsDone: List[target.SCaseAlt],
          pat: SCasePat,
          env: Env,
          alts: List[source.SCaseAlt],
      ) extends Cont

      final case class Let1(
          boundsDone: List[target.SExpr],
          env: Env,
          bounds: List[source.SExpr],
          body: source.SExpr,
      ) extends Cont

      final case class Let2(
          boundsDone: List[target.SExpr]
      ) extends Cont

      final case class TryCatch1(
          env: Env,
          handler: source.SExpr,
      ) extends Cont

      final case class TryCatch2(
          body: target.SExpr
      ) extends Cont

      final case object ScopeExercise extends Cont

      final case class LabelClosure(label: Profile.Label) extends Cont
    }

    /* The entire traversal in performed by this single tail recursive 'loop' function.
     *
     *   The 'loop' has two arguments:
     *   - The traversal item (Down/Up), and a continuation-stack 'conts'.
     *
     *   The traversal is matched to see if we are going 'Down`, or 'Up.
     *   - When going 'Down', we perform case-analysis on the source-expression being traversed.
     *   - when going 'Up, we perform case-analysis on the continuation-stack.
     *          When the continuation-stack is empty, we are finished.
     */
    @tailrec
    def loop(traversal: Traversal, conts: List[Cont]): target.SExpr = {

      traversal match {

        // Going Down: match on expression form...
        case Down(exp, env) =>
          exp match {
            case source.SEVar(r) =>
              val abs = Abs(env.sourceDepth - r)
              loop(Up(env.lookup(abs)), conts)

            case source.SEVal(x) => loop(Up(target.SEVal(x)), conts)
            case source.SEBuiltin(x) => loop(Up(target.SEBuiltin(x)), conts)
            case source.SEValue(x) => loop(Up(target.SEValue(x)), conts)

            case source.SELocation(loc, body) =>
              loop(Down(body, env), Cont.Location(loc) :: conts)

            case source.SEAbs(arity, body) =>
              val fvsAsListAbs = freeVars(body, arity).toList.sorted.map { r =>
                Abs(env.sourceDepth - r)
              }
              val fvs = fvsAsListAbs.map { abs => env.lookup(abs) }
              loop(Down(body, env.absBody(arity, fvsAsListAbs)), Cont.Abs(arity, fvs) :: conts)

            case source.SEApp(fun, args) =>
              loop(Down(fun, env), Cont.App1(env, args) :: conts)

            case source.SECase(scrut, alts) =>
              loop(Down(scrut, env), Cont.Case1(env, alts) :: conts)

            case source.SELet(bounds, body) =>
              bounds match {
                case Nil =>
                  loop(Down(body, env), Cont.Let2(Nil) :: conts)
                case bound :: bounds =>
                  loop(Down(bound, env), Cont.Let1(Nil, env, bounds, body) :: conts)
              }

            case source.SETryCatch(body, handler) =>
              loop(Down(body, env), Cont.TryCatch1(env, handler) :: conts)

            case source.SEScopeExercise(body) =>
              loop(Down(body, env), Cont.ScopeExercise :: conts)

            case source.SELabelClosure(label, expr) =>
              loop(Down(expr, env), Cont.LabelClosure(label) :: conts)
          }

        // Going Up: match on continuation...
        case Up(result) =>
          conts match {

            case Nil => result // The final result of the tail-recursive 'loop'.

            case cont :: conts =>
              cont match {

                // We rebind the current result (i.e. 'val scrut = result') to help
                // indicate how it is embedded into the target expression being constructed.

                case Cont.Location(loc) =>
                  val body = result
                  loop(Up(target.SELocation(loc, body)), conts)

                case Cont.Abs(arity, fvs) =>
                  val body = result
                  loop(Up(target.SEMakeClo(fvs, arity, body)), conts)

                case Cont.App1(env, args) =>
                  val fun = result
                  args match {
                    case Nil =>
                      loop(Up(target.SEApp(fun, Nil)), conts)
                    case arg :: args =>
                      loop(Down(arg, env), Cont.App2(fun, Nil, env, args) :: conts)
                  }

                case Cont.App2(fun, argsDone0, env, args) =>
                  val argsDone = result :: argsDone0
                  args match {
                    case Nil =>
                      loop(Up(target.SEApp(fun, argsDone.reverse)), conts)
                    case arg :: args =>
                      loop(Down(arg, env), Cont.App2(fun, argsDone, env, args) :: conts)
                  }

                case Cont.Case1(env, alts) =>
                  val scrut = result
                  alts match {
                    case Nil =>
                      loop(Up(target.SECase(scrut, Nil)), conts)
                    case source.SCaseAlt(pat, rhs) :: alts =>
                      val n = pat.numArgs
                      val env1 = env.extend(n)
                      loop(Down(rhs, env1), Cont.Case2(scrut, Nil, pat, env, alts) :: conts)
                  }

                case Cont.Case2(scrut, altsDone0, pat, env, alts) =>
                  val altsDone = target.SCaseAlt(pat, result) :: altsDone0
                  alts match {
                    case Nil =>
                      loop(Up(target.SECase(scrut, altsDone.reverse)), conts)
                    case source.SCaseAlt(pat, rhs) :: alts =>
                      val n = pat.numArgs
                      val env1 = env.extend(n)
                      loop(Down(rhs, env1), Cont.Case2(scrut, altsDone, pat, env, alts) :: conts)
                  }

                case Cont.Let1(boundsDone0, env, bounds, body) =>
                  val boundsDone = result :: boundsDone0
                  val n = boundsDone.length
                  val env1 = env.extend(n)
                  bounds match {
                    case Nil =>
                      loop(Down(body, env1), Cont.Let2(boundsDone) :: conts)
                    case bound :: bounds =>
                      loop(Down(bound, env1), Cont.Let1(boundsDone, env, bounds, body) :: conts)
                  }

                case Cont.Let2(boundsDone) =>
                  val body = result
                  loop(Up(target.SELet(boundsDone.reverse, body)), conts)

                case Cont.TryCatch1(env, handler) =>
                  val body = result
                  loop(Down(handler, env.extend(1)), Cont.TryCatch2(body) :: conts)

                case Cont.TryCatch2(body) =>
                  val handler = result
                  loop(Up(target.SETryCatch(body, handler)), conts)

                case Cont.ScopeExercise =>
                  val body = result
                  loop(Up(target.SEScopeExercise(body)), conts)

                case Cont.LabelClosure(label) =>
                  val expr = result
                  loop(Up(target.SELabelClosure(label, expr)), conts)
              }
          }
      }
    }

    /* The (stack-safe) transformation is started here, passing the original source
     * expression (source1), an empty environment, and an empty continuation-stack.
     */
    loop(Down(source0, Env()), Nil)
  }

  // TODO: Recode to avoid polynomial-complexity of 'freeVars' computation. Issue #11830

  /** Compute the free variables in a speedy expression.
    * The returned free variables are de bruijn indices
    * adjusted to the stack of the caller.
    */
  private[this] def freeVars(expr: source.SExpr, initiallyBound: Int): Set[Int] = {
    // @tailrec // TODO: This implementation is not stack-safe. Issue #11830
    def go(expr: source.SExpr, bound: Int, free: Set[Int]): Set[Int] =
      expr match {
        case source.SEVar(i) =>
          if (i > bound) {
            val rel = (i - bound) /* adjust to caller's environment */
            free + rel
          } else {
            free
          }
        case _: source.SEVal => free
        case _: source.SEBuiltin => free
        case _: source.SEValue => free
        case source.SELocation(_, body) =>
          go(body, bound, free)
        case source.SEApp(fun, args) =>
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
      }

    go(expr, initiallyBound, Set.empty)
  }

}
