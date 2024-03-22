// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

  import source.SEVarLevel

  private[speedy] def closureConvert(source0: source.SExpr): target.SExpr = {

    final case class Env(
        sourceDepth: Int,
        mapping: Map[SEVarLevel, target.SELoc],
        targetDepth: Int,
    ) {

      def lookup(v: SEVarLevel): target.SELoc = {
        mapping.get(v) match {
          case Some(loc) => loc
          case None => sys.error(s"lookup($v),in:$mapping")
        }
      }

      def extend(n: Int): Env = {
        // Create mappings for `n` new stack items, and combine with the (unshifted!) existing mapping.
        val m2 = (0 until n).view.map { i =>
          val v = SEVarLevel(sourceDepth + i)
          (v, target.SELocAbsoluteS(targetDepth + i))
        }
        Env(sourceDepth + n, mapping ++ m2, targetDepth + n)
      }

      def absBody(arity: Int, freeVars: List[SEVarLevel]): Env = {
        val newRemapsF =
          freeVars.view.zipWithIndex.map { case (v, i) =>
            v -> target.SELocF(i)
          }.toMap
        val newRemapsA = (0 until arity).view.map { case i =>
          val v = SEVarLevel(sourceDepth + i)
          v -> target.SELocA(i)
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
    sealed abstract class Traversal extends Product with Serializable
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
      *   For expression forms with no recursive instance (i.e. SEVarLevel, SEVal), there are
      *   no corresponding continuation forms.
      *
      *   For expression forms with a single recursive instance (i.e. SELocation), there
      *   is a single continuation form: (Cont.Location).
      *
      *   For expression forms with two recursive instances (i.e. SETryCatch), there are
      *   two corresponding continuation forms: (Cont.TryCatch1, Cont.TryCatch2).
      *
      *   For the more complex expression forms containing a list of recursive instances
      *   (i.e. SEApp), the corresponding continuation forms are also more complex,
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
    sealed abstract class Cont extends Product with Serializable
    object Cont {

      final case class Location(loc: Ref.Location) extends Cont

      final case class Abs(arity: Int, freeLocs: List[target.SELoc]) extends Cont

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
          boundsDoneLength: Int,
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

      final case object PreventCatch extends Cont
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
            case v: SEVarLevel =>
              loop(Up(env.lookup(v)), conts)

            case source.SEVal(x) => loop(Up(target.SEVal(x)), conts)
            case source.SEBuiltin(x) => loop(Up(target.SEBuiltin(x)), conts)
            case source.SEValue(x) => loop(Up(target.SEValue(x)), conts)

            case source.SELocation(loc, body) =>
              loop(Down(body, env), Cont.Location(loc) :: conts)

            case source.SEAbs(arity, body) =>
              val freeVars =
                computeFreeVars(body, env.sourceDepth).toList.sortBy(_.level)
              val freeLocs = freeVars.map { v => env.lookup(v) }
              loop(Down(body, env.absBody(arity, freeVars)), Cont.Abs(arity, freeLocs) :: conts)

            case source.SEApp(fun, args) =>
              loop(Down(fun, env), Cont.App1(env, args) :: conts)

            case source.SECase(scrut, alts) =>
              loop(Down(scrut, env), Cont.Case1(env, alts) :: conts)

            case source.SELet(bounds, body) =>
              bounds match {
                case Nil =>
                  loop(Down(body, env), Cont.Let2(Nil) :: conts)
                case bound :: bounds =>
                  loop(Down(bound, env), Cont.Let1(Nil, 0, env, bounds, body) :: conts)
              }

            case source.SETryCatch(body, handler) =>
              loop(Down(body, env), Cont.TryCatch1(env, handler) :: conts)

            case source.SEScopeExercise(body) =>
              loop(Down(body, env), Cont.ScopeExercise :: conts)

            case source.SEPreventCatch(body) =>
              loop(Down(body, env), Cont.PreventCatch :: conts)

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

                case Cont.Abs(arity, freeLocs) =>
                  val body = result
                  loop(Up(target.SEMakeClo(freeLocs, arity, body)), conts)

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

                case Cont.Let1(boundsDone0, n, env0, bounds, body) =>
                  val boundsDone = result :: boundsDone0
                  val env = env0.extend(1)
                  bounds match {
                    case Nil =>
                      loop(Down(body, env), Cont.Let2(boundsDone) :: conts)
                    case bound :: bounds =>
                      loop(
                        Down(bound, env),
                        Cont.Let1(boundsDone, n + 1, env, bounds, body) :: conts,
                      )
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

                case Cont.PreventCatch =>
                  val body = result
                  loop(Up(target.SEPreventCatch(body)), conts)
              }
          }
      }
    }

    /* The (stack-safe) transformation is started here, passing the original source
     * expression (source1), an empty environment, and an empty continuation-stack.
     */
    loop(Down(source0, Env()), Nil)
  }

  /** Compute the free variables of a speedy expression */
  private[this] def computeFreeVars(expr0: source.SExpr, sourceDepth: Int): Set[SEVarLevel] = {
    @tailrec // woo hoo, stack safe!
    def go(acc: Set[SEVarLevel], work: List[source.SExpr]): Set[SEVarLevel] = {
      // 'acc' is the (accumulated) set of free variables we have found so far.
      // 'work' is a list of source expressions which we still have to process.
      work match {
        case Nil => acc // final result
        case expr :: work => {
          expr match {
            case v @ SEVarLevel(level) =>
              if (level < sourceDepth) {
                go(acc + v, work)
              } else {
                go(acc, work)
              }
            case _: source.SEVal => go(acc, work)
            case _: source.SEBuiltin => go(acc, work)
            case _: source.SEValue => go(acc, work)
            case source.SELocation(_, body) => go(acc, body :: work)
            case source.SEApp(fun, args) => go(acc, fun :: args ++ work)
            case source.SEAbs(_, body) => go(acc, body :: work)
            case source.SECase(scrut, alts) =>
              val bodies = alts.map { case source.SCaseAlt(_, body) => body }
              go(acc, scrut :: bodies ++ work)
            case source.SELet(bounds, body) => go(acc, body :: bounds ++ work)
            case source.SELabelClosure(_, expr) => go(acc, expr :: work)
            case source.SETryCatch(body, handler) => go(acc, handler :: body :: work)
            case source.SEScopeExercise(body) => go(acc, body :: work)
            case source.SEPreventCatch(body) => go(acc, body :: work)
          }
        }
      }
    }
    go(Set.empty, List(expr0))
  }

}
