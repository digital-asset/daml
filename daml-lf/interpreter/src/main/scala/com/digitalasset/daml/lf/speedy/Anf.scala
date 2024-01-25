// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/**  Transformation to ANF based AST for the speedy interpreter.
  *
  *  "ANF" stands for A-normal form.
  *  In essence it means that sub-expressions of most expression nodes are in atomic-form.
  *  The one exception is the let-expression.
  *
  *  Atomic mean: a variable reference (ELoc), a (literal) value, or a builtin.
  *  This is captured by any speedy-expression which `extends SExprAtomic`.
  *
  *  The reason we convert to ANF is to improve the efficiency of speedy execution: the
  *  execution engine can take advantage of the atomic assumption, and often removes
  *  additional execution steps - in particular the pushing of continuations to allow
  *  execution to continue after a compound expression is reduced to a value.
  *
  *  The speedy machine now expects that it will never have to execute a non-ANF expression,
  *  crashing at runtime if one is encountered. In particular we must ensure that the
  *  expression forms: SEApp(General) and SECase are removed, and replaced by the simpler
  *  SEAppAtomic and SECaseAtomic (plus SELet as required).
  *
  *  This compilation phase transforms from SExpr1 to SExpr.
  *    SExpr contains only the expression forms which execute on the speedy machine.
  *    SExpr1 contains expression forms which exist during the speedy compilation pipeline.
  *
  *  We use "source." and "t." for lightweight discrimination.
  */

import com.daml.lf.speedy.{SExpr1 => source}
import com.daml.lf.speedy.{SExpr => target}
import com.daml.lf.speedy.Compiler.CompilationError

import scala.annotation.tailrec
import scala.util.control.TailCalls._
import scalaz.{@@, Tag}

private[lf] object Anf {

  /** Entry point for the ANF transformation phase. */
  @throws[CompilationError]
  def flattenToAnf(exp: source.SExpr): target.SExpr = {
    val depth = DepthA(0)
    val env = initEnv
    flattenExp(depth, env, exp).result
  }

  /** `DepthE` tracks the stack-depth of the original expression being traversed */
  private sealed trait DepthETag

  private type DepthE = Int @@ DepthETag
  private val DepthE = Tag.of[DepthETag]

  private implicit class OpsDepthE[T](val x: DepthE) extends AnyVal {
    def n: Int = Tag.unwrap(x)

    def incr(m: Int): DepthE = DepthE(m + n)
  }

  /** `DepthA` tracks the stack-depth of the ANF expression being constructed */
  private sealed trait DepthATag

  private type DepthA = Int @@ DepthATag
  private val DepthA = Tag.of[DepthATag]

  private implicit class OpsDepthA[T](val x: DepthA) extends AnyVal {
    def n: Int = Tag.unwrap(x)

    def incr(m: Int): DepthA = DepthA(m + n)
  }

  /** `Env` contains the mapping from old to new depth, as well as the old-depth as these
    * components always travel together
    */
  private final case class Env(absMap: Map[DepthE, DepthA], oldDepth: DepthE)

  private val initEnv: Env = Env(absMap = Map.empty, oldDepth = DepthE(0))

  private def trackBindings(depth: DepthA, env: Env, n: Int): Env = {
    val extra = (0 until n).map(i => (env.oldDepth.incr(i), depth.incr(i)))
    Env(absMap = env.absMap ++ extra, oldDepth = env.oldDepth.incr(n))
  }

  private type Res = TailRec[target.SExpr]

  /** Tx is the type for the stacked transformation functions managed by the ANF
    * transformation, mainly transformExp.
    *
    * @tparam T The type of expression this will be applied to.
    */
  private type Tx[T] = (DepthA, T) => Res

  /** During conversion we need to deal with bindings which are made/found at a given
    * absolute stack depth. These are represented using `AbsBinding`. An absolute stack
    * depth is the offset from the depth of the stack when the function is entered. We call
    * it absolute because an offset doesn't change as new bindings are pushed onto the
    * stack.
    *
    * Happily, the source expressions use absolute stack offsets in `SELocAbsoluteS`.
    * In contrast to the target expressions which use relative offsets in `SELocS`. The
    * relative-offset to a binding varies as new bindings are pushed on the stack.
    */
  private case class AbsBinding(abs: DepthA)

  private def makeAbsoluteB(env: Env, abs: Int): AbsBinding = {
    env.absMap.get(DepthE(abs)) match {
      case None => throw CompilationError(s"makeAbsoluteB(env=$env,abs=$abs)")
      case Some(abs) => AbsBinding(abs)
    }
  }

  private def makeRelativeB(depth: DepthA, binding: AbsBinding): Int = {
    (depth.n - binding.abs.n)
  }

  private type AbsAtom = Either[target.SExprAtomic, AbsBinding]

  private def makeAbsoluteA(env: Env, atom: source.SExprAtomic): AbsAtom = atom match {
    case source.SELocAbsoluteS(abs) => Right(makeAbsoluteB(env, abs))
    case source.SELocA(x) => Left(target.SELocA(x))
    case source.SELocF(x) => Left(target.SELocF(x))
    case source.SEValue(x) => Left(target.SEValue(x))
    case source.SEBuiltin(x) => Left(target.SEBuiltin(x))
  }

  private def makeRelativeA(depth: DepthA)(atom: AbsAtom): target.SExprAtomic = atom match {
    case Left(x: target.SELocS) => throw CompilationError(s"makeRelativeA: unexpected: $x")
    case Left(atom) => atom
    case Right(binding) => target.SELocS(makeRelativeB(depth, binding))
  }

  private type AbsLoc = Either[source.SELoc, AbsBinding]

  private def makeAbsoluteL(env: Env, loc: source.SELoc): AbsLoc = loc match {
    case source.SELocAbsoluteS(abs) => Right(makeAbsoluteB(env, abs))
    case x: source.SELocA => Left(x)
    case x: source.SELocF => Left(x)
  }

  private def makeRelativeL(depth: DepthA)(loc: AbsLoc): target.SELoc = loc match {
    case Left(x: source.SELocAbsoluteS) => throw CompilationError(s"makeRelativeL: unexpected: $x")
    case Left(source.SELocA(x)) => target.SELocA(x)
    case Left(source.SELocF(x)) => target.SELocF(x)
    case Right(binding) => target.SELocS(makeRelativeB(depth, binding))
  }

  private def patternNArgs(pat: target.SCasePat): Int = pat match {
    case _: target.SCPEnum | _: target.SCPPrimCon | target.SCPNil | target.SCPDefault |
        target.SCPNone =>
      0
    case _: target.SCPVariant | target.SCPSome => 1
    case target.SCPCons => 2
  }

  private def expandMultiLet(rhss: List[source.SExpr], body: source.SExpr): source.SExpr = {
    // loop over rhss in reverse order
    @tailrec
    def loop(acc: source.SExpr, xs: List[source.SExpr]): source.SExpr = {
      xs match {
        case Nil => acc
        case rhs :: xs => loop(source.SELet1General(rhs, acc), xs)
      }
    }

    loop(body, rhss.reverse)
  }

  /** Monadic map for [[TailRec]]. */
  private def traverse[A, B](
      xs: List[A],
      f: A => TailRec[B],
  ): TailRec[List[B]] = {
    xs match {
      case Nil => done(Nil)
      case x :: xs =>
        for {
          x <- f(x)
          xs <- tailcall {
            traverse(xs, f)
          }
        } yield (x :: xs)
    }
  }

  /**    The transformation code is implemented using a what looks like
    *    continuation-passing style; the code routinely creates new functions to
    *    pass down the stack as it explores the expression tree. This is quite
    *    common for translation to ANF. In general, naming nested compound
    *    expressions requires turning an expression kind of inside out, lifting the
    *    introduced let-expression up to the the nearest enclosing abstraction or
    *    case-branch.
    *
    *    For speedy, the ANF pass occurs after translation to De Brujin and closure
    *    conversions, which adds the additional complication of re-indexing the
    *    variable indexes. This is achieved by tracking the old and new depth & the
    *    mapping between them. See the types: DepthE, DepthA and Env.
    *
    *    Using a coding style that looks like CPS is a natural way to express the
    *    ANF transformation. The transformation is however not tail recursive: it
    *    is only mostly in CPS form, and is actually stack intensive. To address
    *    this, all functions of the core loop return trampolines
    *    (scala.util.control.TailCalls).
    */

  private[this] def flattenExp(depth: DepthA, env: Env, exp: source.SExpr): Res = {
    transformExp(depth, env, exp) { (_, sexpr) => done(sexpr) }
  }

  private[this] def transformLet1(
      depth: DepthA,
      env: Env,
      rhs: source.SExpr,
      body: source.SExpr,
  )(transform: Tx[target.SExpr]): Res = {
    transformExp(depth, env, rhs) { (depth, rhs) =>
      val depth1 = depth.incr(1)
      val env1 = trackBindings(depth, env, 1)
      transformExp(depth1, env1, body)(transform).map { body =>
        target.SELet1(rhs, body)
      }
    }
  }

  private[this] def flattenAlts(
      depth: DepthA,
      env: Env,
      alts0: List[source.SCaseAlt],
  ): TailRec[List[SExpr.SCaseAlt]] = {
    traverse(alts0, (alt: source.SCaseAlt) => flattenAlt(depth, env, alt))
  }

  private[this] def flattenAlt(
      depth: DepthA,
      env: Env,
      alt: source.SCaseAlt,
  ): TailRec[SExpr.SCaseAlt] = {

    alt match {
      case source.SCaseAlt(pat, body) =>
        val n = patternNArgs(pat)
        val env1 = trackBindings(depth, env, n)
        flattenExp(depth.incr(n), env1, body).map { body =>
          target.SCaseAlt(pat, body)
        }
    }
  }

  /** `transformExp` is the function at the heart of the ANF transformation.
    *  You can read its type as saying: "Caller, give me a general expression
    *  `exp`, (& depth/env info), and a transformation function `transform`
    *  which says what you want to do with the transformed expression. Then I
    *  will do the transform, and call `transform` with it. I reserve the right
    *  to wrap further expression-AST around the expression returned by
    *  `transform`.
    *
    *  See: `atomizeExp` for an instance where this wrapping occurs.
    *
    *  Note: this wrapping is the reason why we need a "second" CPS transform to
    *  achieve constant stack through trampoline.
    */
  private[this] def transformExp(depth: DepthA, env: Env, exp: source.SExpr)(
      transform: Tx[target.SExpr]
  ): Res = tailcall {
    exp match {
      case atom0: source.SExprAtomic =>
        val atom = makeRelativeA(depth)(makeAbsoluteA(env, atom0))
        transform(depth, atom)

      case source.SEVal(x) => transform(depth, target.SEVal(x))

      case source.SEApp(func, args) =>
        transformMultiApp(depth, env, func, args)(transform)

      case source.SEMakeClo(fvs0, arity, body) =>
        val fvs = fvs0.map((loc) => makeRelativeL(depth)(makeAbsoluteL(env, loc)))
        flattenExp(DepthA(0), initEnv, body).flatMap { body =>
          transform(depth, target.SEMakeClo(fvs.toArray, arity, body))
        }

      case source.SECase(scrut, alts0) =>
        atomizeExp(depth, env, scrut) { (depth, scrut) =>
          val scrut1 = makeRelativeA(depth)(scrut)
          flattenAlts(depth, env, alts0).flatMap { alts =>
            transform(depth, target.SECaseAtomic(scrut1, alts.toArray))
          }
        }

      case source.SELet(rhss, body) =>
        val expanded = expandMultiLet(rhss, body)
        transformExp(depth, env, expanded)(transform)

      case source.SELet1General(rhs, body) =>
        transformLet1(depth, env, rhs, body)(transform)

      case source.SELocation(loc, body) =>
        transformExp(depth, env, body) { (depth, body) =>
          tailcall {
            transform(depth, target.SELocation(loc, body))
          }
        }

      case source.SELabelClosure(label, exp) =>
        transformExp(depth, env, exp) { (depth, exp) =>
          tailcall {
            transform(depth, target.SELabelClosure(label, exp))
          }
        }

      case source.SETryCatch(body, handler0) =>
        // we must not lift applications from either the body or the handler outside of
        // the try-catch block, so we flatten each separately:
        flattenExp(depth, env, body).flatMap { body =>
          flattenExp(depth.incr(1), trackBindings(depth, env, 1), handler0).flatMap { handler =>
            transform(depth, target.SETryCatch(body, handler))
          }
        }

      case source.SEScopeExercise(body) =>
        flattenExp(depth, env, body).flatMap { body =>
          transform(depth, target.SEScopeExercise(body))
        }

      case source.SEPreventCatch(body) =>
        flattenExp(depth, env, body).flatMap { body =>
          transform(depth, target.SEPreventCatch(body))
        }

    }
  }

  private[this] def atomizeExps(
      depth: DepthA,
      env: Env,
      exps: List[source.SExpr],
  )(transform: Tx[List[AbsAtom]]): Res =
    exps match {
      case Nil => transform(depth, Nil)
      case exp :: exps =>
        atomizeExp(depth, env, exp) { (depth, atom) =>
          tailcall {
            atomizeExps(depth, env, exps) { (depth, atoms) =>
              tailcall {
                transform(depth, atom :: atoms)
              }
            }
          }
        }
    }

  private[this] def atomizeExp(depth: DepthA, env: Env, exp: source.SExpr)(
      transform: Tx[AbsAtom]
  ): Res = {

    exp match {
      case ea: source.SExprAtomic => transform(depth, makeAbsoluteA(env, ea))
      case _ =>
        transformExp(depth, env, exp) { (depth, exp) =>
          val atom = Right(AbsBinding(depth))
          tailcall {
            transform(depth.incr(1), atom).map { body =>
              target.SELet1(exp, body)
            }
          }
        }
    }
  }

  private[this] def transformMultiApp(
      depth: DepthA,
      env: Env,
      func: source.SExpr,
      args: List[source.SExpr],
  )(transform: Tx[target.SExpr]): Res = {
    atomizeExps(depth, env, args.reverse) { (depth, rargs) =>
      atomizeExp(depth, env, func) { (depth, func) =>
        val func1 = makeRelativeA(depth)(func)
        val args1 = rargs.reverse.map(makeRelativeA(depth))
        transform(depth, target.SEAppAtomic(func1, args1.toArray))
      }
    }
  }
}
