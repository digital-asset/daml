// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

import com.daml.lf.data.Trampoline.{Bounce, Land, Trampoline}
import com.daml.lf.speedy.{SExpr1 => source}
import com.daml.lf.speedy.{SExpr => target}
import com.daml.lf.speedy.Compiler.CompilationError

import scala.annotation.nowarn
import scala.annotation.tailrec

import scalaz.{@@, Tag}

private[lf] object Anf {

  /** * Entry point for the ANF transformation phase
    */
  @throws[CompilationError]
  def flattenToAnf(exp: source.SExpr): target.SExpr = {
    flattenToAnfInternal(exp)
  }

  @throws[CompilationError]
  private def flattenToAnfInternal(exp: source.SExpr): target.SExpr = {
    val depth = DepthA(0)
    val env = initEnv
    flattenExp(depth, env, exp) { exp =>
      Land(exp)
    }.bounce
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
    *    ANF transformation. However, this means the transformation is very
    *    stack-intensive. To address that, we need the code to be in "true" CPS
    *    form, which is not quite the same as the semantics required by the ANF
    *    transform. Having the code in ANF form allows us to use the trampoline
    *    technique to execute the computation in constant stack space.
    *
    *    This means that, in some sense, the following code has two (interleaved)
    *    levels of CPS-looking style. For the sake of clarity, in further comments
    *    as well as in the code, we will use the term "continuation" and the
    *    variable names "k", strictly for the "true" continuations that have
    *    been added to achieve constant stack space, and use the term
    *    "transformation function" and the variable names "transform", "tx" for
    *    the functions that express the semantics of the ANF transformation.
    *
    *    Things are further muddied by the following:
    *    1. A number of top-level functions defined in this object also qualify as
    *       "transformation functions", even though they themselves receive
    *       transformation functions as arguments and/or define new ones on the fly
    *       (flattenExp, transformLet1, flattenAlts, transformExp, atomizeExp,
    *       atomizeExps).
    *    2. To achieve full CPS, transformation functions themselves need to accept
    *       (and apply) a continuation.
    *
    *    Not all functions in this object are in CPS (only the ones that are part of
    *    the main recursive loop), but those that do always take the continuation as
    *    their last argument.
    */

  /** `DepthE` tracks the stack-depth of the original expression being traversed */
  private[this] sealed trait DepthETag
  private[this] type DepthE = Int @@ DepthETag
  private[this] val DepthE = Tag.of[DepthETag]
  private[this] implicit class OpsDepthE[T](val x: DepthE) extends AnyVal {
    def n: Int = Tag.unwrap(x)
    def incr(m: Int): DepthE = DepthE(m + n)
  }

  /** `DepthA` tracks the stack-depth of the ANF expression being constructed */
  private[this] sealed trait DepthATag
  private[this] type DepthA = Int @@ DepthATag
  private[this] val DepthA = Tag.of[DepthATag]
  private[this] implicit class OpsDepthA[T](val x: DepthA) extends AnyVal {
    def n: Int = Tag.unwrap(x)
    def incr(m: Int): DepthA = DepthA(m + n)
  }

  /** `Env` contains the mapping from old to new depth, as well as the old-depth as these
    * components always travel together
    */
  private[this] final case class Env(absMap: Map[DepthE, DepthA], oldDepth: DepthE)

  private[this] val initEnv: Env = Env(absMap = Map.empty, oldDepth = DepthE(0))

  private[this] def trackBindings(depth: DepthA, env: Env, n: Int): Env = {
    val extra = (0 until n).map(i => (env.oldDepth.incr(i), depth.incr(i)))
    Env(absMap = env.absMap ++ extra, oldDepth = env.oldDepth.incr(n))
  }

  private[this] type Res = Trampoline[target.SExpr]

  /** Tx is the type for the stacked transformation functions managed by the ANF
    * transformation, mainly transformExp.
    *
    * All of the transformation functions would, without CPS, return a target.SExpr,
    * so that is the input type of the continuation.
    *
    * Both type parameters are ultimately needed because SCaseAlt does not
    * extend source.SExpr. If it did, T would always be source.SExpr and A would always be
    * target.SExpr.
    *
    * Note that Scala does not seem to be able to generate anonymous function of
    * a parameterized type, so we use nested `defs` instead.
    *
    * @tparam T The type of expression this will be applied to.
    */
  private[this] type Tx[T] = (DepthA, T) => K[target.SExpr] => Res

  /** K Is the type for continuations.
    *
    * @tparam T Type the function would have returned had it not been in CPS.
    */
  private[this] type K[T] = T => Res

  /** During conversion we need to deal with bindings which are made/found at a given
    *    absolute stack depth. These are represented using `AbsBinding`. An absolute stack
    *    depth is the offset from the depth of the stack when the function is entered. We call
    *    it absolute because an offset doesn't change as new bindings are pushed onto the
    *    stack.
    *
    *    Happily, the source expressions use absolute stack offsets in `SELocAbsoluteS`.
    *    In contrast to the target expressions which use relative offsets in `SELocS`. The
    *    relative-offset to a binding varies as new bindings are pushed on the stack.
    */
  private[this] case class AbsBinding(abs: DepthA)

  private[this] def makeAbsoluteB(env: Env, abs: Int): AbsBinding = {
    env.absMap.get(DepthE(abs)) match {
      case None => throw CompilationError(s"makeAbsoluteB(env=$env,abs=$abs)")
      case Some(abs) => AbsBinding(abs)
    }
  }

  private[this] def makeRelativeB(depth: DepthA, binding: AbsBinding): Int = {
    (depth.n - binding.abs.n)
  }

  private[this] type AbsAtom = Either[target.SExprAtomic, AbsBinding]

  private[this] def makeAbsoluteA(env: Env, atom: source.SExprAtomic): AbsAtom = atom match {
    case source.SELocAbsoluteS(abs) => Right(makeAbsoluteB(env, abs))
    case source.SELocA(x) => Left(target.SELocA(x))
    case source.SELocF(x) => Left(target.SELocF(x))
    case source.SEValue(x) => Left(target.SEValue(x))
    case source.SEBuiltin(x) => Left(target.SEBuiltin(x))
  }

  private[this] def makeRelativeA(depth: DepthA)(atom: AbsAtom): target.SExprAtomic = atom match {
    case Left(x: target.SELocS) => throw CompilationError(s"makeRelativeA: unexpected: $x")
    case Left(atom) => atom
    case Right(binding) => target.SELocS(makeRelativeB(depth, binding))
  }

  private[this] type AbsLoc = Either[source.SELoc, AbsBinding]

  private[this] def makeAbsoluteL(env: Env, loc: source.SELoc): AbsLoc = loc match {
    case source.SELocAbsoluteS(abs) => Right(makeAbsoluteB(env, abs))
    case x: source.SELocA => Left(x)
    case x: source.SELocF => Left(x)
  }

  private[this] def makeRelativeL(depth: DepthA)(loc: AbsLoc): target.SELoc = loc match {
    case Left(x: source.SELocAbsoluteS) => throw CompilationError(s"makeRelativeL: unexpected: $x")
    case Left(source.SELocA(x)) => target.SELocA(x)
    case Left(source.SELocF(x)) => target.SELocF(x)
    case Right(binding) => target.SELocS(makeRelativeB(depth, binding))
  }

  private[this] def flattenExp(depth: DepthA, env: Env, exp: source.SExpr)(
      k: K[target.SExpr]
  ): Res = {

    transformExp(depth, env, exp)(k) { (_, sexpr) => k =>
      Bounce { () =>
        k(sexpr)
      }
    }
  }

  private[this] def transformLet1(
      depth: DepthA,
      env: Env,
      rhs: source.SExpr,
      body: source.SExpr,
  )(k: K[target.SExpr])(transform: Tx[target.SExpr]): Res = {

    transformExp(depth, env, rhs)(k) { (depth, rhs) => k =>
      val depth1 = depth.incr(1)
      val env1 = trackBindings(depth, env, 1)
      transformExp(depth1, env1, body) { body =>
        Bounce { () =>
          k(target.SELet1(rhs, body))
        }
      }(transform)
    }
  }

  private[this] def flattenAlts(depth: DepthA, env: Env, alts0: List[source.SCaseAlt])(
      k: K[List[target.SCaseAlt]]
  ): Res = {

    def loop(acc: List[target.SCaseAlt], alts: List[source.SCaseAlt]): Res = {
      alts match {
        case alt :: alts =>
          flattenAlt(depth, env, alt) { alt =>
            loop(alt :: acc, alts)
          }
        case Nil =>
          k(acc.reverse)
      }
    }
    loop(Nil, alts0)
  }

  private[this] def flattenAlt(depth: DepthA, env: Env, alt: source.SCaseAlt)(
      k: K[target.SCaseAlt]
  ): Res = {

    alt match {
      case source.SCaseAlt(pat, body) =>
        val n = patternNArgs(pat)
        val env1 = trackBindings(depth, env, n)
        flattenExp(depth.incr(n), env1, body) { body =>
          k(target.SCaseAlt(pat, body))
        }
    }
  }

  private[this] def patternNArgs(pat: target.SCasePat): Int = pat match {
    case _: target.SCPEnum | _: target.SCPPrimCon | target.SCPNil | target.SCPDefault |
        target.SCPNone =>
      0
    case _: target.SCPVariant | target.SCPSome => 1
    case target.SCPCons => 2
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
      k: K[target.SExpr]
  )(transform: Tx[target.SExpr]): Res = Bounce { () =>
    exp match {
      case atom0: source.SExprAtomic =>
        val atom = makeRelativeA(depth)(makeAbsoluteA(env, atom0))
        transform(depth, atom)(k)

      case source.SEVal(x) => transform(depth, target.SEVal(x))(k)

      case source.SEApp(func, args) =>
        // It's safe to perform ANF if the func-expression has no effects when evaluated.
        val safeFunc =
          func match {
            // we know that trivially in these two cases
            case source.SEBuiltin(b) =>
              val overApp = args.lengthCompare(b.arity) > 0
              !overApp
            case _ => false
          }
        // It's also safe to perform ANF for applications of a single argument.
        val singleArg = args.lengthCompare(1) == 0
        if (safeFunc || singleArg) {
          transformMultiApp(depth, env, func, args, k)(transform)
        } else {
          transformMultiAppSafely(depth, env, func, args, k)(transform)
        }

      case source.SEMakeClo(fvs0, arity, body) =>
        val fvs = fvs0.map((loc) => makeRelativeL(depth)(makeAbsoluteL(env, loc)))
        flattenExp(DepthA(0), initEnv, body) { body =>
          transform(depth, target.SEMakeClo(fvs.toArray, arity, body))(k)
        }

      case source.SECase(scrut, alts0) =>
        atomizeExp(depth, env, scrut, k) { (depth, scrut) => k =>
          val scrut1 = makeRelativeA(depth)(scrut)
          flattenAlts(depth, env, alts0) { alts =>
            transform(depth, target.SECaseAtomic(scrut1, alts.toArray))(k)
          }
        }

      case source.SELet(rhss, body) =>
        val expanded = expandMultiLet(rhss, body)
        transformExp(depth, env, expanded)(k)(transform)

      case source.SELet1General(rhs, body) =>
        transformLet1(depth, env, rhs, body)(k)(transform)

      case source.SELocation(loc, body) =>
        transformExp(depth, env, body)(k) { (depth, body) => k =>
          Bounce { () =>
            transform(depth, target.SELocation(loc, body))(k)
          }
        }

      case source.SELabelClosure(label, exp) =>
        transformExp(depth, env, exp)(k) { (depth, exp) => k =>
          Bounce { () =>
            transform(depth, target.SELabelClosure(label, exp))(k)
          }
        }

      case source.SETryCatch(body, handler0) =>
        // we must not lift applications from either the body or the handler outside of
        // the try-catch block, so we flatten each separately:
        flattenExp(depth, env, body) { body =>
          flattenExp(depth.incr(1), trackBindings(depth, env, 1), handler0) { handler =>
            transform(depth, target.SETryCatch(body, handler))(k)
          }
        }

      case source.SEScopeExercise(body) =>
        flattenExp(depth, env, body) { body =>
          transform(depth, target.SEScopeExercise(body))(k)
        }

      case source.SEPreventCatch(body) =>
        flattenExp(depth, env, body) { body =>
          transform(depth, target.SEPreventCatch(body))(k)
        }

    }
  }

  private[this] def atomizeExps(
      depth: DepthA,
      env: Env,
      exps: List[source.SExpr],
      k: K[target.SExpr],
  )(transform: Tx[List[AbsAtom]]): Res =
    exps match {
      case Nil => transform(depth, Nil)(k)
      case exp :: exps =>
        atomizeExp(depth, env, exp, k) { (depth, atom) => k =>
          Bounce { () =>
            atomizeExps(depth, env, exps, k) { (depth, atoms) => k =>
              Bounce { () =>
                transform(depth, atom :: atoms)(k)
              }
            }
          }
        }
    }

  private[this] def atomizeExp(depth: DepthA, env: Env, exp: source.SExpr, k: K[target.SExpr])(
      transform: Tx[AbsAtom]
  ): Res = {

    exp match {
      case ea: source.SExprAtomic => transform(depth, makeAbsoluteA(env, ea))(k)
      case _ => {
        transformExp(depth, env, exp)(k) { (depth, exp) => k =>
          val atom = Right(AbsBinding(depth))
          Bounce { () =>
            transform(depth.incr(1), atom) { body =>
              Bounce { () =>
                k(target.SELet1(exp, body))
              }
            }
          }
        }
      }
    }
  }

  private[this] def expandMultiLet(rhss: List[source.SExpr], body: source.SExpr): source.SExpr = {
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

  /* This function is used when transforming known functions.  And so we can we sure that
   the ANF transform is safe, and will not change the evaluation order
   */
  private[this] def transformMultiApp(
      depth: DepthA,
      env: Env,
      func: source.SExpr,
      args: List[source.SExpr],
      k: K[target.SExpr],
  )(transform: Tx[target.SExpr]): Res = {

    atomizeExp(depth, env, func, k) { (depth, func) => k =>
      atomizeExps(depth, env, args, k) { (depth, args) => k =>
        val func1 = makeRelativeA(depth)(func)
        val args1 = args.map(makeRelativeA(depth))
        transform(depth, target.SEAppAtomic(func1, args1.toArray))(k)
      }
    }
  }

  /* This function must be used when transforming an application of unknown function.  The
   translated application is *not* in proper ANF form.
   */

  @nowarn("cat=deprecation&origin=com.daml.lf.speedy.SExpr.SEAppOnlyFunIsAtomic")
  private[this] def transformMultiAppSafely(
      depth: DepthA,
      env: Env,
      func: source.SExpr,
      args: List[source.SExpr],
      k: K[target.SExpr],
  )(transform: Tx[target.SExpr]): Res = {

    atomizeExp(depth, env, func, k) { (depth, func) => k =>
      val func1 = makeRelativeA(depth)(func)
      // we dont atomize the args here
      flattenExpList(depth, env, args) { args =>
        // we build a non-atomic application here (only the function is atomic)
        transform(depth, target.SEAppOnlyFunIsAtomic(func1, args.toArray))(k)
      }
    }
  }

  private[this] def flattenExpList(depth: DepthA, env: Env, exps0: List[source.SExpr])(
      k: K[List[target.SExpr]]
  ): Res = {

    def loop(acc: List[target.SExpr], exps: List[source.SExpr]): Res = {
      exps match {
        case exp :: exps =>
          flattenExp(depth, env, exp) { exp =>
            loop(exp :: acc, exps)
          }
        case Nil =>
          k(acc.reverse)
      }
    }
    loop(Nil, exps0)
  }

}
