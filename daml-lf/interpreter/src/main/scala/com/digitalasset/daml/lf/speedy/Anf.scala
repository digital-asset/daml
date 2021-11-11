// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  *  expression forms: SEAppGeneral and SECase are removed, and replaced by the simpler
  *  SEAppAtomic and SECaseAtomic (plus SELet as required).
  *
  *  This compilation phase transforms from SExpr0 to SExpr.
  *    SExpr contains only the expression forms which execute on the speedy machine.
  *    SExpr0 contains expression forms which exist during the speedy compilation pipeline.
  *
  *  We use "source." and "t." for lightweight discrimination.
  */

import com.daml.lf.data.Trampoline.{Bounce, Land, Trampoline}
import com.daml.lf.speedy.{SExpr1 => source}
import com.daml.lf.speedy.{SExpr => target}
import com.daml.lf.speedy.Compiler.CompilationError

import scala.annotation.tailrec

private[lf] object Anf {

  /** * Entry point for the ANF transformation phase
    */
  @throws[CompilationError]
  def flattenToAnf(exp: source.SExpr): target.SExpr = {
    flattenToAnfInternal(exp).wrapped
  }

  /** `AExpr` tracks when an expression has been transformed. It is
    * private to this file.
    */
  private final case class AExpr(wrapped: target.SExpr) extends Serializable

  @throws[CompilationError]
  private def flattenToAnfInternal(exp: source.SExpr): AExpr = {
    val depth = DepthA(0)
    val env = initEnv
    flattenExp(depth, env, exp) { anf =>
      Land(anf)
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
    *    variable names "k", "txK" strictly for the "true" continuations that have
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
  private[this] final case class DepthE(n: Int) {
    def incr(m: Int): DepthE = DepthE(n + m)
  }

  /** `DepthA` tracks the stack-depth of the ANF expression being constructed */
  private[this] final case class DepthA(n: Int) {
    def incr(m: Int): DepthA = DepthA(n + m)
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

  /** Tx is the type for the stacked transformation functions managed by the ANF
    * transformation, mainly transformExp.
    *
    * All of the transformation functions would, without CPS, return an AExpr,
    * so that is the input type of the continuation.
    *
    * Both type parameters are ultimately needed because SCaseAlt does not
    * extend SExpr. If it did, T would always be SExpr and A would always be
    * AExpr.
    *
    * Note that Scala does not seem to be able to generate anonymous function of
    * a parameterized type, so we use nested `defs` instead.
    *
    * @tparam T The type of expression this will be applied to.
    * @tparam A The return type of the continuation (minus the Trampoline
    *           wrapping).
    */
  private[this] type Tx[T, A] = (DepthA, T, K[AExpr, A]) => Trampoline[A]

  /** K Is the type for continuations.
    *
    * @tparam T Type the function would have returned had it not been in CPS.
    * @tparam A The return type of the continuation (minus the Trampoline
    *           wrapping).
    */
  private[this] type K[T, A] = T => Trampoline[A]

  /** During conversion we need to deal with bindings which are made/found at a given
    *    absolute stack depth. These are represented using `AbsBinding`. An absolute stack
    *    depth is the offset from the depth of the stack when the function is entered. We call
    *    it absolute because an offset doesn't change as new bindings are pushed onto the
    *    stack.
    *
    *    Note the contrast with the expression form `ELocS` which contains a relative offset
    *    from the end of the stack. This relative-position is used in both the original
    *    expression which we traverse AND the new ANF expression we are constructing. The
    *    relative-offset to a binding varies as new bindings are pushed on the stack.
    */
  private[this] case class AbsBinding(abs: DepthA)

  private[this] def makeAbsoluteB(env: Env, rel: Int): AbsBinding = {
    val oldAbs = env.oldDepth.incr(-rel)
    env.absMap.get(oldAbs) match {
      case None => throw CompilationError(s"makeAbsoluteB(env=$env,rel=$rel)")
      case Some(abs) => AbsBinding(abs)
    }
  }

  private[this] def makeRelativeB(depth: DepthA, binding: AbsBinding): Int = {
    (depth.n - binding.abs.n)
  }

  private[this] type AbsAtom = Either[target.SExprAtomic, AbsBinding]

  private def convertLoc(x: source.SELoc): target.SELoc = {
    x match {
      case source.SELocS(x) => target.SELocS(x)
      case source.SELocA(x) => target.SELocA(x)
      case source.SELocF(x) => target.SELocF(x)
    }
  }

  private def convertAtom(x: source.SExprAtomic): target.SExprAtomic = {
    x match {
      case loc: source.SELoc => convertLoc(loc)
      case source.SEValue(x) => target.SEValue(x)
      case source.SEBuiltin(x) => target.SEBuiltin(x)
      case source.SEBuiltinRecursiveDefinition(x) => target.SEBuiltinRecursiveDefinition(x)
      case _: source.SEVar => sys.error(s"Anf1.convertAtom, unexpected: $x")
    }
  }

  private[this] def makeAbsoluteA(env: Env, atom: source.SExprAtomic): AbsAtom = atom match {
    case source.SELocS(rel) => Right(makeAbsoluteB(env, rel))
    case x => Left(convertAtom(x))
  }

  private[this] def makeRelativeA(depth: DepthA)(atom: AbsAtom): target.SExprAtomic = atom match {
    case Left(x: target.SELocS) => throw CompilationError(s"makeRelativeA: unexpected: $x")
    case Left(atom) => atom
    case Right(binding) => target.SELocS(makeRelativeB(depth, binding))
  }

  private[this] type AbsLoc = Either[source.SELoc, AbsBinding]

  private[this] def makeAbsoluteL(env: Env, loc: source.SELoc): AbsLoc = loc match {
    case source.SELocS(rel) => Right(makeAbsoluteB(env, rel))
    case x: source.SELocA => Left(x)
    case x: source.SELocF => Left(x)
  }

  private[this] def makeRelativeL(depth: DepthA)(loc: AbsLoc): target.SELoc = loc match {
    case Left(x: source.SELocS) => throw CompilationError(s"makeRelativeL: unexpected: $x")
    case Left(loc) => convertLoc(loc)
    case Right(binding) => target.SELocS(makeRelativeB(depth, binding))
  }

  private[this] def flattenExp[A](depth: DepthA, env: Env, exp: source.SExpr)(
      k: K[AExpr, A]
  ): Trampoline[A] = {
    Bounce(() =>
      transformExp[A](depth, env, exp, k) { (_, sexpr, txK) =>
        Bounce(() => txK(AExpr(sexpr)))
      }
    )
  }

  private[this] def transformLet1[A](
      depth: DepthA,
      env: Env,
      rhs: source.SExpr,
      body: source.SExpr,
      k: K[AExpr, A],
      transform: Tx[target.SExpr, A],
  ): Trampoline[A] = {
    Bounce(() =>
      transformExp(depth, env, rhs, k) { (depth, rhs, txK) =>
        val depth1 = depth.incr(1)
        val env1 = trackBindings(depth, env, 1)
        Bounce(() =>
          transformExp(
            depth1,
            env1,
            body,
            { body1 =>
              Bounce(() => txK(AExpr(target.SELet1(rhs, body1.wrapped))))
            },
          )(transform)
        )
      }
    )
  }

  private[this] def flattenAlts[A](depth: DepthA, env: Env, alts: Array[source.SCaseAlt])(
      k: K[Array[target.SCaseAlt], A]
  ): Trampoline[A] = {
    // Note: this could be made properly CPS and thus constant stack through
    // trampoline by implementing a CPS version of map. However, map on an
    // array is implemented as a loop so this should be fine.
    Bounce(() =>
      k(alts.map { case source.SCaseAlt(pat, body0) =>
        val n = patternNArgs(pat)
        val env1 = trackBindings(depth, env, n)
        flattenExp(depth.incr(n), env1, body0)(body => {
          Land(target.SCaseAlt(pat, body.wrapped))
        }).bounce
      })
    )
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
  private[this] def transformExp[A](depth: DepthA, env: Env, exp: source.SExpr, k: K[AExpr, A])(
      transform: Tx[target.SExpr, A]
  ): Trampoline[A] =
    exp match {
      case atom0: source.SExprAtomic =>
        val atom = makeRelativeA(depth)(makeAbsoluteA(env, atom0))
        Bounce(() => transform(depth, atom, k))

      case source.SEVal(x) => Bounce(() => transform(depth, target.SEVal(x), k))
      case source.SEImportValue(ty, v) =>
        Bounce(() => transform(depth, target.SEImportValue(ty, v), k))

      case source.SEAppGeneral(func, args) =>
        // It's safe to perform ANF if the func-expression has no effects when evaluated.
        val safeFunc =
          func match {
            // we know that trivially in these two cases
            case source.SEBuiltin(b) => (args.size <= b.arity)
            case _ => false
          }
        // It's also safe to perform ANF for applications of a single argument.
        if (safeFunc || args.size == 1) {
          transformMultiApp[A](depth, env, func, args, k)(transform)
        } else {
          transformMultiAppSafely[A](depth, env, func, args, k)(transform)
        }

      case source.SEMakeClo(fvs0, arity, body0) =>
        val fvs = fvs0.map((loc) => makeRelativeL(depth)(makeAbsoluteL(env, loc)))
        val body = flattenToAnfInternal(body0).wrapped
        Bounce(() => transform(depth, target.SEMakeClo(fvs, arity, body), k))

      case source.SECase(scrut, alts0) => {
        Bounce(() =>
          atomizeExp(depth, env, scrut, k) { (depth, scrut, txK) =>
            val scrut1 = makeRelativeA(depth)(scrut)
            Bounce(() =>
              flattenAlts(depth, env, alts0) { alts =>
                Bounce(() => transform(depth, target.SECaseAtomic(scrut1, alts), txK))
              }
            )
          }
        )
      }

      case source.SELet(rhss, body) =>
        val expanded = expandMultiLet(rhss, body)
        Bounce(() => transformExp(depth, env, expanded, k)(transform))

      case source.SELet1General(rhs, body) =>
        Bounce(() => transformLet1(depth, env, rhs, body, k, transform))

      case source.SELocation(loc, body) => {
        Bounce(() =>
          transformExp(depth, env, body, k) { (depth, body, txK) =>
            Bounce(() => transform(depth, target.SELocation(loc, body), txK))
          }
        )
      }

      case source.SELabelClosure(label, exp) => {
        Bounce(() =>
          transformExp(depth, env, exp, k) { (depth, exp, txK) =>
            Bounce(() => transform(depth, target.SELabelClosure(label, exp), txK))
          }
        )
      }

      case source.SETryCatch(body0, handler0) =>
        // we must not lift applications from either the body or the handler outside of
        // the try-catch block, so we flatten each separately:
        val body: target.SExpr = flattenExp(depth, env, body0)(anf => Land(anf.wrapped)).bounce
        val handler: target.SExpr =
          flattenExp(depth.incr(1), trackBindings(depth, env, 1), handler0)(anf =>
            Land(anf.wrapped)
          ).bounce
        Bounce(() => transform(depth, target.SETryCatch(body, handler), k))

      case source.SEScopeExercise(body0) =>
        val body: target.SExpr = flattenExp(depth, env, body0)(anf => Land(anf.wrapped)).bounce
        Bounce(() => transform(depth, target.SEScopeExercise(body), k))

      case _: source.SEAbs | _: source.SEDamlException =>
        throw CompilationError(s"flatten: unexpected: $exp")
    }

  private[this] def atomizeExps[A](
      depth: DepthA,
      env: Env,
      exps: List[source.SExpr],
      k: K[AExpr, A],
  )(
      transform: Tx[List[AbsAtom], A]
  ): Trampoline[A] =
    exps match {
      case Nil => Bounce(() => transform(depth, Nil, k))
      case exp :: exps =>
        Bounce(() =>
          atomizeExp(depth, env, exp, k) { (depth, atom, txK1) =>
            Bounce(() =>
              atomizeExps(depth, env, exps, txK1) { (depth, atoms, txK2) =>
                Bounce(() => transform(depth, atom :: atoms, txK2))
              }
            )
          }
        )
    }

  private[this] def atomizeExp[A](depth: DepthA, env: Env, exp: source.SExpr, k: K[AExpr, A])(
      transform: Tx[AbsAtom, A]
  ): Trampoline[A] = {
    exp match {
      case ea: source.SExprAtomic => Bounce(() => transform(depth, makeAbsoluteA(env, ea), k))
      case _ => {
        Bounce(() =>
          transformExp(depth, env, exp, k) { (depth, anf, txK) =>
            val atom = Right(AbsBinding(depth))
            Bounce(() =>
              transform(
                depth.incr(1),
                atom,
                { body =>
                  Bounce(() => txK(AExpr(target.SELet1(anf, body.wrapped))))
                },
              )
            )
          }
        )
      }
    }
  }

  private[this] def expandMultiLet(rhss: List[source.SExpr], body: source.SExpr): source.SExpr = {
    //loop over rhss in reverse order
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
  private[this] def transformMultiApp[A](
      depth: DepthA,
      env: Env,
      func: source.SExpr,
      args: Array[source.SExpr],
      k: K[AExpr, A],
  )(transform: Tx[target.SExpr, A]): Trampoline[A] = {
    Bounce(() =>
      atomizeExp(depth, env, func, k) { (depth, func, txK1) =>
        Bounce(() =>
          atomizeExps(depth, env, args.toList, txK1) { (depth, args, txK) =>
            val func1 = makeRelativeA(depth)(func)
            val args1 = args.map(makeRelativeA(depth))
            Bounce(() => transform(depth, target.SEAppAtomic(func1, args1.toArray), txK))
          }
        )
      }
    )
  }

  /* This function must be used when transforming an application of unknown function.  The
   translated application is *not* in proper ANF form.
   */

  private[this] def transformMultiAppSafely[A](
      depth: DepthA,
      env: Env,
      func: source.SExpr,
      args: Array[source.SExpr],
      k: K[AExpr, A],
  )(transform: Tx[target.SExpr, A]): Trampoline[A] = {

    Bounce(() =>
      atomizeExp(depth, env, func, k) { (depth, func, txK) =>
        val func1 = makeRelativeA(depth)(func)
        // we dont atomize the args here
        val args1 = args.map(arg => flattenExp(depth, env, arg)(anf => Land(anf)).bounce.wrapped)
        Bounce(() =>
          // we build a non-atomic application here (only the function is atomic)
          transform(depth, target.SEAppAtomicFun(func1, args1.toArray), txK)
        )
      }
    )

  }

}
