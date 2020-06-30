// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

/**
  Transformation to ANF based AST for the speedy interpreter.

  "ANF" stands for A-normal form.
  In essence it means that sub-expressions of most expression nodes are in atomic-form.
  The one exception is the let-expression.

  Atomic mean: a variable reference (ELoc), a (literal) value, or a builtin.
  This is captured by any speedy-expression which `extends SExprAtomic`.

  TODO: <EXAMPLE HERE>

  The reason we convert to ANF is to improve the efficiency of speedy execution: the
  execution engine can take advantage of the atomic assumption, and often removes
  additional execution steps - in particular the pushing of continuations to allow
  execution to continue after a compound expression is reduced to a value.

  The speedy machine now expects that it will never have to execute a non-ANF expression,
  crashing at runtime if one is encountered.  In particular we must ensure that the
  expression forms: SEAppGeneral and SECase are removed, and replaced by the simpler
  SEAppAtomic and SECaseAtomic (plus SELet as required).

  */
import com.daml.lf.speedy.SExpr._

import scala.annotation.tailrec

object Anf {

  /*** Entry point for the ANF transformation phase */
  def flattenToAnf(exp: SExpr): AExpr = {
    val depth = DepthA(0)
    val env = initEnv
    flattenExp(depth, env, exp, flattenedExpression => Land(flattenedExpression)).bounce
  }

  /**
    The transformation code is implemented using a continuation-passing style of
    translation (which is quite common for translation to ANF). In general, naming nested
    compound expressions requires turning an expression kind of inside out, lifting the
    introduced let-expression up to the the nearest enclosing abstraction or case-branch.

    For speedy, the ANF pass occurs after translation to De Brujin and closure conversions,
    which adds the additional complication of re-indexing the variable indexes. This is
    achieved by tracking the old and new depth & the mapping between them. See the types:
    DepthE, DepthA and Env.

    There is also the issue of avoiding stack-overflow during compilation, which is
    managed by the using of a Trampoline[T] type.
    */
  case class CompilationError(error: String) extends RuntimeException(error)

  /** `DepthE` tracks the stack-depth of the original expression being traversed */
  case class DepthE(n: Int) {
    def incr(m: Int) = DepthE(n + m)
  }

  /** `DepthA` tracks the stack-depth of the ANF expression being constructed */
  case class DepthA(n: Int) {
    def incr(m: Int) = DepthA(n + m)
  }

  /** `Env` contains the mapping from old to new depth, as well as the old-depth as these
    * components always travel together */
  case class Env(absMap: Map[DepthE, DepthA], oldDepth: DepthE)

  val initEnv = Env(absMap = Map.empty, oldDepth = DepthE(0))

  def trackBindings(depth: DepthA, env: Env, n: Int): Env = {
    val extra = (0 to n - 1).map(i => (env.oldDepth.incr(i), depth.incr(i)))
    Env(absMap = env.absMap ++ extra, oldDepth = env.oldDepth.incr(n))
  }

  // TODO: reference something here about trampolines
  sealed abstract class Trampoline[T] {
    @tailrec
    final def bounce: T = this match {
      case Land(x) => x
      case Bounce(continue) => continue().bounce
    }
  }

  final case class Land[T](x: T) extends Trampoline[T]
  final case class Bounce[T](continue: () => Trampoline[T]) extends Trampoline[T]

  /** `Res` is the final, fully transformed ANF expression, returned by the continuations. */
  type Res = Trampoline[AExpr]

  /** `K[T]` is the continuation type which must be passed to the core transformation
    functions, i,e, `transformExp`.

    Notice how the DepthA is threaded through the continuation.
    */
  type K[T] = ((DepthA, T) => Res)

  /** During conversion we need to deal with bindings which are made/found at a given
    absolute stack depth. These are represented using `AbsBinding`. An absolute stack
    depth is the offset from the depth of the stack when the function is entered. We call
    it absolute because an offset doesn't change as new bindings are pushed onto the
    stack.

    Note the contrast with the expression form `ELocS` which contains a relative offset
    from the end of the stack. This relative-position is used in both the original
    expression which we traverse AND the new ANF expression we are constructing. The
    relative-offset to a binding varies as new bindings are pushed on the stack.
    */
  case class AbsBinding(abs: DepthA)

  def makeAbsoluteB(env: Env, rel: Int): AbsBinding = {
    val oldAbs = env.oldDepth.incr(-rel)
    env.absMap.get(oldAbs) match {
      case None => throw CompilationError(s"makeAbsoluteB(env=$env,rel=$rel)")
      case Some(abs) => AbsBinding(abs)
    }
  }

  def makeRelativeB(depth: DepthA, binding: AbsBinding): Int = {
    (depth.n - binding.abs.n)
  }

  type AbsAtom = Either[SExprAtomic, AbsBinding]

  def makeAbsoluteA(env: Env, atom: SExprAtomic): AbsAtom = atom match {
    case SELocS(rel) => Right(makeAbsoluteB(env, rel))
    case x => Left(x)
  }

  def makeRelativeA(depth: DepthA)(atom: AbsAtom): SExprAtomic = atom match {
    case Left(x: SELocS) => throw CompilationError(s"makeRelativeA: unexpected: $x")
    case Left(atom) => atom
    case Right(binding) => SELocS(makeRelativeB(depth, binding))
  }

  type AbsLoc = Either[SELoc, AbsBinding]

  def makeAbsoluteL(env: Env, loc: SELoc): AbsLoc = loc match {
    case SELocS(rel) => Right(makeAbsoluteB(env, rel))
    case x: SELocA => Left(x)
    case x: SELocF => Left(x)
  }

  def makeRelativeL(depth: DepthA)(loc: AbsLoc): SELoc = loc match {
    case Left(x: SELocS) => throw CompilationError(s"makeRelativeL: unexpected: $x")
    case Left(loc) => loc
    case Right(binding) => SELocS(makeRelativeB(depth, binding))
  }

  def flattenExp(depth: DepthA, env: Env, exp: SExpr, k: (AExpr => Trampoline[AExpr])): Res = {
    Bounce(() => k(transformExp(depth, env, exp, { case (_, sexpr) => Land(AExpr(sexpr)) }).bounce))
  }

  def transformLet1(depth: DepthA, env: Env, rhs: SExpr, body: SExpr, k: K[SExpr]): Res = {
    flattenExp(
      depth,
      env,
      rhs, { rhs1 =>
        val depth1 = depth.incr(1)
        val env1 = trackBindings(depth, env, 1)
        flattenExp(depth1, env1, body, { body1 =>
          k(depth, SELet1(rhs1.wrapped, body1.wrapped))
        })
      }
    )
  }

  def flattenAlts(depth: DepthA, env: Env, alts: Array[SCaseAlt]): Array[SCaseAlt] = {
    alts.map {
      case SCaseAlt(pat, body0) =>
        val n = patternNArgs(pat)
        val env1 = trackBindings(depth, env, n)
        SCaseAlt(pat, flattenExp(depth.incr(n), env1, body0, body => {
          Land(body)
        }).bounce.wrapped)
    }
  }

  def patternNArgs(pat: SCasePat): Int = pat match {
    case _: SCPEnum | _: SCPPrimCon | SCPNil | SCPDefault | SCPNone => 0
    case _: SCPVariant | SCPSome => 1
    case SCPCons => 2
  }

  /** `transformExp` is the function at the heart of the ANF transformation.  You can read
    it's type as saying: "Caller, give me a general expression `exp`, (& depth/env info),
    and a continuation function `k` which says what you want to do with the transformed
    expression. Then I will do the transform, and call `k` with it. I reserve the right to
    wrap further expression-AST around the expression returned by `k`.
    See: `atomizeExp` for a instance where this wrapping occurs.
    */
  def transformExp(depth: DepthA, env: Env, exp: SExpr, k: K[SExpr]): Res =
    Bounce(() =>
      exp match {
        case atom0: SExprAtomic =>
          val atom = makeRelativeA(depth)(makeAbsoluteA(env, atom0))
          k(depth, atom)

        case x: SEVal => k(depth, x)
        case x: SEImportValue => k(depth, x)

        case SEAppGeneral(func, args) =>
          atomizeExp(
            depth,
            env,
            func, {
              case (depth, func) =>
                atomizeExps(
                  depth,
                  env,
                  args.toList, {
                    case (depth, args) =>
                      val func1 = makeRelativeA(depth)(func)
                      val args1 = args.map(makeRelativeA(depth))
                      k(depth, SEAppAtomic(func1, args1.toArray))
                  }
                )
            }
          )
        case SEMakeClo(fvs0, arity, body0) =>
          val fvs = fvs0.map((loc) => makeRelativeL(depth)(makeAbsoluteL(env, loc)))
          val body = flattenToAnf(body0).wrapped
          k(depth, SEMakeClo(fvs, arity, body))

        case SECase(scrut, alts0) =>
          atomizeExp(depth, env, scrut, {
            case (depth, scrut) =>
              val scrut1 = makeRelativeA(depth)(scrut)
              val alts = flattenAlts(depth, env, alts0)
              k(depth, SECaseAtomic(scrut1, alts))
          })

        case SELet(rhss, body) =>
          val expanded = expandMultiLet(rhss.toList, body)
          transformExp(depth, env, expanded, k)

        case SELet1General(rhs, body) =>
          transformLet1(depth, env, rhs, body, k)

        case SECatch(body0, handler0, fin0) =>
          flattenExp(
            depth,
            env,
            body0,
            body => {
              flattenExp(depth, env, handler0, handler => {
                flattenExp(depth, env, fin0, fin => {
                  k(depth, SECatch(body.wrapped, handler.wrapped, fin.wrapped))
                })
              })
            }
          )

        case SELocation(loc, body) =>
          transformExp(depth, env, body, {
            case (depth, body) =>
              k(depth, SELocation(loc, body))
          })

        case SELabelClosure(label, exp) =>
          transformExp(depth, env, exp, {
            case (depth, exp) =>
              k(depth, SELabelClosure(label, exp))
          })

        case x: SEAbs => throw CompilationError(s"flatten: unexpected: $x")
        case x: SEWronglyTypeContractId => throw CompilationError(s"flatten: unexpected: $x")
        case x: SEVar => throw CompilationError(s"flatten: unexpected: $x")

        case x: SEAppAtomicGeneral => throw CompilationError(s"flatten: unexpected: $x")
        case x: SEAppAtomicSaturatedBuiltin => throw CompilationError(s"flatten: unexpected: $x")
        case x: SELet1Builtin => throw CompilationError(s"flatten: unexpected: $x")
        case x: SECaseAtomic => throw CompilationError(s"flatten: unexpected: $x")

    })

  def atomizeExps(depth: DepthA, env: Env, exps: List[SExpr], k: K[List[AbsAtom]]): Res =
    exps match {
      case Nil => k(depth, Nil)
      case exp :: exps =>
        Bounce(() =>
          atomizeExp(depth, env, exp, {
            case (depth, atom) =>
              atomizeExps(depth, env, exps, {
                case (depth, atoms) =>
                  Bounce(() => k(depth, atom :: atoms))
              })
          }))
    }

  def atomizeExp(depth: DepthA, env: Env, exp: SExpr, k: K[AbsAtom]): Res = {
    exp match {
      case ea: SExprAtomic => k(depth, makeAbsoluteA(env, ea))
      case _ =>
        transformExp(
          depth,
          env,
          exp, {
            case (depth, anf) =>
              val atom = Right(AbsBinding(depth))
              // Here we call `k' with a newly introduced variable:
              val body = k(depth.incr(1), atom).bounce.wrapped
              // Here we wrap the result of `k` with an enclosing let expression:
              Land(AExpr(SELet1(anf, body)))
          }
        )
    }
  }

  def expandMultiLet(rhss: List[SExpr], body: SExpr): SExpr = {
    //loop over rhss in reverse order
    @tailrec
    def loop(acc: SExpr, xs: List[SExpr]): SExpr = {
      xs match {
        case Nil => acc
        case rhs :: xs => loop(SELet1General(rhs, acc), xs)
      }
    }
    loop(body, rhss.reverse)
  }

}
