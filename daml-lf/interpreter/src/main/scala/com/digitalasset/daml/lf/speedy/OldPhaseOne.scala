// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// TODO https://github.com/digital-asset/daml/issues/11561
// - Remove this OLD version of the phase one compiler... only retained for side-by-side diff comparison with NEW version

package com.daml.lf
package speedy

import java.util

import com.daml.lf.data.Ref._
import com.daml.lf.data.{ImmArray, Numeric, Struct}
import com.daml.lf.language.Ast._
import com.daml.lf.language.{LookupError, PackageInterface}
import com.daml.lf.speedy.Compiler.{ProfilingMode, StackTraceMode, CompilationError}
import com.daml.lf.speedy.SBuiltin._
import com.daml.lf.speedy.SValue._
import com.daml.lf.speedy.{SExpr => t}
import com.daml.lf.speedy.{SExpr0 => s}
import com.daml.nameof.NameOf

import scala.annotation.tailrec
import scala.reflect.ClassTag

private[speedy] object OldPhaseOne {

  case class Config(
      profiling: ProfilingMode,
      stacktracing: StackTraceMode,
  )

  private val SEGetTime = s.SEBuiltin(SBGetTime)

  private val SBEToTextNumeric = s.SEAbs(1, s.SEBuiltin(SBToText))

  private val SENat: Numeric.Scale => Some[s.SEValue] =
    Numeric.Scale.values.map(n => Some(s.SEValue(STNat(n))))

  // Hand-implemented `map` uses less stack.
  private def mapToArray[A, B: ClassTag](input: ImmArray[A])(f: A => B): List[B] = {
    val output = Array.ofDim[B](input.length)
    var i = 0
    input.foreach { value =>
      output(i) = f(value)
      i += 1
    }
    output.toList
  }

  /* share Env with (new) PhaseOne
  private[speedy] abstract class VarRef { def name: Name }
  // corresponds to Daml-LF expression variable.
  private[this] case class EVarRef(name: ExprVarName) extends VarRef
  // corresponds to Daml-LF type variable.
  private[this] case class TVarRef(name: TypeVarName) extends VarRef

  case class Position(idx: Int)

  private[speedy] object Env {
    val Empty = Env(0, Map.empty)
  }

  private[speedy] case class Env(
      position: Int,
      varIndices: Map[VarRef, Position],
  ) {

    def toSEVar(p: Position) = s.SEVarLevel(p.idx)

    def nextPosition = Position(position)

    def pushVar: Env = copy(position = position + 1)

    private[this] def bindVar(ref: VarRef, p: Position) =
      copy(varIndices = varIndices.updated(ref, p))

    def pushVar(ref: VarRef): Env =
      bindVar(ref, nextPosition).pushVar

    def pushExprVar(name: ExprVarName): Env =
      pushVar(EVarRef(name))

    def pushExprVar(maybeName: Option[ExprVarName]): Env =
      maybeName match {
        case Some(name) => pushExprVar(name)
        case None => pushVar
      }

    def pushTypeVar(name: ExprVarName): Env =
      pushVar(TVarRef(name))

    def hideTypeVar(name: TypeVarName): Env =
      copy(varIndices = varIndices - TVarRef(name))

    def bindExprVar(name: ExprVarName, p: Position): Env =
      bindVar(EVarRef(name), p)

    private[this] def vars: List[VarRef] = varIndices.keys.toList

    private[this] def lookupVar(varRef: VarRef): Option[s.SExpr] =
      varIndices.get(varRef).map(toSEVar)

    def lookupExprVar(name: ExprVarName): s.SExpr =
      lookupVar(EVarRef(name))
        .getOrElse(throw CompilationError(s"Unknown variable: $name. Known: ${vars.mkString(",")}"))

    def lookupTypeVar(name: TypeVarName): Option[s.SExpr] =
      lookupVar(TVarRef(name))
  }
   */
}

private[lf] final class OldPhaseOne(
    interface: PackageInterface,
    config: OldPhaseOne.Config,
) {

  import OldPhaseOne._
  import PhaseOne.{Env, Position}

  // Entry point for stage1 of speedy compilation pipeline
  // TODO https://github.com/digital-asset/daml/issues/11561
  // - reimplement this function to be stack safe
  @throws[CompilationError]
  private[speedy] def translateFromLF(env: Env, expr0: Expr): s.SExpr = {
    compile(env, expr0)
  }

  private[this] def handleLookup[X](location: String, x: Either[LookupError, X]) =
    x match {
      case Right(value) => value
      case Left(err) => throw SError.SErrorCrash(location, err.pretty)
    }

  // Stack-trace support is disabled by avoiding the construction of SELocation nodes.
  private[this] def maybeSELocation(loc: Location, sexp: s.SExpr): s.SExpr = {
    config.stacktracing match {
      case Compiler.NoStackTrace => sexp
      case Compiler.FullStackTrace => s.SELocation(loc, sexp)
    }
  }

  private[this] val withLabelS: (Profile.Label, s.SExpr) => s.SExpr =
    config.profiling match {
      case Compiler.NoProfile => { (_, expr) =>
        expr
      }
      case Compiler.FullProfile => { (label, expr) =>
        expr match {
          case s.SELabelClosure(_, expr1) => s.SELabelClosure(label, expr1)
          case _ => s.SELabelClosure(label, expr)
        }
      }
    }

  private[this] def withOptLabelS[L: Profile.LabelModule.Allowed](
      optLabel: Option[L with AnyRef],
      expr: s.SExpr,
  ): s.SExpr =
    optLabel match {
      case Some(label) => withLabelS(label, expr)
      case None => expr
    }

  private[this] def app(f: s.SExpr, a: s.SExpr) = s.SEApp(f, List(a))

  private[this] def let(env: Env, bound: s.SExpr)(f: (Position, Env) => s.SExpr): s.SELet =
    f(env.nextPosition, env.pushVar) match {
      case s.SELet(bounds, body) =>
        s.SELet(bound :: bounds, body)
      case otherwise =>
        s.SELet(List(bound), otherwise)
    }

  private[this] def unaryFunction(env: Env)(f: (Position, Env) => s.SExpr): s.SEAbs =
    f(env.nextPosition, env.pushVar) match {
      case s.SEAbs(n, body) => s.SEAbs(n + 1, body)
      case otherwise => s.SEAbs(1, otherwise)
    }

  private[this] def labeledUnaryFunction[L: Profile.LabelModule.Allowed](
      label: L with AnyRef,
      env: Env,
  )(
      body: (Position, Env) => s.SExpr
  ): s.SExpr =
    unaryFunction(env)((positions, env) => withLabelS(label, body(positions, env)))

  private[this] def compile(env: Env, expr0: Expr): s.SExpr =
    expr0 match {
      case EVar(name) =>
        env.lookupExprVar(name)
      case EVal(ref) =>
        s.SEVal(t.LfDefRef(ref))
      case EBuiltin(bf) =>
        compileBuiltin(env, bf)
      case EPrimCon(con) =>
        compilePrimCon(con)
      case EPrimLit(lit) =>
        compilePrimLit(lit)
      case EAbs(_, _, _) | ETyAbs(_, _) =>
        compileAbss(env, expr0)
      case EApp(_, _) | ETyApp(_, _) =>
        compileApps(env, expr0)
      case ERecCon(tApp, fields) =>
        compileERecCon(env, tApp, fields)
      case ERecProj(tapp, field, record) =>
        SBRecProj(
          tapp.tycon,
          handleLookup(
            NameOf.qualifiedNameOfCurrentFunc,
            interface.lookupRecordFieldInfo(tapp.tycon, field),
          ).index,
        )(compile(env, record))
      case erecupd: ERecUpd =>
        compileERecUpd(env, erecupd)
      case EStructCon(fields) =>
        val fieldsInputOrder =
          Struct.assertFromSeq(fields.iterator.map(_._1).zipWithIndex.toSeq)
        s.SEApp(
          s.SEBuiltin(SBStructCon(fieldsInputOrder)),
          mapToArray(fields) { case (_, e) => compile(env, e) },
        )
      case EStructProj(field, struct) =>
        SBStructProj(field)(compile(env, struct))
      case EStructUpd(field, struct, update) =>
        SBStructUpd(field)(compile(env, struct), compile(env, update))
      case ECase(scrut, alts) =>
        compileECase(env, scrut, alts)
      case ENil(_) =>
        s.SEValue.EmptyList
      case ECons(_, front, tail) =>
        // TODO(JM): Consider emitting SEValue(SList(...)) for
        // constant lists?
        val args =
          (front.iterator.map(compile(env, _)) ++ Seq(compile(env, tail))).toList
        if (front.length == 1) {
          s.SEApp(s.SEBuiltin(SBCons), args)
        } else {
          s.SEApp(s.SEBuiltin(SBConsMany(front.length)), args)
        }
      case ENone(_) =>
        s.SEValue.None
      case ESome(_, body) =>
        SBSome(compile(env, body))
      case EEnumCon(tyCon, consName) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          interface.lookupEnumConstructor(tyCon, consName),
        )
        s.SEValue(SEnum(tyCon, consName, rank))
      case EVariantCon(tapp, variant, arg) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          interface.lookupVariantConstructor(tapp.tycon, variant),
        ).rank
        SBVariantCon(tapp.tycon, variant, rank)(compile(env, arg))
      case let: ELet =>
        compileELet(env, let)
      case EUpdate(upd) =>
        compileEUpdate(env, upd)
      case ELocation(loc, EScenario(scen)) =>
        maybeSELocation(loc, compileScenario(env, scen, Some(loc)))
      case EScenario(scen) =>
        compileScenario(env, scen, None)
      case ELocation(loc, e) =>
        maybeSELocation(loc, compile(env, e))
      case EToAny(ty, e) =>
        SBToAny(ty)(compile(env, e))
      case EFromAny(ty, e) =>
        SBFromAny(ty)(compile(env, e))
      case ETypeRep(typ) =>
        s.SEValue(STypeRep(typ))
      case EToAnyException(ty, e) =>
        SBToAny(ty)(compile(env, e))
      case EFromAnyException(ty, e) =>
        SBFromAny(ty)(compile(env, e))
      case EThrow(_, ty, e) =>
        SBThrow(SBToAny(ty)(compile(env, e)))
      case EToInterface(iface @ _, tpl @ _, e) =>
        SBToInterface(tpl)(compile(env, e))
      case EFromInterface(iface @ _, tpl, e) =>
        SBFromInterface(tpl)(compile(env, e))
      case ECallInterface(iface, methodName, e) =>
        SBCallInterface(iface, methodName)(compile(env, e))
      case EToRequiredInterface(requiredIfaceId @ _, requiringIfaceId @ _, body @ _) =>
        compile(env, body)
      case EFromRequiredInterface(requiredIfaceId @ _, requiringIfaceId, body @ _) =>
        SBFromRequiredInterface(requiringIfaceId)(compile(env, body))
      case EInterfaceTemplateTypeRep(ifaceId, body @ _) =>
        SBInterfaceTemplateTypeRep(ifaceId)(compile(env, body))
      case ESignatoryInterface(ifaceId, body @ _) =>
        val arg = compile(env, body)
        SBSignatoryInterface(ifaceId)(arg)
      case EObserverInterface(ifaceId, body @ _) =>
        val arg = compile(env, body)
        SBObserverInterface(ifaceId)(arg)
      case EExperimental(name, _) =>
        SBExperimental(name)
    }

  @inline
  private[this] def compileIdentity(env: Env) = s.SEAbs(1, s.SEVarLevel(env.position))

  @inline
  private[this] def compileBuiltin(env: Env, bf: BuiltinFunction): s.SExpr = {

    def SBCompareNumeric(b: SBuiltinPure) = {
      val d = env.position
      s.SEAbs(3, s.SEApp(s.SEBuiltin(b), List(s.SEVarLevel(d + 1), s.SEVarLevel(d + 2))))
    }

    val SBLessNumeric = SBCompareNumeric(SBLess)
    val SBLessEqNumeric = SBCompareNumeric(SBLessEq)
    val SBGreaterNumeric = SBCompareNumeric(SBGreater)
    val SBGreaterEqNumeric = SBCompareNumeric(SBGreaterEq)
    val SBEqualNumeric = SBCompareNumeric(SBEqual)

    bf match {
      case BCoerceContractId => compileIdentity(env)
      // Numeric Comparisons
      case BLessNumeric => SBLessNumeric
      case BLessEqNumeric => SBLessEqNumeric
      case BGreaterNumeric => SBGreaterNumeric
      case BGreaterEqNumeric => SBGreaterEqNumeric
      case BEqualNumeric => SBEqualNumeric
      case BNumericToText => SBEToTextNumeric

      case BTextMapEmpty => s.SEValue.EmptyTextMap
      case BGenMapEmpty => s.SEValue.EmptyGenMap
      case _ =>
        s.SEBuiltin(bf match {
          case BTrace => SBTrace

          // Decimal arithmetic
          case BAddNumeric => SBAddNumeric
          case BSubNumeric => SBSubNumeric
          case BMulNumeric => SBMulNumeric
          case BDivNumeric => SBDivNumeric
          case BRoundNumeric => SBRoundNumeric
          case BCastNumeric => SBCastNumeric
          case BShiftNumeric => SBShiftNumeric

          // Int64 arithmetic
          case BAddInt64 => SBAddInt64
          case BSubInt64 => SBSubInt64
          case BMulInt64 => SBMulInt64
          case BModInt64 => SBModInt64
          case BDivInt64 => SBDivInt64
          case BExpInt64 => SBExpInt64

          // Conversions
          case BInt64ToNumeric => SBInt64ToNumeric
          case BNumericToInt64 => SBNumericToInt64
          case BDateToUnixDays => SBDateToUnixDays
          case BUnixDaysToDate => SBUnixDaysToDate
          case BTimestampToUnixMicroseconds => SBTimestampToUnixMicroseconds
          case BUnixMicrosecondsToTimestamp => SBUnixMicrosecondsToTimestamp

          // Text functions
          case BExplodeText => SBExplodeText
          case BImplodeText => SBImplodeText
          case BAppendText => SBAppendText

          case BInt64ToText => SBToText
          case BTextToText => SBToText
          case BTimestampToText => SBToText
          case BPartyToText => SBToText
          case BDateToText => SBToText
          case BContractIdToText => SBContractIdToText
          case BPartyToQuotedText => SBPartyToQuotedText
          case BCodePointsToText => SBCodePointsToText
          case BTextToParty => SBTextToParty
          case BTextToInt64 => SBTextToInt64
          case BTextToNumeric => SBTextToNumeric
          case BTextToCodePoints => SBTextToCodePoints

          case BSHA256Text => SBSHA256Text

          // List functions
          case BFoldl => SBFoldl
          case BFoldr => SBFoldr
          case BEqualList => SBEqualList

          // Errors
          case BError => SBError

          // Comparison
          case BEqualContractId => SBEqual
          case BEqual => SBEqual
          case BLess => SBLess
          case BLessEq => SBLessEq
          case BGreater => SBGreater
          case BGreaterEq => SBGreaterEq

          // TextMap

          case BTextMapInsert => SBMapInsert
          case BTextMapLookup => SBMapLookup
          case BTextMapDelete => SBMapDelete
          case BTextMapToList => SBMapToList
          case BTextMapSize => SBMapSize

          // GenMap

          case BGenMapInsert => SBMapInsert
          case BGenMapLookup => SBMapLookup
          case BGenMapDelete => SBMapDelete
          case BGenMapKeys => SBMapKeys
          case BGenMapValues => SBMapValues
          case BGenMapSize => SBMapSize

          case BScaleBigNumeric => SBScaleBigNumeric
          case BPrecisionBigNumeric => SBPrecisionBigNumeric
          case BAddBigNumeric => SBAddBigNumeric
          case BSubBigNumeric => SBSubBigNumeric
          case BDivBigNumeric => SBDivBigNumeric
          case BMulBigNumeric => SBMulBigNumeric
          case BShiftRightBigNumeric => SBShiftRightBigNumeric
          case BNumericToBigNumeric => SBNumericToBigNumeric
          case BBigNumericToNumeric => SBBigNumericToNumeric
          case BBigNumericToText => SBToText

          // Unstable Text Primitives
          case BTextToUpper => SBTextToUpper
          case BTextToLower => SBTextToLower
          case BTextSlice => SBTextSlice
          case BTextSliceIndex => SBTextSliceIndex
          case BTextContainsOnly => SBTextContainsOnly
          case BTextReplicate => SBTextReplicate
          case BTextSplitOn => SBTextSplitOn
          case BTextIntercalate => SBTextIntercalate

          // Implemented using normal SExpr

          case BCoerceContractId | BLessNumeric | BLessEqNumeric | BGreaterNumeric |
              BGreaterEqNumeric | BEqualNumeric | BNumericToText | BTextMapEmpty | BGenMapEmpty =>
            throw CompilationError(s"unexpected $bf")

          case BAnyExceptionMessage => SBAnyExceptionMessage
        })
    }
  }

  @inline
  private[this] def compilePrimCon(con: PrimCon): s.SExpr =
    con match {
      case PCTrue => s.SEValue.True
      case PCFalse => s.SEValue.False
      case PCUnit => s.SEValue.Unit
    }

  @inline
  private[this] def compilePrimLit(lit: PrimLit): s.SExpr =
    s.SEValue(lit match {
      case PLInt64(i) => SInt64(i)
      case PLNumeric(d) => SNumeric(d)
      case PLText(t) => SText(t)
      case PLTimestamp(ts) => STimestamp(ts)
      case PLDate(d) => SDate(d)
      case PLRoundingMode(roundingMode) => SInt64(roundingMode.ordinal.toLong)
    })

  // ERecUpd(_, f2, ERecUpd(_, f1, e0, e1), e2) => (e0, [f1, f2], [e1, e2])
  @inline
  private[this] def collectRecUpds(expr: Expr): (Expr, List[Name], List[Expr]) = {
    @tailrec
    def go(expr: Expr, fields: List[Name], updates: List[Expr]): (Expr, List[Name], List[Expr]) =
      stripLocs(expr) match {
        case ERecUpd(_, field, record, update) =>
          go(record, field :: fields, update :: updates)
        case _ =>
          (expr, fields, updates)
      }
    go(expr, List.empty, List.empty)
  }

  private def noArgs = new util.ArrayList[SValue](0)

  @inline
  private[this] def compileERecCon(
      env: Env,
      tApp: TypeConApp,
      fields: ImmArray[(FieldName, Expr)],
  ): s.SExpr =
    if (fields.isEmpty)
      s.SEValue(SRecord(tApp.tycon, ImmArray.Empty, noArgs))
    else
      s.SEApp(
        s.SEBuiltin(SBRecCon(tApp.tycon, fields.map(_._1))),
        fields.iterator.map(f => compile(env, f._2)).toList,
      )

  private[this] def compileERecUpd(env: Env, erecupd: ERecUpd): s.SExpr = {
    val tapp = erecupd.tycon
    val (record, fields, updates) = collectRecUpds(erecupd)
    if (fields.length == 1) {
      val index = handleLookup(
        NameOf.qualifiedNameOfCurrentFunc,
        interface.lookupRecordFieldInfo(tapp.tycon, fields.head),
      ).index
      SBRecUpd(tapp.tycon, index)(compile(env, record), compile(env, updates.head))
    } else {
      val indices =
        fields.map(name =>
          handleLookup(
            NameOf.qualifiedNameOfCurrentFunc,
            interface.lookupRecordFieldInfo(tapp.tycon, name),
          ).index
        )
      SBRecUpdMulti(tapp.tycon, indices.to(ImmArray))(
        (record :: updates).map(compile(env, _)): _*
      )
    }
  }

  private[this] def compileECase(env: Env, scrut: Expr, alts: ImmArray[CaseAlt]): s.SExpr =
    s.SECase(
      compile(env, scrut),
      mapToArray(alts) { case CaseAlt(pat, expr) =>
        pat match {
          case CPVariant(tycon, variant, binder) =>
            val rank = handleLookup(
              NameOf.qualifiedNameOfCurrentFunc,
              interface.lookupVariantConstructor(tycon, variant),
            ).rank
            s.SCaseAlt(
              t.SCPVariant(tycon, variant, rank),
              compile(env.pushExprVar(binder), expr),
            )

          case CPEnum(tycon, constructor) =>
            val rank = handleLookup(
              NameOf.qualifiedNameOfCurrentFunc,
              interface.lookupEnumConstructor(tycon, constructor),
            )
            s.SCaseAlt(t.SCPEnum(tycon, constructor, rank), compile(env, expr))

          case CPNil =>
            s.SCaseAlt(t.SCPNil, compile(env, expr))

          case CPCons(head, tail) =>
            s.SCaseAlt(t.SCPCons, compile(env.pushExprVar(head).pushExprVar(tail), expr))

          case CPPrimCon(pc) =>
            s.SCaseAlt(t.SCPPrimCon(pc), compile(env, expr))

          case CPNone =>
            s.SCaseAlt(t.SCPNone, compile(env, expr))

          case CPSome(body) =>
            s.SCaseAlt(t.SCPSome, compile(env.pushExprVar(body), expr))

          case CPDefault =>
            s.SCaseAlt(t.SCPDefault, compile(env, expr))
        }
      },
    )

  // Compile nested lets using constant stack.
  @tailrec
  private[this] def compileELet(
      env0: Env,
      eLet0: ELet,
      bounds0: List[s.SExpr] = List.empty,
  ): s.SELet = {
    val binding = eLet0.binding
    val bounds = withOptLabelS(binding.binder, compile(env0, binding.bound)) :: bounds0
    val env1 = env0.pushExprVar(binding.binder)
    eLet0.body match {
      case eLet1: ELet =>
        compileELet(env1, eLet1, bounds)
      case body0 =>
        compile(env1, body0) match {
          case s.SELet(bounds1, body1) =>
            s.SELet(bounds.foldLeft(bounds1)((acc, b) => b :: acc), body1)
          case otherwise =>
            s.SELet(bounds.reverse, otherwise)
        }
    }
  }

  @inline
  private[this] def compileEUpdate(env: Env, update: Update): s.SExpr =
    update match {
      case UpdatePure(_, e) =>
        compilePure(env, e)
      case UpdateBlock(bindings, body) =>
        compileBlock(env, bindings, body)
      case UpdateFetch(tmplId, coidE) =>
        t.FetchDefRef(tmplId)(compile(env, coidE))
      case UpdateFetchInterface(ifaceId, coidE) =>
        t.FetchDefRef(ifaceId)(compile(env, coidE))
      case UpdateEmbedExpr(_, e) =>
        compileEmbedExpr(env, e)
      case UpdateCreate(tmplId, arg) =>
        t.CreateDefRef(tmplId)(compile(env, arg))
      case UpdateCreateInterface(iface, arg) =>
        t.CreateDefRef(iface)(compile(env, arg))
      case UpdateExercise(tmplId, chId, cidE, argE) =>
        t.ChoiceDefRef(tmplId, chId)(compile(env, cidE), compile(env, argE))
      case UpdateExerciseInterface(ifaceId, chId, cidE, argE, typeRepE, guardE) =>
        t.GuardedChoiceDefRef(ifaceId, chId)(
          compile(env, cidE),
          compile(env, argE),
          compile(env, typeRepE),
          compile(env, guardE),
        )
      case UpdateExerciseByKey(tmplId, chId, keyE, argE) =>
        t.ChoiceByKeyDefRef(tmplId, chId)(compile(env, keyE), compile(env, argE))
      case UpdateGetTime =>
        SEGetTime
      case UpdateLookupByKey(RetrieveByKey(templateId, key)) =>
        t.LookupByKeyDefRef(templateId)(compile(env, key))
      case UpdateFetchByKey(RetrieveByKey(templateId, key)) =>
        t.FetchByKeyDefRef(templateId)(compile(env, key))

      case UpdateTryCatch(_, body, binder, handler) =>
        unaryFunction(env) { (tokenPos, env0) =>
          s.SETryCatch(
            app(compile(env0, body), env0.toSEVar(tokenPos)), {
              val env1 = env0.pushExprVar(binder)
              SBTryHandler(
                compile(env1, handler),
                env1.lookupExprVar(binder),
                env1.toSEVar(tokenPos),
              )
            },
          )
        }
    }

  @tailrec
  private[this] def compileAbss(env: Env, expr0: Expr, arity: Int = 0): s.SExpr =
    expr0 match {
      case EAbs((binder, typ @ _), body, ref @ _) =>
        compileAbss(env.pushExprVar(binder), body, arity + 1)
      case ETyAbs((binder, KNat), body) =>
        compileAbss(env.pushTypeVar(binder), body, arity + 1)
      case ETyAbs((binder, _), body) =>
        compileAbss(env.hideTypeVar(binder), body, arity)
      case _ if arity == 0 =>
        compile(env, expr0)
      case _ =>
        withLabelS(t.AnonymousClosure, s.SEAbs(arity, compile(env, expr0)))
    }

  @tailrec
  private[this] def compileApps(
      env: Env,
      expr0: Expr,
      args: List[s.SExpr] = List.empty,
  ): s.SExpr =
    expr0 match {
      case EApp(fun, arg) =>
        compileApps(env, fun, compile(env, arg) :: args)
      case ETyApp(fun, arg) =>
        compileApps(env, fun, translateType(env, arg).fold(args)(_ :: args))
      case _ if args.isEmpty =>
        compile(env, expr0)
      case _ =>
        s.SEApp(compile(env, expr0), args)
    }

  private[this] def translateType(env: Env, typ: Type): Option[s.SExpr] =
    typ match {
      case TNat(n) => SENat(n)
      case TVar(name) => env.lookupTypeVar(name)
      case _ => None
    }

  private[this] def compileScenario(env: Env, scen: Scenario, optLoc: Option[Location]): s.SExpr =
    scen match {
      case ScenarioPure(_, e) =>
        compilePure(env, e)
      case ScenarioBlock(bindings, body) =>
        compileBlock(env, bindings, body)
      case ScenarioCommit(partyE, updateE, _retType @ _) =>
        compileCommit(env, partyE, updateE, optLoc, mustFail = false)
      case ScenarioMustFailAt(partyE, updateE, _retType @ _) =>
        compileCommit(env, partyE, updateE, optLoc, mustFail = true)
      case ScenarioGetTime =>
        SEGetTime
      case ScenarioGetParty(e) =>
        compileGetParty(env, e)
      case ScenarioPass(relTime) =>
        compilePass(env, relTime)
      case ScenarioEmbedExpr(_, e) =>
        compileEmbedExpr(env, e)
    }

  @inline
  private[this] def compileCommit(
      env: Env,
      partyE: Expr,
      updateE: Expr,
      optLoc: Option[Location],
      mustFail: Boolean,
  ): s.SExpr =
    // let party = <partyE>
    //     update = <updateE>
    // in $submit(mustFail)(party, update)
    let(env, compile(env, partyE)) { (partyLoc, env) =>
      let(env, compile(env, updateE)) { (updateLoc, env) =>
        SBSSubmit(optLoc, mustFail)(env.toSEVar(partyLoc), env.toSEVar(updateLoc))
      }
    }

  @inline
  private[this] def compileGetParty(env: Env, expr: Expr): s.SExpr =
    labeledUnaryFunction(Profile.GetPartyLabel, env) { (tokenPos, env) =>
      SBSGetParty(compile(env, expr), env.toSEVar(tokenPos))
    }

  @inline
  private[this] def compilePass(env: Env, time: Expr): s.SExpr =
    labeledUnaryFunction(Profile.PassLabel, env) { (tokenPos, env) =>
      SBSPass(compile(env, time), env.toSEVar(tokenPos))
    }

  @inline
  private[this] def compileEmbedExpr(env: Env, expr: Expr): s.SExpr =
    // EmbedExpr's get wrapped into an extra layer of abstraction
    // to delay evaluation.
    // e.g.
    // embed (error "foo") => \token -> error "foo"
    unaryFunction(env) { (tokenPos, env) =>
      app(compile(env, expr), env.toSEVar(tokenPos))
    }

  private[this] def compilePure(env: Env, body: Expr): s.SExpr =
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    let(env, compile(env, body)) { (bodyPos, env) =>
      unaryFunction(env) { (tokenPos, env) =>
        SBSPure(env.toSEVar(bodyPos), env.toSEVar(tokenPos))
      }
    }

  private[this] def compileBlock(env: Env, bindings: ImmArray[Binding], body: Expr): s.SExpr =
    // do
    //   x <- f
    //   y <- g x
    //   z x y
    // =>
    // let f' = f
    // in \token ->
    //   let x = f' token
    //       y = g x token
    //   in z x y token
    let(env, compile(env, bindings.head.bound)) { (firstPos, env) =>
      unaryFunction(env) { (tokenPos, env) =>
        let(env, app(env.toSEVar(firstPos), env.toSEVar(tokenPos))) { (firstBoundPos, _env) =>
          val env = bindings.head.binder.fold(_env)(_env.bindExprVar(_, firstBoundPos))

          def loop(env: Env, list: List[Binding]): s.SExpr = list match {
            case Binding(binder, _, bound) :: tail =>
              let(env, app(compile(env, bound), env.toSEVar(tokenPos))) { (boundPos, _env) =>
                val env = binder.fold(_env)(_env.bindExprVar(_, boundPos))
                loop(env, tail)
              }
            case Nil =>
              app(compile(env, body), env.toSEVar(tokenPos))
          }

          loop(env, bindings.tail.toList)
        }
      }
    }

  @tailrec
  private[this] def stripLocs(expr: Expr): Expr =
    expr match {
      case ELocation(_, expr1) => stripLocs(expr1)
      case _ => expr
    }
}
