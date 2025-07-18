// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy
package compiler

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{ImmArray, Struct}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{LookupError, PackageInterface}
import com.digitalasset.daml.lf.speedy.Compiler.{ProfilingMode, StackTraceMode, CompilationError}
import com.digitalasset.daml.lf.speedy.SBuiltinFun._
import com.digitalasset.daml.lf.speedy.SValue._
import com.digitalasset.daml.lf.speedy.compiler.SExpr0._
import com.digitalasset.daml.lf.speedy.{SExpr => t}
import com.daml.nameof.NameOf

import scala.annotation.tailrec

/** Initial Conversion (Phase of the speedy compiler pipeline)
  *
  * This compilation phase transforms from LF to SExpr0.
  * Include among other:
  *  - type erasure
  *  - translation of LF builtin into Speedy builtin
  *  - de Bruijn indexes transformation
  */

private[compiler] object PhaseOne {

  final case class Config(
      profiling: ProfilingMode,
      stacktracing: StackTraceMode,
  )

  private val SUGetTime = SEBuiltin(SBUGetTime)

  // corresponds to Daml-LF expression variable.
  case class VarRef(name: ExprVarName)

  final case class Position(idx: Int)

  object Env {
    val Empty = Env(0, Map.empty)
  }

  case class Env(
      position: Int,
      varIndices: Map[VarRef, Position],
  ) {

    def toSEVar(p: Position) = SEVarLevel(p.idx)

    def nextPosition = Position(position)

    def pushVar: Env = copy(position = position + 1)

    private[this] def bindVar(ref: VarRef, p: Position) =
      copy(varIndices = varIndices.updated(ref, p))

    def pushVar(ref: VarRef): Env =
      bindVar(ref, nextPosition).pushVar

    def pushExprVar(name: ExprVarName): Env =
      pushVar(VarRef(name))

    def pushExprVar(maybeName: Option[ExprVarName]): Env =
      maybeName match {
        case Some(name) => pushExprVar(name)
        case None => pushVar
      }

    def bindExprVar(name: ExprVarName, p: Position): Env =
      bindVar(VarRef(name), p)

    private[this] def vars: List[VarRef] = varIndices.keys.toList

    private[this] def lookupVar(varRef: VarRef): Option[SExpr] =
      varIndices.get(varRef).map(toSEVar)

    def lookupExprVar(name: ExprVarName): SExpr =
      lookupVar(VarRef(name))
        .getOrElse(throw CompilationError(s"Unknown variable: $name. Known: ${vars.mkString(",")}"))
  }

  // A type to represent a step of compilation Work
  sealed abstract class Work extends Product with Serializable
  object Work {
    final case class Return(result: SExpr) extends Work
    final case class CompileExp(env: Env, exp: Expr, cont: SExpr => Work) extends Work
    final case class Bind(work: Work, f: SExpr => Work) extends Work
  }
}

private[lf] final class PhaseOne(
    pkgInterface: PackageInterface,
    config: PhaseOne.Config,
) {

  import PhaseOne._
  import Work.{Return, Bind, CompileExp}

  private[this] def bindWork(work: Work)(f: SExpr => Work): Work = {
    Bind(work, f)
  }

  // Entry point for stage1 of speedy compilation pipeline
  @throws[CompilationError]
  def translateFromLF(env: Env, exp: Expr): SExpr = {
    outerCompile(env, exp)
  }

  private[this] def handleLookup[X](location: String, x: Either[LookupError, X]) =
    x match {
      case Right(value) => value
      case Left(err) => throw SError.SErrorCrash(location, err.pretty)
    }

  // Stack-trace support is disabled by avoiding the construction of SELocation nodes.
  private[this] def maybeSELocation(loc: Location, sexp: SExpr): SExpr = {
    config.stacktracing match {
      case Compiler.NoStackTrace => sexp
      case Compiler.FullStackTrace => SELocation(loc, sexp)
    }
  }

  private[this] def withLabel(label: Profile.Label, sexp: SExpr): SExpr =
    config.profiling match {
      case Compiler.NoProfile => sexp
      case Compiler.FullProfile =>
        sexp match {
          case SELabelClosure(_, sexp) => SELabelClosure(label, sexp)
          case sexp => SELabelClosure(label, sexp)
        }
    }

  private[this] def app(f: SExpr, a: SExpr) = SEApp(f, List(a))

  private[this] def let(env: Env, bound: SExpr)(f: (Position, Env) => Work): Work = {
    bindWork(f(env.nextPosition, env.pushVar)) {
      case SELet(bounds, body) =>
        Return(SELet(bound :: bounds, body))
      case otherwise =>
        Return(SELet(List(bound), otherwise))
    }
  }

  private[this] def unaryFunction(env: Env)(f: (Position, Env) => Work): Work = {
    bindWork(f(env.nextPosition, env.pushVar)) {
      case SEAbs(n, body) => Return(SEAbs(n + 1, body))
      case otherwise => Return(SEAbs(1, otherwise))
    }
  }

  private[this] def outerCompile(env: Env, exp: Expr): SExpr = {
    import Work._

    @tailrec
    def loop(work: Work): SExpr = {
      work match {
        case Return(result) => result // The final result of the tail-recursive 'loop'.
        case CompileExp(env, exp, cont) => loop(Bind(processExp(env, exp), cont))
        case Bind(work0, f0) => loop(processBind(work0, f0))
      }
    }

    @tailrec
    def processBind(work: Work, f: SExpr => Work): Work = {
      work match {
        case Return(result) => f(result)
        case CompileExp(env, exp, cont) =>
          Bind(processExp(env, exp), { result => Bind(cont(result), f) })
        case Bind(work, cont) =>
          processBind(work, { result => Bind(cont(result), f) })
      }
    }

    loop(CompileExp(env, exp, Return))
  }

  private[this] def compileExp(env: Env, exp: Expr)(cont: SExpr => Work): Work = {
    CompileExp(env, exp, cont)
  }

  private[this] def compileExps(env: Env, exps: List[Expr])(
      k: List[SExpr] => Work
  ): Work = {
    def loop(acc: List[SExpr], exps: List[Expr]): Work = {
      exps match {
        case Nil => k(acc.reverse)
        case exp :: exps =>
          compileExp(env, exp) { exp =>
            loop(exp :: acc, exps)
          }
      }
    }
    loop(Nil, exps)
  }

  private[this] def processExp(env: Env, exp: Expr): Work = {
    exp match {
      case EVar(name) =>
        Return(env.lookupExprVar(name))
      case EVal(ref) =>
        Return(SEVal(t.LfDefRef(ref)))
      case EBuiltinFun(bf) =>
        Return(compileBuiltin(env, bf))
      case EBuiltinCon(con) =>
        Return(compileBuiltinCon(con))
      case EBuiltinLit(lit) =>
        Return(compileBuiltinLit(lit))
      case EAbs(_, _) | ETyAbs(_, _) =>
        compileAbss(env, exp, arity = 0)
      case EApp(_, _) | ETyApp(_, _) =>
        compileApps(env, exp, args = List.empty)
      case ERecCon(tApp, fields) =>
        compileERecCon(env, tApp, fields)
      case ERecProj(tapp, field, record) =>
        compileExp(env, record) { record =>
          val fieldNum: Int = handleLookup(
            NameOf.qualifiedNameOfCurrentFunc,
            pkgInterface.lookupRecordFieldInfo(tapp.tycon, field),
          ).index
          Return(SBRecProj(tapp.tycon, fieldNum)(record))
        }
      case erecupd: ERecUpd =>
        compileERecUpd(env, erecupd)
      case EStructCon(fields) =>
        val exps = fields.toList.map { case (_, e) => e }
        compileExps(env, exps) { exps =>
          val fieldsInputOrder =
            Struct.assertFromSeq(fields.iterator.map(_._1).zipWithIndex.toSeq)
          Return(SEApp(SEBuiltin(SBStructCon(fieldsInputOrder)), exps))
        }
      case EStructProj(field, struct) =>
        compileExp(env, struct) { struct =>
          Return(SBStructProj(field)(struct))
        }
      case EStructUpd(field, struct, update) =>
        compileExp(env, struct) { struct =>
          compileExp(env, update) { update =>
            Return(SBStructUpd(field)(struct, update))
          }
        }
      case ECase(scrut, alts) =>
        compileECase(env, scrut, alts)
      case ENil(_) =>
        Return(SEValue.EmptyList)
      case ECons(_, front, tail) =>
        val exps: List[Expr] = front.toList ++ List(tail)
        compileExps(env, exps) { exps =>
          if (front.length == 1) {
            Return(SEApp(SEBuiltin(SBCons), exps))
          } else {
            Return(SEApp(SEBuiltin(SBConsMany(front.length)), exps))
          }
        }
      case ENone(_) =>
        Return(SEValue.None)
      case ESome(_, body) =>
        compileExp(env, body) { body =>
          Return(SBSome(body))
        }
      case EEnumCon(tyCon, consName) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          pkgInterface.lookupEnumConstructor(tyCon, consName),
        )
        Return(SEValue(SEnum(tyCon, consName, rank)))
      case EVariantCon(tapp, variant, arg) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          pkgInterface.lookupVariantConstructor(tapp.tycon, variant),
        ).rank
        compileExp(env, arg) { arg =>
          Return(SBVariantCon(tapp.tycon, variant, rank)(arg))
        }
      case let: ELet =>
        compileELet(env, let, List.empty)
      case EUpdate(upd) =>
        compileEUpdate(env, upd)
      case ELocation(loc, exp) =>
        compileExp(env, exp) { exp =>
          Return(maybeSELocation(loc, exp))
        }
      case EToAny(ty, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBToAny(ty)(exp))
        }
      case EFromAny(ty, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBFromAny(ty)(exp))
        }
      case ETypeRep(typ) =>
        Return(SEValue(STypeRep(typ)))
      case EToAnyException(ty, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBToAny(ty)(exp))
        }
      case EFromAnyException(ty, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBFromAny(ty)(exp))
        }
      case EThrow(_, ty, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBThrow(SBToAny(ty)(exp)))
        }
      case EToInterface(iface @ _, tpl @ _, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBToAnyContract(tpl)(exp))
        }
      case EFromInterface(iface @ _, tpl, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBFromInterface(tpl)(exp))
        }
      case EUnsafeFromInterface(iface @ _, tpl, cidExp, ifaceExp) =>
        compileExp(env, cidExp) { cidExp =>
          compileExp(env, ifaceExp) { ifaceExp =>
            Return(SBUnsafeFromInterface(tpl)(cidExp, ifaceExp))
          }
        }
      case ECallInterface(iface, methodName, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBCallInterface(iface, methodName)(exp))
        }
      case EToRequiredInterface(requiredIfaceId @ _, requiringIfaceId @ _, exp) =>
        compileExp(env, exp)(Return)
      case EFromRequiredInterface(requiredIfaceId @ _, requiringIfaceId, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBFromRequiredInterface(requiringIfaceId)(exp))
        }
      case EUnsafeFromRequiredInterface(requiredIfaceId, requiringIfaceId, cidExp, ifaceExp) =>
        compileExp(env, cidExp) { cidExp =>
          compileExp(env, ifaceExp) { ifaceExp =>
            Return(
              SBUnsafeFromRequiredInterface(requiredIfaceId, requiringIfaceId)(cidExp, ifaceExp)
            )
          }
        }
      case EInterfaceTemplateTypeRep(ifaceId, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBInterfaceTemplateTypeRep(ifaceId)(exp))
        }
      case ESignatoryInterface(ifaceId, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBSignatoryInterface(ifaceId)(exp))
        }
      case EObserverInterface(ifaceId, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBObserverInterface(ifaceId)(exp))
        }
      case EViewInterface(ifaceId, exp) =>
        compileExp(env, exp) { exp =>
          Return(SBViewInterface(ifaceId)(exp))
        }
      case EChoiceController(tpl, choiceName, contract, choiceArg) =>
        compileExp(env, contract) { contract =>
          compileExp(env, choiceArg) { choiceArg =>
            Return(t.ChoiceControllerDefRef(tpl, choiceName)(contract, choiceArg))
          }
        }
      case EChoiceObserver(tpl, choiceName, contract, choiceArg) =>
        compileExp(env, contract) { contract =>
          compileExp(env, choiceArg) { choiceArg =>
            Return(t.ChoiceObserverDefRef(tpl, choiceName)(contract, choiceArg))
          }
        }
      case EExperimental(name, _) =>
        Return(SBExperimental(name))
    }

  }

  private[this] def compileIdentity(env: Env) = SEAbs(1, SEVarLevel(env.position))

  private[this] def compileBuiltin(env: Env, bf: BuiltinFunction): SExpr = {

    bf match {
      case BCoerceContractId => compileIdentity(env)
      case BTextMapEmpty => SEValue.EmptyTextMap
      case BGenMapEmpty => SEValue.EmptyGenMap

      case _ =>
        SEBuiltin(bf match {
          case BTrace => SBTrace

          // Int64 arithmetic
          case BAddInt64 => SBAddInt64
          case BSubInt64 => SBSubInt64
          case BMulInt64 => SBMulInt64
          case BModInt64 => SBModInt64
          case BDivInt64 => SBDivInt64
          case BExpInt64 => SBExpInt64

          // Conversions
          case BDateToUnixDays => SBDateToUnixDays
          case BUnixDaysToDate => SBUnixDaysToDate
          case BTimestampToUnixMicroseconds => SBTimestampToUnixMicroseconds
          case BUnixMicrosecondsToTimestamp => SBUnixMicrosecondsToTimestamp

          // Text functions
          case BExplodeText => SBExplodeText
          case BImplodeText => SBImplodeText
          case BAppendText => SBAppendText

          case BInt64ToText => SBToText
          case BTimestampToText => SBToText
          case BPartyToText => SBToText
          case BDateToText => SBToText
          case BContractIdToText => SBContractIdToText
          case BPartyToQuotedText => SBPartyToQuotedText
          case BCodePointsToText => SBCodePointsToText
          case BTextToParty => SBTextToParty
          case BTextToInt64 => SBTextToInt64
          case BTextToCodePoints => SBTextToCodePoints
          case BTextToContractId => SBTextToContractId
          case BSHA256Text => SBSHA256Text
          case BKECCAK256Text => SBKECCAK256Text
          case BDecodeHex => SBDecodeHex
          case BEncodeHex => SBEncodeHex

          // List functions
          case BFoldl => SBFoldl
          case BFoldr => SBFoldr
          case BEqualList => SBEqualList

          // Errors
          case BError => SBUserError

          // Comparison
          case BEqualContractId => SBEqual
          case BEqual => SBEqual
          case BLess => SBLess
          case BLessEq => SBLessEq
          case BGreater => SBGreater
          case BGreaterEq => SBGreaterEq
          case BSECP256K1Bool => SBSECP256K1Bool

          // TextMap

          case BTextMapInsert => SBMapInsert
          case BTextMapLookup => SBMapLookup
          case BTextMapDelete => SBMapDelete
          case BTextMapToList => SBMapToList
          case BTextMapSize => SBMapSize

          // Numeric
          case BLessNumeric => SBLess
          case BLessEqNumeric => SBLessEq
          case BGreaterNumeric => SBGreater
          case BGreaterEqNumeric => SBGreaterEq
          case BEqualNumeric => SBEqual
          case BNumericToText => SBToText
          case BAddNumeric => SBAddNumeric
          case BSubNumeric => SBSubNumeric
          case BMulNumeric => SBMulNumeric
          case BDivNumeric => SBDivNumeric
          case BRoundNumeric => SBRoundNumeric
          case BCastNumeric => SBCastNumeric
          case BShiftNumeric => SBShiftNumeric
          case BInt64ToNumeric => SBInt64ToNumeric
          case BTextToNumeric => SBTextToNumeric
          case BNumericToInt64 => SBNumericToInt64
          case BNumericToBigNumeric => SBNumericToBigNumeric
          case BBigNumericToNumeric => SBBigNumericToNumeric

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
          case BBigNumericToText => SBToText

          // TypeRep
          case BTypeRepTyConName => SBTypeRepTyConName

          // Implemented using SExpr
          case BCoerceContractId | BTextMapEmpty | BGenMapEmpty | BLessNumeric | BLessEqNumeric |
              BGreaterNumeric | BGreaterEqNumeric | BEqualNumeric | BNumericToText |
              BAddNumeric | BSubNumeric | BMulNumeric | BDivNumeric | BRoundNumeric | BCastNumeric |
              BShiftNumeric | BInt64ToNumeric | BTextToNumericLegacy | BTextToNumeric |
              BNumericToInt64 | BNumericToBigNumeric | BBigNumericToNumeric =>
            throw CompilationError(s"unexpected $bf")

          case BAnyExceptionMessage => SBAnyExceptionMessage

          case BFailWithStatus => SBFailWithStatus
        })
    }
  }

  private[this] def compileBuiltinCon(con: BuiltinCon): SExpr =
    con match {
      case BCTrue => SEValue.True
      case BCFalse => SEValue.False
      case BCUnit => SEValue.Unit
    }

  private[this] def compileBuiltinLit(lit: BuiltinLit): SExpr =
    SEValue(lit match {
      case BLInt64(i) => SInt64(i)
      case BLNumeric(d) => SNumeric(d)
      case BLText(t) => SText(t)
      case BLTimestamp(ts) => STimestamp(ts)
      case BLDate(d) => SDate(d)
      case BLRoundingMode(roundingMode) => SInt64(roundingMode.ordinal.toLong)
      case BLFailureCategory(failureCategory) => SInt64(failureCategory.cantonCategoryId.toLong)
    })

  // ERecUpd(_, f2, ERecUpd(_, f1, e0, e1), e2) => (e0, [f1, f2], [e1, e2])
  private[this] def collectRecUpds(exp: Expr): (Expr, List[Name], List[Expr]) = {
    @tailrec
    def go(exp: Expr, fields: List[Name], updates: List[Expr]): (Expr, List[Name], List[Expr]) =
      stripLocs(exp) match {
        case ERecUpd(_, field, record, update) =>
          go(record, field :: fields, update :: updates)
        case _ =>
          (exp, fields, updates)
      }
    go(exp, List.empty, List.empty)
  }

  private[this] def compileERecCon(
      env: Env,
      tapp: TypeConApp,
      fields: ImmArray[(FieldName, Expr)],
  ): Work =
    tapp match {
      case TypeConApp(tycon, _) =>
        if (fields.isEmpty)
          Return(SEValue(SRecord(tycon, ImmArray.Empty, ArrayList.empty)))
        else {
          val exps = fields.toList.map(_._2)
          compileExps(env, exps) { exps =>
            val fieldNames = fields.map(_._1)
            Return(SEApp(SEBuiltin(SBRecCon(tycon, fieldNames)), exps))
          }
        }
    }

  private[this] def compileERecUpd(env: Env, erecupd: ERecUpd): Work = {
    val tapp = erecupd.tycon
    val (record, fields, updates) = collectRecUpds(erecupd)
    val fieldNums = fields.map { field =>
      handleLookup(
        NameOf.qualifiedNameOfCurrentFunc,
        pkgInterface.lookupRecordFieldInfo(tapp.tycon, field),
      ).index
    }
    fieldNums match {
      case List(num) =>
        compileExp(env, record) { record =>
          compileExp(env, updates.head) { update =>
            Return(SBRecUpd(tapp.tycon, num)(record, update))
          }
        }
      case _ =>
        compileExps(env, record :: updates) { exps =>
          Return(SBRecUpdMulti(tapp.tycon, fieldNums)(exps: _*))
        }
    }
  }

  private[this] def compileECase(env: Env, scrut: Expr, alts: ImmArray[CaseAlt]): Work = {
    compileExp(env, scrut) { scrut =>
      compileAlts(Nil, env, alts.toList) { alts =>
        Return(SECase(scrut, alts))
      }
    }
  }

  private[this] def compileAlts(acc: List[SCaseAlt], env: Env, exps: List[CaseAlt])(
      k: List[SCaseAlt] => Work
  ): Work = {
    exps match {
      case Nil => k(acc.reverse)
      case alt :: alts =>
        alt match {
          case CaseAlt(pat, rhs) =>
            compileAlt(env, pat, rhs) { rhs =>
              compileAlts(rhs :: acc, env, alts)(k)
            }
        }
    }
  }

  private[this] def compileAlt(env: Env, pat: CasePat, rhs: Expr)(k: SCaseAlt => Work): Work = {
    pat match {
      case CPVariant(tycon, variant, binder) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          pkgInterface.lookupVariantConstructor(tycon, variant),
        ).rank
        compileExp(env.pushExprVar(binder), rhs) { rhs =>
          k(SCaseAlt(t.SCPVariant(tycon, variant, rank), rhs))
        }
      case CPEnum(tycon, constructor) =>
        val rank = handleLookup(
          NameOf.qualifiedNameOfCurrentFunc,
          pkgInterface.lookupEnumConstructor(tycon, constructor),
        )
        compileExp(env, rhs) { rhs =>
          k(SCaseAlt(t.SCPEnum(tycon, constructor, rank), rhs))
        }
      case CPNil =>
        compileExp(env, rhs) { rhs =>
          k(SCaseAlt(t.SCPNil, rhs))
        }
      case CPCons(head, tail) =>
        compileExp(env.pushExprVar(head).pushExprVar(tail), rhs) { rhs =>
          k(SCaseAlt(t.SCPCons, rhs))
        }
      case CPBuiltinCon(pc) =>
        compileExp(env, rhs) { rhs =>
          k(SCaseAlt(t.SCPBuiltinCon(pc), rhs))
        }
      case CPNone =>
        compileExp(env, rhs) { rhs =>
          k(SCaseAlt(t.SCPNone, rhs))
        }
      case CPSome(e) =>
        compileExp(env.pushExprVar(e), rhs) { rhs =>
          k(SCaseAlt(t.SCPSome, rhs))
        }
      case CPDefault =>
        compileExp(env, rhs) { rhs =>
          k(SCaseAlt(t.SCPDefault, rhs))
        }
    }
  }

  private[this] def compileELet(env0: Env, eLet0: ELet, bounds0: List[SExpr]): Work = {
    eLet0 match {
      case ELet(Binding(binder, _, bound), body) =>
        compileExp(env0, bound) { bound0 =>
          val bound =
            binder match {
              case Some(label) => withLabel(label, bound0)
              case None => bound0
            }
          val bounds = bound :: bounds0
          val env1 = env0.pushExprVar(binder)
          body match {
            case eLet1: ELet =>
              compileELet(env1, eLet1, bounds) // recursive call in compileExp is stack-safe
            case _ =>
              compileExp(env1, body) {
                case SELet(bounds1, body1) =>
                  Return(SELet(bounds.foldLeft(bounds1)((acc, b) => b :: acc), body1))
                case body =>
                  Return(SELet(bounds.reverse, body))
              }
          }
        }
    }
  }

  private[this] def compileEUpdate(env: Env, update: Update): Work =
    update match {
      case UpdatePure(_, e) =>
        compilePure(env, e)
      case UpdateBlock(bindings, body) =>
        compileBlock(env, bindings, body)
      case UpdateFetchTemplate(tmplId, coid) =>
        compileExp(env, coid) { coid =>
          Return(t.FetchTemplateDefRef(tmplId)(coid))
        }
      case UpdateFetchInterface(ifaceId, coid) =>
        compileExp(env, coid) { coid =>
          Return(t.FetchInterfaceDefRef(ifaceId)(coid))
        }
      case UpdateEmbedExpr(_, exp) =>
        compileEmbedExpr(env, exp)
      case UpdateCreate(tmplId, arg) =>
        compileExp(env, arg) { arg =>
          Return(t.CreateDefRef(tmplId)(arg))
        }
      case UpdateCreateInterface(ifaceId, arg) =>
        unaryFunction(env) { (tokPos, env) =>
          compileExp(env, arg) { arg =>
            let(env, arg) { (payloadPos, env) =>
              let(env, SBResolveCreate(env.toSEVar(payloadPos), env.toSEVar(tokPos))) {
                (cidPos, env) =>
                  let(env, SEPreventCatch(SBViewInterface(ifaceId)(env.toSEVar(payloadPos)))) {
                    (_, env) => Return(env.toSEVar(cidPos))
                  }
              }
            }
          }
        }
      case UpdateExercise(tmplId, chId, cid, arg) =>
        compileExp(env, cid) { cid =>
          compileExp(env, arg) { arg =>
            Return(t.TemplateChoiceDefRef(tmplId, chId)(cid, arg))
          }
        }
      case UpdateExerciseInterface(ifaceId, chId, cid, arg, maybeGuard) =>
        compileExp(env, cid) { cid =>
          compileExp(env, arg) { arg =>
            def choiceDefRef(guard: SExpr) =
              Return(t.InterfaceChoiceDefRef(ifaceId, chId)(guard, cid, arg))
            maybeGuard match {
              case Some(guard) =>
                compileExp(env, guard)(choiceDefRef(_))
              case None =>
                choiceDefRef(SEAbs(1, SEValue.True))
            }
          }
        }
      case UpdateExerciseByKey(tmplId, chId, key, arg) =>
        compileExp(env, key) { key =>
          compileExp(env, arg) { arg =>
            Return(t.ChoiceByKeyDefRef(tmplId, chId)(key, arg))
          }
        }
      case UpdateGetTime =>
        Return(SUGetTime)
      case UpdateLedgerTimeLT(time) =>
        compileExp(env, time) { time =>
          Return(SBULedgerTimeLT(time))
        }
      case UpdateLookupByKey(templateId) =>
        Return(t.LookupByKeyDefRef(templateId)())
      case UpdateFetchByKey(templateId) =>
        Return(t.FetchByKeyDefRef(templateId)())
      case UpdateTryCatch(_, body, binder, handler) =>
        unaryFunction(env) { (tokenPos, env) =>
          compileExp(env, body) { body =>
            val env1 = env.pushExprVar(binder)
            compileExp(env1, handler) { handler =>
              Return(
                SETryCatch(
                  app(body, env.toSEVar(tokenPos)),
                  SBTryHandler(
                    handler,
                    env1.lookupExprVar(binder),
                    env1.toSEVar(tokenPos),
                  ),
                )
              )
            }
          }
        }
    }

  @tailrec
  private[this] def compileAbss(env: Env, exp: Expr, arity: Int): Work = {
    exp match {
      case EAbs((binder, typ @ _), body) =>
        compileAbss(env.pushExprVar(binder), body, arity + 1)
      case ETyAbs(_, body) =>
        compileAbss(env, body, arity)
      case _ if arity == 0 =>
        compileExp(env, exp)(Return)
      case _ =>
        compileExp(env, exp) { exp =>
          Return(withLabel(t.AnonymousClosure, SEAbs(arity, exp)))
        }
    }
  }

  val compileAppsX = compileApps _ // This allows silencing the @tailrec warning in one place...
  @tailrec // ...while still ensuring tail-recursion for the call in the ETyApp case.
  private[this] def compileApps(env: Env, exp: Expr, args: List[SExpr]): Work = {
    exp match {
      case EApp(fun, arg) =>
        compileExp(env, arg) { arg =>
          compileAppsX(env, fun, arg :: args) // recursive call in compileExp is stack-safe
        }
      case ETyApp(fun, _) =>
        compileApps(env, fun, args)
      case _ if args.isEmpty =>
        compileExp(env, exp)(Return)
      case _ =>
        compileExp(env, exp) { fun =>
          Return(SEApp(fun, args))
        }
    }
  }

  private[this] def compileEmbedExpr(env: Env, exp: Expr): Work =
    // EmbedExpr's get wrapped into an extra layer of abstraction
    // to delay evaluation.
    // e.g.
    // embed (error "foo") => \token -> error "foo"
    unaryFunction(env) { (tokenPos, env) =>
      compileExp(env, exp) { exp =>
        Return(app(exp, env.toSEVar(tokenPos)))
      }
    }

  private[this] def compilePure(env: Env, body: Expr): Work =
    // pure <E>
    // =>
    // ((\x token -> x) <E>)
    compileExp(env, body) { body =>
      let(env, body) { (bodyPos, env) =>
        unaryFunction(env) { (tokenPos, env) =>
          Return(SBPure(env.toSEVar(bodyPos), env.toSEVar(tokenPos)))
        }
      }
    }

  private[this] def compileBlock(
      env: Env,
      bindings: ImmArray[Binding],
      body: Expr,
  ): Work =
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
    compileExp(env, bindings.head.bound) { first =>
      let(env, first) { (firstPos, env) =>
        unaryFunction(env) { (tokenPos, env) =>
          let(env, app(env.toSEVar(firstPos), env.toSEVar(tokenPos))) { (firstBoundPos, _env) =>
            val env = bindings.head.binder.fold(_env)(_env.bindExprVar(_, firstBoundPos))

            def loop(env: Env, list: List[Binding]): Work =
              list match {
                case Binding(binder, _, bound) :: tail =>
                  compileExp(env, bound) { bound =>
                    let(env, app(bound, env.toSEVar(tokenPos))) { (boundPos, _env) =>
                      val env = binder.fold(_env)(_env.bindExprVar(_, boundPos))
                      loop(env, tail)
                    }
                  }
                case Nil =>
                  compileExp(env, body) { body =>
                    Return(app(body, env.toSEVar(tokenPos)))
                  }
              }

            loop(env, bindings.tail.toList)
          }
        }
      }
    }

  @tailrec
  private[this] def stripLocs(exp: Expr): Expr =
    exp match {
      case ELocation(_, exp) => stripLocs(exp)
      case _ => exp
    }
}
