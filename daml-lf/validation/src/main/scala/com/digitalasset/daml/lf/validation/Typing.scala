// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.validation

import com.digitalasset.daml.lf.data.{ImmArray, Numeric}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language.{LanguageVersion, LanguageMajorVersion => LMV}
import com.digitalasset.daml.lf.validation.AlphaEquiv._
import com.digitalasset.daml.lf.validation.Util._
import com.digitalasset.daml.lf.validation.traversable.TypeTraversable

import scala.annotation.tailrec

private[validation] object Typing {

  /* Typing */

  private def checkUniq[A](xs: Iterator[A], mkError: A => ValidationError): Unit = {
    (Set.empty[A] /: xs)((acc, x) => if (acc(x)) throw mkError(x) else acc + x)
    ()
  }

  private def kindOfBuiltin(bType: BuiltinType): Kind = bType match {
    case BTInt64 | BTText | BTTimestamp | BTParty | BTBool | BTDate | BTUnit | BTAny | BTTypeRep =>
      KStar
    case BTNumeric => KArrow(KNat, KStar)
    case BTList | BTUpdate | BTScenario | BTContractId | BTOptional | BTTextMap =>
      KArrow(KStar, KStar)
    case BTArrow | BTGenMap => KArrow(KStar, KArrow(KStar, KStar))
  }

  private def typeOfPrimLit(lit: PrimLit): Type = lit match {
    case PLInt64(_) => TInt64
    case PLNumeric(s) => TNumeric(TNat(Numeric.scale(s)))
    case PLText(_) => TText
    case PLTimestamp(_) => TTimestamp
    case PLParty(_) => TParty
    case PLDate(_) => TDate
  }

  protected[validation] lazy val typeOfBuiltinFunction = {
    val alpha = TVar(Name.assertFromString("$alpha$"))
    val beta = TVar(Name.assertFromString("$beta$"))
    val gamma = TVar(Name.assertFromString("$gamma$"))
    def tBinop(typ: Type): Type = typ ->: typ ->: typ
    val tNumBinop = TForall(alpha.name -> KNat, tBinop(TNumeric(alpha)))
    val tMultiNumBinop =
      TForall(
        alpha.name -> KNat,
        TForall(
          beta.name -> KNat,
          TForall(gamma.name -> KNat, TNumeric(alpha) ->: TNumeric(beta) ->: TNumeric(gamma))))
    val tNumConversion =
      TForall(alpha.name -> KNat, TForall(beta.name -> KNat, TNumeric(alpha) ->: TNumeric(beta)))
    def tComparison(bType: BuiltinType): Type = TBuiltin(bType) ->: TBuiltin(bType) ->: TBool
    val tNumComparison = TForall(alpha.name -> KNat, TNumeric(alpha) ->: TNumeric(alpha) ->: TBool)

    Map[BuiltinFunction, Type](
      BTrace -> TForall(alpha.name -> KStar, TText ->: alpha ->: alpha),
      // Numeric arithmetic
      BAddNumeric -> tNumBinop,
      BSubNumeric -> tNumBinop,
      BMulNumeric -> tMultiNumBinop,
      BDivNumeric -> tMultiNumBinop,
      BRoundNumeric -> TForall(alpha.name -> KNat, TInt64 ->: TNumeric(alpha) ->: TNumeric(alpha)),
      BCastNumeric -> tNumConversion,
      BShiftNumeric -> tNumConversion,
      // Int64 arithmetic
      BAddInt64 -> tBinop(TInt64),
      BSubInt64 -> tBinop(TInt64),
      BMulInt64 -> tBinop(TInt64),
      BDivInt64 -> tBinop(TInt64),
      BModInt64 -> tBinop(TInt64),
      BExpInt64 -> tBinop(TInt64),
      // Conversions
      BInt64ToNumeric -> TForall(alpha.name -> KNat, TInt64 ->: TNumeric(alpha)),
      BNumericToInt64 -> TForall(alpha.name -> KNat, TNumeric(alpha) ->: TInt64),
      BDateToUnixDays -> (TDate ->: TInt64),
      BUnixDaysToDate -> (TInt64 ->: TDate),
      BTimestampToUnixMicroseconds -> (TTimestamp ->: TInt64),
      BUnixMicrosecondsToTimestamp -> (TInt64 ->: TTimestamp),
      // Folds
      BFoldl ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            (beta ->: alpha ->: beta) ->: beta ->: TList(alpha) ->: beta)),
      BFoldr ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            (alpha ->: beta ->: beta) ->: beta ->: TList(alpha) ->: beta)),
      // Maps
      BTextMapEmpty ->
        TForall(
          alpha.name -> KStar,
          TTextMap(alpha)
        ),
      BTextMapInsert ->
        TForall(
          alpha.name -> KStar,
          TText ->: alpha ->: TTextMap(alpha) ->: TTextMap(alpha)
        ),
      BTextMapLookup ->
        TForall(
          alpha.name -> KStar,
          TText ->: TTextMap(alpha) ->: TOptional(alpha)
        ),
      BTextMapDelete ->
        TForall(
          alpha.name -> KStar,
          TText ->: TTextMap(alpha) ->: TTextMap(alpha)
        ),
      BTextMapToList ->
        TForall(
          alpha.name -> KStar,
          TTextMap(alpha) ->: TList(
            TStruct(ImmArray(keyFieldName -> TText, valueFieldName -> alpha)))
        ),
      BTextMapSize ->
        TForall(
          alpha.name -> KStar,
          TTextMap(alpha) ->: TInt64
        ),
      // GenMaps
      BGenMapEmpty ->
        TForall(alpha.name -> KStar, TForall(beta.name -> KStar, TGenMap(alpha, beta))),
      BGenMapInsert ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            alpha ->: beta ->: TGenMap(alpha, beta) ->: TGenMap(alpha, beta))),
      BGenMapLookup ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            alpha ->: TGenMap(alpha, beta) ->: TOptional(beta)
          )),
      BGenMapDelete ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            alpha ->: TGenMap(alpha, beta) ->: TGenMap(alpha, beta)
          )),
      BGenMapKeys ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            TGenMap(alpha, beta) ->: TList(alpha)
          )),
      BGenMapValues ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            TGenMap(alpha, beta) ->: TList(beta)
          )),
      BGenMapSize ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            TGenMap(alpha, beta) ->: TInt64
          )),
      // Text functions
      BExplodeText -> (TText ->: TList(TText)),
      BAppendText -> tBinop(TText),
      BToTextInt64 -> (TInt64 ->: TText),
      BToTextNumeric -> TForall(alpha.name -> KNat, TNumeric(alpha) ->: TText),
      BToTextText -> (TText ->: TText),
      BToTextTimestamp -> (TTimestamp ->: TText),
      BToTextParty -> (TParty ->: TText),
      BToTextDate -> (TDate ->: TText),
      BSHA256Text -> (TText ->: TText),
      BToQuotedTextParty -> (TParty ->: TText),
      BToTextCodePoints -> (TList(TInt64) ->: TText),
      BFromTextParty -> (TText ->: TOptional(TParty)),
      BFromTextInt64 -> (TText ->: TOptional(TInt64)),
      BFromTextNumeric -> TForall(alpha.name -> KNat, TText ->: TOptional(TNumeric(alpha))),
      BFromTextCodePoints -> (TText ->: TList(TInt64)),
      BError -> TForall(alpha.name -> KStar, TText ->: alpha),
      // ComparisonsA
      BLessInt64 -> tComparison(BTInt64),
      BLessNumeric -> tNumComparison,
      BLessText -> tComparison(BTText),
      BLessTimestamp -> tComparison(BTTimestamp),
      BLessDate -> tComparison(BTDate),
      BLessParty -> tComparison(BTParty),
      BLessEqInt64 -> tComparison(BTInt64),
      BLessEqNumeric -> tNumComparison,
      BLessEqText -> tComparison(BTText),
      BLessEqTimestamp -> tComparison(BTTimestamp),
      BLessEqDate -> tComparison(BTDate),
      BLessEqParty -> tComparison(BTParty),
      BGreaterInt64 -> tComparison(BTInt64),
      BGreaterNumeric -> tNumComparison,
      BGreaterText -> tComparison(BTText),
      BGreaterTimestamp -> tComparison(BTTimestamp),
      BGreaterDate -> tComparison(BTDate),
      BGreaterParty -> tComparison(BTParty),
      BGreaterEqInt64 -> tComparison(BTInt64),
      BGreaterEqNumeric -> tNumComparison,
      BGreaterEqText -> tComparison(BTText),
      BGreaterEqTimestamp -> tComparison(BTTimestamp),
      BGreaterEqDate -> tComparison(BTDate),
      BGreaterEqParty -> tComparison(BTParty),
      BImplodeText -> (TList(TText) ->: TText),
      BEqualNumeric -> tNumComparison,
      BEqualList ->
        TForall(
          alpha.name -> KStar,
          (alpha ->: alpha ->: TBool) ->: TList(alpha) ->: TList(alpha) ->: TBool),
      BEqualContractId ->
        TForall(alpha.name -> KStar, TContractId(alpha) ->: TContractId(alpha) ->: TBool),
      BEqual -> TForall(alpha.name -> KStar, alpha ->: alpha ->: TBool),
      BCoerceContractId ->
        TForall(
          alpha.name -> KStar,
          TForall(beta.name -> KStar, TContractId(alpha) ->: TContractId(beta))),
      // Unstable text functions
      BTextToUpper -> (TText ->: TText),
      BTextToLower -> (TText ->: TText),
      BTextSlice -> (TInt64 ->: TInt64 ->: TText ->: TText),
      BTextSliceIndex -> (TText ->: TText ->: TOptional(TInt64)),
      BTextContainsOnly -> (TText ->: TText ->: TBool),
      BTextReplicate -> (TInt64 ->: TText ->: TText),
      BTextSplitOn -> (TText ->: TText ->: TList(TText)),
      BTextIntercalate -> (TText ->: TList(TText) ->: TText),
    )
  }

  private def typeOfPRimCon(con: PrimCon): Type = con match {
    case PCTrue => TBool
    case PCFalse => TBool
    case PCUnit => TUnit
  }

  def checkModule(world: World, pkgId: PackageId, mod: Module): Unit =
    mod.definitions.foreach {
      case (dfnName, DDataType(_, params, cons)) =>
        val env =
          Env(mod.languageVersion, world, ContextTemplate(pkgId, mod.name, dfnName), params.toMap)
        checkUniq[TypeVarName](params.keys, EDuplicateTypeParam(env.ctx, _))
        def tyConName = TypeConName(pkgId, QualifiedName(mod.name, dfnName))
        cons match {
          case DataRecord(fields, template) =>
            env.checkRecordType(fields)
            template.foreach { tmpl =>
              if (params.nonEmpty) throw EExpectedTemplatableType(env.ctx, tyConName)
              env.checkTemplate(tyConName, tmpl)
            }
          case DataVariant(fields) =>
            env.checkVariantType(fields)
          case DataEnum(values) =>
            env.checkEnumType(tyConName, params, values)
        }
      case (dfnName, dfn: DValue) =>
        Env(mod.languageVersion, world, ContextDefValue(pkgId, mod.name, dfnName)).checkDValue(dfn)
      case (dfnName, DTypeSyn(params, replacementTyp)) =>
        val env =
          Env(mod.languageVersion, world, ContextTemplate(pkgId, mod.name, dfnName), params.toMap)
        checkUniq[TypeVarName](params.keys, EDuplicateTypeParam(env.ctx, _))
        env.checkType(replacementTyp, KStar)
    }

  case class Env(
      languageVersion: LanguageVersion,
      world: World,
      ctx: Context,
      tVars: Map[TypeVarName, Kind] = Map.empty,
      eVars: Map[ExprVarName, Type] = Map.empty
  ) {

    import world._

    /* Env Ops */

    private val supportsFlexibleControllers =
      LanguageVersion.ordering.gteq(languageVersion, LanguageVersion(LMV.V1, "2"))

    private val supportsComplexContractKeys =
      LanguageVersion.ordering.gteq(languageVersion, LanguageVersion(LMV.V1, "4"))

    private def introTypeVar(v: TypeVarName, k: Kind): Env = {
      copy(tVars = tVars + (v -> k))
    }

    private def introExprVar(x: ExprVarName, t: Type): Env = copy(eVars = eVars + (x -> t))

    private def introExprVar(xOpt: Option[ExprVarName], t: Type): Env =
      xOpt.fold(this)(introExprVar(_, t))

    private def lookupExpVar(name: ExprVarName): Type =
      eVars.getOrElse(name, throw EUnknownExprVar(ctx, name))

    private def lookupTypeVar(name: TypeVarName): Kind =
      tVars.getOrElse(name, throw EUnknownTypeVar(ctx, name))

    /* Typing Ops*/

    def checkVariantType(variants: ImmArray[(VariantConName, Type)]): Unit = {
      checkUniq[VariantConName](variants.keys, EDuplicateVariantCon(ctx, _))
      variants.values.foreach(checkType(_, KStar))
    }

    def checkEnumType[X](
        tyConName: TypeConName,
        params: ImmArray[X],
        values: ImmArray[EnumConName]
    ): Unit = {
      if (params.nonEmpty) throw EIllegalHigherEnumType(ctx, tyConName)
      checkUniq[Name](values.iterator, EDuplicateEnumCon(ctx, _))
    }

    def checkDValue(dfn: DValue): Unit = dfn match {
      case DValue(typ, _, body, isTest) =>
        checkType(typ, KStar)
        checkExpr(body, typ)
        if (isTest) dropForalls(typ) match {
          case TScenario(_) =>
            ()
          case _ =>
            throw EExpectedScenarioType(ctx, typ)
        }
    }

    @tailrec
    private def dropForalls(typ0: Type): Type = typ0 match {
      case TForall(_, typ) => dropForalls(typ)
      case _ => typ0
    }

    def checkRecordType(fields: ImmArray[(FieldName, Type)]): Unit = {
      checkUniq[FieldName](fields.keys, EDuplicateField(ctx, _))
      fields.values.foreach(checkType(_, KStar))
    }

    private def checkChoice(tplName: TypeConName, choice: TemplateChoice): Unit =
      choice match {
        case TemplateChoice(
            name @ _,
            consuming @ _,
            controllers,
            selfBinder,
            (param, paramType),
            returnType,
            update) =>
          checkType(paramType, KStar)
          checkType(returnType, KStar)
          if (supportsFlexibleControllers) {
            introExprVar(param, paramType).checkExpr(controllers, TParties)
          } else {
            param.filter(eVars.isDefinedAt).foreach(x => throw EIllegalShadowingExprVar(ctx, x))
            checkExpr(controllers, TParties)
          }
          introExprVar(selfBinder, TContractId(TTyCon(tplName)))
            .introExprVar(param, paramType)
            .checkExpr(update, TUpdate(returnType))
          ()
      }

    def checkTemplate(tplName: TypeConName, template: Template): Unit = {
      val Template(param, precond, signatories, agreementText, choices, observers, mbKey) =
        template
      val env = introExprVar(param, TTyCon(tplName))
      env.checkExpr(precond, TBool)
      env.checkExpr(signatories, TParties)
      env.checkExpr(observers, TParties)
      env.checkExpr(agreementText, TText)
      choices.values.foreach(env.checkChoice(tplName, _))
      mbKey.foreach { key =>
        checkType(key.typ, KStar)
        env.checkExpr(key.body, key.typ)
        if (!supportsComplexContractKeys) {
          checkValidKeyExpression(key.body)
        }
        checkExpr(key.maintainers, TFun(key.typ, TParties))
        ()
      }
    }

    private def checkValidKeyExpression(expr0: Expr): Unit = expr0 match {
      case ERecCon(_, fields) =>
        fields.values.foreach(checkValidKeyExpression)
      case otherwise =>
        checkValidProjections(otherwise)
    }

    private def checkValidProjections(expr0: Expr): Unit = expr0 match {
      case EVar(_) =>
      case ERecProj(_, _, rec) =>
        checkValidProjections(rec)
      case e =>
        throw EIllegalKeyExpression(ctx, e)
    }

    private def checkTypConApp(app: TypeConApp): DataCons = app match {
      case TypeConApp(tyCon, tArgs) =>
        val DDataType(_, tparams, dataCons) = lookupDataType(ctx, tyCon)
        if (tparams.length != tArgs.length) throw ETypeConAppWrongArity(ctx, tparams.length, app)
        (tArgs.iterator zip tparams.values).foreach((checkType _).tupled)
        TypeSubst.substitute((tparams.keys zip tArgs.iterator).toMap, dataCons)
    }

    def checkType(typ: Type, kind: Kind): Unit = {
      val typKind = kindOf(typ)
      if (kind != typKind)
        throw EKindMismatch(ctx, foundKind = typKind, expectedKind = kind)
    }

    private def kindOfDataType(defDataType: DDataType): Kind =
      defDataType.params.reverse.foldLeft[Kind](KStar) { case (acc, (_, k)) => KArrow(k, acc) }

    def kindOf(typ0: Type): Kind = typ0 match {
      case TSynApp(syn, args) =>
        val ty = expandSynApp(syn, args)
        checkType(ty, KStar)
        KStar
      case TVar(v) =>
        lookupTypeVar(v)
      case TNat(_) =>
        KNat
      case TTyCon(tycon) =>
        kindOfDataType(lookupDataType(ctx, tycon))
      case TApp(tFun, tArg) =>
        kindOf(tFun) match {
          case KStar | KNat => throw EExpectedHigherKind(ctx, KStar)
          case KArrow(argKind, resKind) =>
            checkType(tArg, argKind)
            resKind
        }
      case TBuiltin(bType) =>
        kindOfBuiltin(bType)
      case TForall((v, k), b) =>
        introTypeVar(v, k).checkType(b, KStar)
        KStar
      case TStruct(recordType) =>
        checkRecordType(recordType)
        KStar
    }

    private def expandTypeSynonyms(typ0: Type): Type = typ0 match {
      case TSynApp(syn, args) =>
        val ty = expandSynApp(syn, args)
        expandTypeSynonyms(ty)
      case TVar(_) =>
        typ0
      case TNat(_) =>
        typ0
      case TTyCon(_) =>
        typ0
      case TBuiltin(_) =>
        typ0
      case TApp(tFun, tArg) =>
        TApp(expandTypeSynonyms(tFun), expandTypeSynonyms(tArg))
      case TForall((v, k), b) =>
        TForall((v, k), introTypeVar(v, k).expandTypeSynonyms(b))
      case TStruct(recordType) =>
        TStruct(recordType.transform { (_, x) =>
          expandTypeSynonyms(x)
        })
    }

    private def expandSynApp(syn: TypeSynName, tArgs: ImmArray[Type]): Type = {
      val DTypeSyn(tparams, replacementTyp) = lookupTypeSyn(ctx, syn)
      if (tparams.length != tArgs.length)
        throw ETypeSynAppWrongArity(ctx, tparams.length, syn, tArgs)
      (tArgs.iterator zip tparams.values).foreach((checkType _).tupled)
      TypeSubst.substitute((tparams.keys zip tArgs.iterator).toMap, replacementTyp)
    }

    private def checkRecCon(typ: TypeConApp, recordExpr: ImmArray[(FieldName, Expr)]): Unit =
      checkTypConApp(typ) match {
        case DataRecord(recordType, _) =>
          val (exprFieldNames, fieldExprs) = recordExpr.unzip
          val (typeFieldNames, fieldTypes) = recordType.unzip
          if (exprFieldNames != typeFieldNames) throw EFieldMismatch(ctx, typ, recordExpr)
          (fieldExprs zip fieldTypes).map((checkExpr _).tupled)
          ()
        case _ =>
          throw EExpectedRecordType(ctx, typ)
      }

    private def checkVariantCon(typ: TypeConApp, con: VariantConName, conArg: Expr): Unit =
      checkTypConApp(typ) match {
        case DataVariant(variantType) =>
          checkExpr(conArg, variantType.lookup(con, EUnknownVariantCon(ctx, con)))
          ()
        case _ =>
          throw EExpectedVariantType(ctx, typ.tycon)
      }

    private def checkEnumCon(typConName: TypeConName, con: EnumConName): Unit =
      lookupDataType(ctx, typConName).cons match {
        case DataEnum(enumType) =>
          if (!enumType.toSeq.contains(con)) throw EUnknownEnumCon(ctx, con)
        case _ =>
          throw EExpectedEnumType(ctx, typConName)
      }

    private def typeOfRecProj(typ0: TypeConApp, field: FieldName, record: Expr): Type =
      checkTypConApp(typ0) match {
        case DataRecord(recordType, _) =>
          val fieldType = recordType.lookup(field, EUnknownField(ctx, field))
          checkExpr(record, typeConAppToType(typ0))
          fieldType
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfRecUpd(typ0: TypeConApp, field: FieldName, record: Expr, update: Expr): Type =
      checkTypConApp(typ0) match {
        case DataRecord(recordType, _) =>
          val typ1 = typeConAppToType(typ0)
          checkExpr(record, typ1)
          checkExpr(update, recordType.lookup(field, EUnknownField(ctx, field)))
          typ1
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfStructCon(fields: ImmArray[(FieldName, Expr)]): Type = {
      checkUniq[FieldName](fields.keys, EDuplicateField(ctx, _))
      TStruct(fields.transform { (_, x) =>
        typeOf(x)
      })
    }

    private def typeOfStructProj(field: FieldName, expr: Expr): Type = typeOf(expr) match {
      case TStruct(structType) =>
        structType.lookup(field, EUnknownField(ctx, field))
      case typ =>
        throw EExpectedStructType(ctx, typ)
    }

    private def typeOfStructUpd(field: FieldName, struct: Expr, update: Expr): Type =
      typeOf(struct) match {
        case typ @ TStruct(structType) =>
          checkExpr(update, structType.lookup(field, EUnknownField(ctx, field)))
          typ
        case typ =>
          throw EExpectedStructType(ctx, typ)
      }

    private def typeOfTmApp(fun: Expr, arg: Expr): Type = typeOf(fun) match {
      case TApp(TApp(TBuiltin(BTArrow), argType), resType) =>
        checkExpr(arg, argType)
        resType
      case typ =>
        throw EExpectedFunctionType(ctx, typ)
    }

    private def typeOfTyApp(expr: Expr, typ: Type): Type =
      typeOf(expr) match {
        case TForall((v, k), body) =>
          checkType(typ, k)
          TypeSubst.substitute(Map(v -> typ), body)
        case typ0 =>
          throw EExpectedUniversalType(ctx, typ0)
      }

    private def typeOfTmLam(x: ExprVarName, typ: Type, body: Expr): Type = {
      checkType(typ, KStar)
      typ ->: introExprVar(x, typ).typeOf(body)
    }

    private def typeofTyLam(tVar: TypeVarName, kind: Kind, expr: Expr): Type =
      TForall(tVar -> kind, introTypeVar(tVar, kind).typeOf(expr))

    private def introCasePattern[A](scrutType: Type, patn: CasePat): Env = patn match {
      case CPVariant(patnTCon, con, varName) =>
        val DDataType(_, tparams, dataCons) = lookupDataType(ctx, patnTCon)
        dataCons match {
          case DataVariant(variantCons) =>
            val conArgType0 = variantCons.lookup(con, EUnknownVariantCon(ctx, con))
            scrutType match {
              case TTyConApp(scrutTCon, scrutTArgs) =>
                if (scrutTCon != patnTCon) throw ETypeConMismatch(ctx, patnTCon, scrutTCon)
                val conArgType =
                  TypeSubst.substitute((tparams.map(_._1) zip scrutTArgs).toMap, conArgType0)
                introExprVar(varName, conArgType)
              case _ =>
                throw EExpectedDataType(ctx, scrutType)
            }
          case _ =>
            throw EExpectedVariantType(ctx, patnTCon)
        }

      case CPEnum(patnTCon, con) =>
        val DDataType(_, _, dataCons) = lookupDataType(ctx, patnTCon)
        dataCons match {
          case DataEnum(enumCons) =>
            if (!enumCons.toSeq.contains(con)) throw EUnknownEnumCon(ctx, con)
            scrutType match {
              case TTyCon(scrutTCon) =>
                if (scrutTCon != patnTCon) throw ETypeConMismatch(ctx, patnTCon, scrutTCon)
                this
              case _ =>
                throw EExpectedDataType(ctx, scrutType)
            }
          case _ =>
            throw EExpectedVariantType(ctx, patnTCon)
        }

      case CPPrimCon(con) =>
        val conType = typeOfPRimCon(con)
        if (!alphaEquiv(scrutType, conType))
          throw ETypeMismatch(ctx, foundType = scrutType, expectedType = conType, expr = None)
        this

      case CPNil =>
        scrutType match {
          case TList(_) =>
            this
          case _ =>
            throw EExpectedOptionType(ctx, scrutType)
        }

      case CPCons(headVar, tailVar) =>
        scrutType match {
          case TList(elemType) =>
            introExprVar(headVar, elemType).introExprVar(tailVar, TList(elemType))
          case _ =>
            throw EExpectedListType(ctx, scrutType)
        }

      case CPNone =>
        scrutType match {
          case TOptional(_) =>
            this
          case _ =>
            throw EExpectedOptionType(ctx, scrutType)
        }

      case CPSome(bodyVar) =>
        scrutType match {
          case TOptional(bodyType) =>
            introExprVar(bodyVar, bodyType)
          case _ =>
            throw EExpectedOptionType(ctx, scrutType)
        }

      case CPDefault =>
        this
    }

    private def typeOfCase(scrut: Expr, alts: ImmArray[CaseAlt]): Type =
      if (alts.isEmpty)
        throw EEmptyCase(ctx)
      else {
        val CaseAlt(patn0, rhs0) = alts(0)
        val scrutType = typeOf(scrut)
        val rhsType = introCasePattern(scrutType, patn0).typeOf(rhs0)
        for (i <- alts.indices.drop(1)) {
          val CaseAlt(patn, rhs) = alts(i)
          introCasePattern(scrutType, patn).checkExpr(rhs, rhsType)
        }
        rhsType
      }

    private def typeOfLet(binding: Binding, body: Expr): Type = binding match {
      case Binding(Some(vName), typ0, expr) =>
        checkType(typ0, KStar)
        val typ1 = checkExpr(expr, typ0)
        introExprVar(vName, typ1).typeOf(body)
      case Binding(_, _, bound @ _) =>
        typeOf(body)
    }

    private def checkCons(elemType: Type, front: ImmArray[Expr], tailExpr: Expr): Unit = {
      checkType(elemType, KStar)
      if (front.isEmpty) throw EEmptyConsFront(ctx)
      front.map(checkExpr(_, elemType))
      checkExpr(tailExpr, TList(elemType))
      ()
    }

    private def checkPure(typ: Type, expr: Expr): Unit = {
      checkType(typ, KStar)
      checkExpr(expr, typ)
      ()
    }

    private def typeOfScenarioBlock(bindings: ImmArray[Binding], body: Expr): Type = {
      val env = bindings.foldLeft(this) {
        case (env, Binding(vName, typ, bound)) =>
          env.checkType(typ, KStar)
          env.checkExpr(bound, TScenario(typ))
          env.introExprVar(vName, typ)
      }
      env.typeOf(body) match {
        case bodyTyp @ TScenario(_) => bodyTyp
        case bodyTyp => throw EExpectedScenarioType(ctx, bodyTyp)
      }
    }

    private def typeOfUpdateBlock(bindings: ImmArray[Binding], body: Expr): Type = {
      val env = bindings.foldLeft(this) {
        case (env, Binding(vName, typ, bound)) =>
          env.checkType(typ, KStar)
          env.checkExpr(bound, TUpdate(typ))
          env.introExprVar(vName, typ)
      }
      env.typeOf(body) match {
        case bodyTyp @ TUpdate(_) => bodyTyp
        case bodyTyp => throw EExpectedUpdateType(ctx, bodyTyp)
      }
    }

    private def typeOfCreate(tpl: TypeConName, arg: Expr): Type = {
      lookupTemplate(ctx, tpl)
      checkExpr(arg, TTyCon(tpl))
      TUpdate(TContractId(TTyCon(tpl)))
    }

    private def typeOfExercise(
        tpl: TypeConName,
        chName: ChoiceName,
        cid: Expr,
        actors: Option[Expr],
        arg: Expr
    ): Type = {
      val choice = lookupChoice(ctx, tpl, chName)
      checkExpr(cid, TContractId(TTyCon(tpl)))
      actors.foreach(checkExpr(_, TParties))
      checkExpr(arg, choice.argBinder._2)
      TUpdate(choice.returnType)
    }

    private def typeOfFetch(tpl: TypeConName, cid: Expr): Type = {
      checkExpr(cid, TContractId(TTyCon(tpl)))
      TUpdate(TTyCon(tpl))
    }

    private def checkRetrieveByKey(retrieveByKey: RetrieveByKey): Unit = {
      lookupTemplate(ctx, retrieveByKey.templateId).key match {
        case None =>
          throw EKeyOperationForTemplateWithNoKey(ctx, retrieveByKey.templateId)
        case Some(key) =>
          checkExpr(retrieveByKey.key, key.typ)
          ()
      }
    }

    private def typeOfUpdate(update: Update): Type = update match {
      case UpdatePure(typ, expr) =>
        checkPure(typ, expr)
        TUpdate(typ)
      case UpdateBlock(bindings, body) =>
        typeOfUpdateBlock(bindings, body)
      case UpdateCreate(tpl, arg) =>
        typeOfCreate(tpl, arg)
      case UpdateExercise(tpl, choice, cid, actors, arg) =>
        typeOfExercise(tpl, choice, cid, actors, arg)
      case UpdateFetch(tpl, cid) =>
        typeOfFetch(tpl, cid)
      case UpdateGetTime =>
        TUpdate(TTimestamp)
      case UpdateEmbedExpr(typ, exp) =>
        checkExpr(exp, TUpdate(typ))
        TUpdate(typ)
      case UpdateFetchByKey(retrieveByKey) =>
        checkRetrieveByKey(retrieveByKey)
        // fetches return the contract id and the contract itself
        TUpdate(
          TStruct(
            ImmArray(
              (contractIdFieldName, TContractId(TTyCon(retrieveByKey.templateId))),
              (contractFieldName, TTyCon(retrieveByKey.templateId)))))
      case UpdateLookupByKey(retrieveByKey) =>
        checkRetrieveByKey(retrieveByKey)
        TUpdate(TOptional(TContractId(TTyCon(retrieveByKey.templateId))))
    }

    private def typeOfCommit(typ: Type, party: Expr, update: Expr): Type = {
      checkType(typ, KStar)
      checkExpr(party, TParty)
      checkExpr(update, TUpdate(typ))
      TScenario(typ)
    }

    private def typeOfMustFailAt(typ: Type, party: Expr, update: Expr): Type = {
      checkType(typ, KStar)
      checkExpr(party, TParty)
      checkExpr(update, TUpdate(typ))
      TScenario(TUnit)
    }

    private def typeOfScenario(scenario: Scenario): Type = scenario match {
      case ScenarioPure(typ, expr) =>
        checkPure(typ, expr)
        TScenario(typ)
      case ScenarioBlock(bindings, body) =>
        typeOfScenarioBlock(bindings, body)
      case ScenarioCommit(party, update, typ) =>
        typeOfCommit(typ, party, update)
      case ScenarioMustFailAt(party, update, typ) =>
        typeOfMustFailAt(typ, party, update)
      case ScenarioPass(delta) =>
        checkExpr(delta, TInt64)
        TScenario(TTimestamp)
      case ScenarioGetTime =>
        TScenario(TTimestamp)
      case ScenarioGetParty(name) =>
        checkExpr(name, TText)
        TScenario(TParty)
      case ScenarioEmbedExpr(typ, exp) =>
        checkExpr(exp, TScenario(typ))
    }

    // we check that typ contains neither variables nor quantifiers
    private def checkGroundType_(typ: Type): Unit = {
      typ match {
        case TVar(_) | TForall(_, _) =>
          throw EExpectedAnyType(ctx, typ)
        case _ =>
          TypeTraversable(typ).foreach(checkGroundType_)
      }
    }

    private def checkGroundType(typ: Type): Unit = {
      checkGroundType_(typ)
      checkType(typ, KStar)
    }

    def typeOf(expr: Expr): Type = {
      val typ0 = typeOf_(expr)
      expandTypeSynonyms(typ0)
    }

    def typeOf_(expr0: Expr): Type = expr0 match {
      case EVar(name) =>
        lookupExpVar(name)
      case EVal(ref) =>
        lookupValue(ctx, ref).typ
      case EBuiltin(fun) =>
        typeOfBuiltinFunction(fun)
      case EPrimCon(con) =>
        typeOfPRimCon(con)
      case EPrimLit(lit) =>
        typeOfPrimLit(lit)
      case ERecCon(tycon, fields) =>
        checkRecCon(tycon, fields)
        typeConAppToType(tycon)
      case ERecProj(tycon, field, record) =>
        typeOfRecProj(tycon, field, record)
      case ERecUpd(tycon, field, record, update) =>
        typeOfRecUpd(tycon, field, record, update)
      case EVariantCon(tycon, variant, arg) =>
        checkVariantCon(tycon, variant, arg)
        typeConAppToType(tycon)
      case EEnumCon(tyCon, constructor) =>
        checkEnumCon(tyCon, constructor)
        TTyCon(tyCon)
      case EStructCon(fields) =>
        typeOfStructCon(fields)
      case EStructProj(field, struct) =>
        typeOfStructProj(field, struct)
      case EStructUpd(field, struct, update) =>
        typeOfStructUpd(field, struct, update)
      case EApp(fun, arg) =>
        typeOfTmApp(fun, arg)
      case ETyApp(expr, typ) =>
        typeOfTyApp(expr, typ)
      case EAbs((varName, typ), body, _) =>
        typeOfTmLam(varName, typ, body)
      case ETyAbs((vName, kind), body) =>
        typeofTyLam(vName, kind, body)
      case ECase(scruct, alts) =>
        typeOfCase(scruct, alts)
      case ELet(binding, body) =>
        typeOfLet(binding, body)
      case ENil(typ) =>
        checkType(typ, KStar)
        TList(typ)
      case ECons(typ, front, tail) =>
        checkCons(typ, front, tail)
        TList(typ)
      case EUpdate(update) =>
        typeOfUpdate(update)
      case EScenario(scenario) =>
        typeOfScenario(scenario)
      case ELocation(_, expr) =>
        typeOf(expr)
      case ENone(typ) =>
        checkType(typ, KStar)
        TOptional(typ)
      case ESome(typ, body) =>
        checkType(typ, KStar)
        val _ = checkExpr(body, typ)
        TOptional(typ)
      case EToAny(typ, body) =>
        checkGroundType(typ)
        checkExpr(body, typ)
        TAny
      case EFromAny(typ, body) =>
        checkGroundType(typ)
        checkExpr(body, TAny)
        TOptional(typ)
      case ETypeRep(typ) =>
        checkGroundType(typ)
        TTypeRep
    }

    def checkExpr(expr: Expr, typ0: Type): Type = {
      val exprType = typeOf(expr)
      val typ = expandTypeSynonyms(typ0)
      if (!alphaEquiv(exprType, typ))
        throw ETypeMismatch(ctx, foundType = exprType, expectedType = typ, expr = Some(expr))
      exprType
    }
  }

  /* Utils */

  private implicit final class TypeOp(val rightType: Type) extends AnyVal {
    def ->:(leftType: Type) = TFun(leftType, rightType)
  }

  private def typeConAppToType(app: TypeConApp): Type = app match {
    case TypeConApp(tcon, targs) => targs.foldLeft[Type](TTyCon(tcon))(TApp)
  }

}
