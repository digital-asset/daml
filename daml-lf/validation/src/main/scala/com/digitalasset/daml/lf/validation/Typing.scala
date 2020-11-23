// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.{ImmArray, Numeric, Struct}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{LanguageVersion, LanguageMajorVersion => LMV}
import com.daml.lf.validation.AlphaEquiv._
import com.daml.lf.validation.Util._
import com.daml.lf.validation.traversable.TypeTraversable

import scala.annotation.tailrec

private[validation] object Typing {

  /* Typing */

  private def checkUniq[A](xs: Iterator[A], mkError: A => ValidationError): Unit = {
    (xs foldLeft Set.empty[A])((acc, x) => if (acc(x)) throw mkError(x) else acc + x)
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
    val tComparison: Type = TForall(alpha.name -> KStar, alpha ->: alpha ->: TBool)
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
            TStruct(Struct.assertFromSeq(List(keyFieldName -> TText, valueFieldName -> alpha))))
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
      BToTextContractId -> TForall(alpha.name -> KStar, TContractId(alpha) ->: TOptional(TText)),
      BSHA256Text -> (TText ->: TText),
      BToQuotedTextParty -> (TParty ->: TText),
      BToTextCodePoints -> (TList(TInt64) ->: TText),
      BFromTextParty -> (TText ->: TOptional(TParty)),
      BFromTextInt64 -> (TText ->: TOptional(TInt64)),
      BFromTextNumeric -> TForall(alpha.name -> KNat, TText ->: TOptional(TNumeric(alpha))),
      BFromTextCodePoints -> (TText ->: TList(TInt64)),
      BError -> TForall(alpha.name -> KStar, TText ->: alpha),
      // ComparisonsA
      BLessNumeric -> tNumComparison,
      BLessEqNumeric -> tNumComparison,
      BGreaterNumeric -> tNumComparison,
      BGreaterEqNumeric -> tNumComparison,
      BImplodeText -> (TList(TText) ->: TText),
      BEqualNumeric -> tNumComparison,
      BEqualList ->
        TForall(
          alpha.name -> KStar,
          (alpha ->: alpha ->: TBool) ->: TList(alpha) ->: TList(alpha) ->: TBool),
      BEqualContractId ->
        TForall(alpha.name -> KStar, TContractId(alpha) ->: TContractId(alpha) ->: TBool),
      BEqual -> tComparison,
      BLess -> tComparison,
      BLessEq -> tComparison,
      BGreater -> tComparison,
      BGreaterEq -> tComparison,
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

  def checkModule(world: World, pkgId: PackageId, mod: Module): Unit = {
    val languageVersion = world.lookupPackage(NoContext, pkgId).languageVersion
    mod.definitions.foreach {
      case (dfnName, DDataType(_, params, cons)) =>
        val env =
          Env(languageVersion, world, ContextDefDataType(pkgId, mod.name, dfnName), params.toMap)
        params.values.foreach(env.checkKind)
        checkUniq[TypeVarName](params.keys, EDuplicateTypeParam(env.ctx, _))
        def tyConName = TypeConName(pkgId, QualifiedName(mod.name, dfnName))
        cons match {
          case DataRecord(fields) =>
            env.checkRecordType(fields)
          case DataVariant(fields) =>
            env.checkVariantType(fields)
          case DataEnum(values) =>
            env.checkEnumType(tyConName, params, values)
        }
      case (dfnName, dfn: DValue) =>
        Env(languageVersion, world, ContextDefValue(pkgId, mod.name, dfnName)).checkDValue(dfn)
      case (dfnName, DTypeSyn(params, replacementTyp)) =>
        val env =
          Env(languageVersion, world, ContextTemplate(pkgId, mod.name, dfnName), params.toMap)
        params.values.foreach(env.checkKind)
        checkUniq[TypeVarName](params.keys, EDuplicateTypeParam(env.ctx, _))
        env.checkType(replacementTyp, KStar)
    }
    mod.templates.foreach {
      case (dfnName, template) =>
        val tyConName = TypeConName(pkgId, QualifiedName(mod.name, dfnName))
        val env = Env(languageVersion, world, ContextTemplate(pkgId, mod.name, dfnName), Map.empty)
        world.lookupDataType(env.ctx, tyConName) match {
          case DDataType(_, ImmArray(), DataRecord(_)) =>
            env.checkTemplate(tyConName, template)
          case _ =>
            throw EExpectedTemplatableType(env.ctx, tyConName)
        }
    }
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

    private def newLocation(loc: Location): Env =
      copy(ctx = ContextLocation(loc))

    private def lookupExpVar(name: ExprVarName): Type =
      eVars.getOrElse(name, throw EUnknownExprVar(ctx, name))

    private def lookupTypeVar(name: TypeVarName): Kind =
      tVars.getOrElse(name, throw EUnknownTypeVar(ctx, name))

    def checkKind(kind: Kind): Unit = {
      @tailrec
      def loop(k: Kind, stack: List[Kind] = List.empty): Unit =
        k match {
          case KArrow(_, KNat) =>
            throw ENatKindRightOfArrow(ctx, k)
          case KArrow(param, result) =>
            loop(param, result :: stack)
          case KStar | KNat =>
            stack match {
              case head :: tail =>
                loop(head, tail)
              case Nil =>
            }
        }

      loop(kind)
    }

    /* Typing Ops*/

    def checkVariantType(variants: ImmArray[(VariantConName, Type)]): Unit = {
      checkUniq[VariantConName](variants.keys, EDuplicateVariantCon(ctx, _))
      variants.values.foreach(checkType(_, KStar))
    }

    def checkEnumType[X](
        tyConName: => TypeConName,
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
            choiceObservers @ _,
            selfBinder,
            (param, paramType),
            returnType,
            update) =>
          checkType(paramType, KStar)
          checkType(returnType, KStar)
          if (supportsFlexibleControllers) {
            introExprVar(param, paramType).checkExpr(controllers, TParties)
          } else {
            if (eVars.isDefinedAt(param))
              throw EIllegalShadowingExprVar(ctx, param)
            checkExpr(controllers, TParties)
          }
          choiceObservers.foreach {
            introExprVar(param, paramType)
              .checkExpr(_, TParties) // FIXME #7709, be conditional on: supportsContractObservers
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
        checkKind(k)
        introTypeVar(v, k).checkType(b, KStar)
        KStar
      case TStruct(fields) =>
        checkRecordType(fields.toImmArray)
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
        TStruct(recordType.mapValues(expandTypeSynonyms(_)))
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
        case DataRecord(recordType) =>
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
        case DataRecord(recordType) =>
          val fieldType = recordType.lookup(field, EUnknownField(ctx, field))
          checkExpr(record, typeConAppToType(typ0))
          fieldType
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfRecUpd(typ0: TypeConApp, field: FieldName, record: Expr, update: Expr): Type =
      checkTypConApp(typ0) match {
        case DataRecord(recordType) =>
          val typ1 = typeConAppToType(typ0)
          checkExpr(record, typ1)
          checkExpr(update, recordType.lookup(field, EUnknownField(ctx, field)))
          typ1
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfStructCon(fields: ImmArray[(FieldName, Expr)]): Type =
      Struct
        .fromSeq(fields.iterator.map { case (f, x) => f -> typeOf(x) }.toSeq)
        .fold(name => throw EDuplicateField(ctx, name), TStruct)

    private def typeOfStructProj(proj: EStructProj): Type = typeOf(proj.struct) match {
      case TStruct(structType) =>
        val index = structType.indexOf(proj.field)
        if (index < 0)
          throw EUnknownField(ctx, proj.field)
        else {
          proj.fieldIndex = Some(index)
          structType.toImmArray(index)._2
        }
      case typ =>
        throw EExpectedStructType(ctx, typ)
    }

    private def typeOfStructUpd(upd: EStructUpd): Type =
      typeOf(upd.struct) match {
        case typ @ TStruct(structType) =>
          val index = structType.indexOf(upd.field)
          if (index < 0)
            throw EUnknownField(ctx, upd.field)
          else {
            upd.fieldIndex = Some(index)
            checkExpr(upd.update, structType.toImmArray(index)._2)
            typ
          }
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

    private def typeofTyLam(tVar: TypeVarName, kind: Kind, expr: Expr): Type = {
      checkKind(kind)
      TForall(tVar -> kind, introTypeVar(tVar, kind).typeOf(expr))
    }

    private[this] def introPatternVariant(
        scrutTCon: TypeConName,
        scrutTArgs: ImmArray[Type],
        tparams: ImmArray[TypeVarName],
        cons: ImmArray[(VariantConName, Type)]
    ): PartialFunction[CasePat, Env] = {
      case CPVariant(patnTCon, con, bodyVar) if scrutTCon == patnTCon =>
        val conArgType = cons.lookup(con, EUnknownVariantCon(ctx, con))
        val bodyType =
          TypeSubst.substitute((tparams.iterator zip scrutTArgs.iterator).toMap, conArgType)
        introExprVar(bodyVar, bodyType)
      case CPDefault => this
      case otherwise => throw EPatternTypeMismatch(ctx, otherwise, TTyConApp(scrutTCon, scrutTArgs))
    }

    private[this] def introPatternEnum(
        scrutTCon: TypeConName,
        cons: ImmArray[VariantConName],
    ): CasePat => Env = {
      case CPEnum(patnTCon, con) if scrutTCon == patnTCon =>
        if (!cons.toSeq.contains(con)) throw EUnknownEnumCon(ctx, con)
        this
      case CPDefault => this
      case otherwise => throw EPatternTypeMismatch(ctx, otherwise, TTyCon(scrutTCon))
    }

    private[this] val introPatternUnit: CasePat => Env = {
      case CPUnit | CPDefault => this
      case otherwise => throw EPatternTypeMismatch(ctx, otherwise, TUnit)
    }

    private[this] val introPatternBool: CasePat => Env = {
      case CPTrue | CPFalse | CPDefault => this
      case otherwise => throw EPatternTypeMismatch(ctx, otherwise, TBool)
    }

    private[this] def introPatternList(elemType: Type): CasePat => Env = {
      case CPCons(headVar, tailVar) =>
        if (headVar == tailVar) throw EClashingPatternVariables(ctx, headVar)
        introExprVar(headVar, elemType).introExprVar(tailVar, TList(elemType))
      case CPNil | CPDefault => this
      case otherwise => throw EPatternTypeMismatch(ctx, otherwise, TList(elemType))
    }

    private[this] def introPatternOptional(elemType: Type): CasePat => Env = {
      case CPSome(bodyVar) => introExprVar(bodyVar, elemType)
      case CPNone | CPDefault => this
      case otherwise => throw EPatternTypeMismatch(ctx, otherwise, TOptional(elemType))
    }

    private[this] def introOnlyPatternDefault(scrutType: Type): CasePat => Env = {
      case CPDefault => this
      case otherwise => throw EPatternTypeMismatch(ctx, otherwise, scrutType)
    }

    private[this] def addPatternRank(ranks: Set[Int], pat: CasePat): MatchedRanks =
      pat match {
        case CPVariant(tycon, variant, _) =>
          lookupDataType(ctx, tycon) match {
            case DDataType(_, _, data: DataVariant) =>
              SomeRanks(ranks + data.constructorRank(variant))
            case _ =>
              throw EUnknownDefinition(ctx, LEDataVariant(tycon))
          }
        case CPEnum(tycon, constructor) =>
          lookupDataType(ctx, tycon) match {
            case DDataType(_, _, data: DataEnum) =>
              SomeRanks(ranks + data.constructorRank(constructor))
            case _ =>
              throw EUnknownDefinition(ctx, LEDataEnum(tycon))
          }
        case CPPrimCon(pc) =>
          pc match {
            case PCFalse | PCUnit => SomeRanks(ranks + 1)
            case PCTrue => SomeRanks(ranks + 0)
          }
        case CPCons(_, _) | CPSome(_) =>
          SomeRanks(ranks + 1)
        case CPNil | CPNil | CPNone =>
          SomeRanks(ranks + 0)
        case CPDefault =>
          AllRanks
      }

    private[this] def checkPatternExhaustiveness(
        expectedPatterns: ExpectedPatterns,
        alts: ImmArray[CaseAlt],
        scrutType: Type,
    ): Unit = {
      val foundPattern = alts.iterator.foldLeft[MatchedRanks](EmptyMatchedRanks) {
        case (AllRanks, _) => AllRanks
        case (SomeRanks(ranks), CaseAlt(pattern, _)) => addPatternRank(ranks, pattern)
      }

      foundPattern match {
        case SomeRanks(ranks) if ranks.size < expectedPatterns.number =>
          throw ENonExhaustivePatterns(ctx, expectedPatterns.missingPatterns(ranks), scrutType)
        case _ =>
      }
    }

    private[this] def typeOfCase(scrut: Expr, alts: ImmArray[CaseAlt]): Type = {
      val scrutType = typeOf(scrut)
      val (expectedPatterns, introPattern) = scrutType match {
        case TTyConApp(scrutTCon, scrutTArgs) =>
          lookupDataType(ctx, scrutTCon) match {
            case DDataType(_, dataParams, dataCons) =>
              dataCons match {
                case DataRecord(_) =>
                  (defaultExpectedPatterns, introOnlyPatternDefault(scrutType))
                case DataVariant(cons) =>
                  (
                    variantExpectedPatterns(scrutTCon, cons),
                    introPatternVariant(scrutTCon, scrutTArgs, dataParams.map(_._1), cons)
                  )
                case DataEnum(cons) =>
                  (
                    enumExpectedPatterns(scrutTCon, cons),
                    introPatternEnum(scrutTCon, cons)
                  )
              }
          }
        case TUnit =>
          (unitExpectedPatterns, introPatternUnit)
        case TBool =>
          (booleanExpectedPatterns, introPatternBool)
        case TList(elem) =>
          (listExpectedPatterns, introPatternList(elem))
        case TOptional(elem) =>
          (optionalExpectedPatterns, introPatternOptional(elem))
        case _ =>
          (defaultExpectedPatterns, introOnlyPatternDefault(scrutType))
      }

      val types = alts.iterator.map { case CaseAlt(patn, rhs) => introPattern(patn).typeOf(rhs) }.toList

      types match {
        case t :: ts =>
          ts.foreach(otherType =>
            if (!alphaEquiv(t, otherType)) throw ETypeMismatch(ctx, otherType, t, None))
          checkPatternExhaustiveness(expectedPatterns, alts, scrutType)
          t
        case Nil =>
          throw EEmptyCase(ctx)
      }
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

    private def typeOfExerciseByKey(
        tmplId: TypeConName,
        chName: ChoiceName,
        key: Expr,
        arg: Expr
    ): Type = {
      checkByKey(tmplId, key)
      val choice = lookupChoice(ctx, tmplId, chName)
      checkExpr(arg, choice.argBinder._2)
      TUpdate(choice.returnType)
    }

    private def typeOfFetch(tpl: TypeConName, cid: Expr): Type = {
      lookupTemplate(ctx, tpl)
      checkExpr(cid, TContractId(TTyCon(tpl)))
      TUpdate(TTyCon(tpl))
    }

    private def checkByKey(tmplId: TypeConName, key: Expr): Unit = {
      lookupTemplate(ctx, tmplId).key match {
        case None =>
          throw EKeyOperationForTemplateWithNoKey(ctx, tmplId)
        case Some(tmplKey) =>
          checkExpr(key, tmplKey.typ)
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
      case UpdateExerciseByKey(tpl, choice, key, arg) =>
        typeOfExerciseByKey(tpl, choice, key, arg)
      case UpdateFetch(tpl, cid) =>
        typeOfFetch(tpl, cid)
      case UpdateGetTime =>
        TUpdate(TTimestamp)
      case UpdateEmbedExpr(typ, exp) =>
        checkExpr(exp, TUpdate(typ))
        TUpdate(typ)
      case UpdateFetchByKey(retrieveByKey) =>
        checkByKey(retrieveByKey.templateId, retrieveByKey.key)
        // fetches return the contract id and the contract itself
        TUpdate(
          TStruct(
            Struct.assertFromSeq(
              List(
                contractIdFieldName -> TContractId(TTyCon(retrieveByKey.templateId)),
                contractFieldName -> TTyCon(retrieveByKey.templateId)
              ))))
      case UpdateLookupByKey(retrieveByKey) =>
        checkByKey(retrieveByKey.templateId, retrieveByKey.key)
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

    // checks that typ contains neither variables, nor quantifiers, nor synonyms
    private def checkAnyType_(typ: Type): Unit = {
      typ match {
        case TVar(_) | TForall(_, _) | TSynApp(_, _) =>
          throw EExpectedAnyType(ctx, typ)
        case _ =>
          TypeTraversable(typ).foreach(checkAnyType_)
      }
    }

    private def checkAnyType(typ: Type): Unit = {
      checkAnyType_(typ)
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
      case proj: EStructProj =>
        typeOfStructProj(proj)
      case upd: EStructUpd =>
        typeOfStructUpd(upd)
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
      case ELocation(loc, expr) =>
        newLocation(loc).typeOf(expr)
      case ENone(typ) =>
        checkType(typ, KStar)
        TOptional(typ)
      case ESome(typ, body) =>
        checkType(typ, KStar)
        val _ = checkExpr(body, typ)
        TOptional(typ)
      case EToAny(typ, body) =>
        checkAnyType(typ)
        checkExpr(body, typ)
        TAny
      case EFromAny(typ, body) =>
        checkAnyType(typ)
        checkExpr(body, TAny)
        TOptional(typ)
      case ETypeRep(typ) =>
        checkAnyType(typ)
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

  private[this] class ExpectedPatterns(val number: Int, patterns: => Iterator[CasePat]) {
    def missingPatterns(ranks: Set[Int]): List[CasePat] =
      patterns.zipWithIndex.collect { case (p, i) if !ranks(i) => p }.toList
  }
  private[this] object ExpectedPatterns {
    def apply(patterns: CasePat*) = new ExpectedPatterns(patterns.length, patterns.iterator)
  }

  private[this] val wildcard: ExprVarName = Name.assertFromString("_")
  private[this] def variantExpectedPatterns(
      scrutTCon: TypeConName,
      cons: ImmArray[(VariantConName, _)],
  ) = new ExpectedPatterns(
    cons.length,
    cons.iterator.map { case (variants, _) => CPVariant(scrutTCon, variants, wildcard) },
  )
  private[this] def enumExpectedPatterns(scrutTCon: TypeConName, cons: ImmArray[EnumConName]) =
    new ExpectedPatterns(cons.length, cons.iterator.map(CPEnum(scrutTCon, _)))
  private[this] val unitExpectedPatterns = ExpectedPatterns(CPUnit)
  private[this] val booleanExpectedPatterns = ExpectedPatterns(CPFalse, CPTrue)
  private[this] val listExpectedPatterns = ExpectedPatterns(CPNil, CPCons(wildcard, wildcard))
  private[this] val optionalExpectedPatterns = ExpectedPatterns(CPNone, CPSome(wildcard))
  private[this] val defaultExpectedPatterns = ExpectedPatterns(CPDefault)

  private[this] sealed trait MatchedRanks
  private[this] final case object AllRanks extends MatchedRanks
  private[this] final case class SomeRanks(ranks: Set[Int]) extends MatchedRanks
  private[this] val EmptyMatchedRanks = SomeRanks(Set.empty)

}
