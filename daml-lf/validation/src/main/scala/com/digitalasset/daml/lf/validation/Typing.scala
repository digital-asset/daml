// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.{ImmArray, Numeric, Struct}
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{LanguageVersion, PackageInterface}
import com.daml.lf.validation.Util._
import com.daml.lf.validation.iterable.TypeIterable
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec

private[validation] object NewTyping { // NICK: WIP stack-safe type-checking code...

  def die[T](s: String): T = { // NICK
    sys.error(s"die: $s")
  }

  sealed abstract class Work[A]
  object Work {
    final case class Ret[A](v: A) extends Work[A]
    final case class Delay[A](thunk: () => Work[A]) extends Work[A]
    final case class Bind[A, X](work: Work[X], k: X => Work[A]) extends Work[A]
    // final case class BindThunk[A, X](work: () => Work[X], k: X => Work[A]) extends Work[A] // NICK: remove
  }

  import Work._ // {Ret, Delay, Bind, BindThunk} // NICK

  def runWork[R](work: Work[R]): R = {

    @tailrec
    def loop[A](work: Work[A]): A = work match {
      case Ret(v) => v
      case Delay(thunk) => loop(thunk())
      case Bind(w, k) => loop(loopBind(w, k))
      // case BindThunk(thunk, k) => loop(loopBind(thunk(), k))
    }

    @tailrec
    def loopBind[A, X](work: Work[X], k: X => Work[A]): Work[A] = work match {
      case Ret(x) => k(x)
      case Delay(thunk) => loopBind(thunk(), k)
      case Bind(work1, k1) => loopBind(work1, { x: Any => Bind(k1(x), k) }) // NICK: Any?
      // case BindThunk(thunk, k1) => loopBind(thunk(), { x: Any => BindThunk(() => k1(x), k) }) // NICK: Any?
    }

    loop(work)
  }

  import Util.handleLookup

  /* Typing */

  private def checkUniq[A](xs: Iterator[A], mkError: A => ValidationError): Unit = {
    discard((xs foldLeft Set.empty[A])((acc, x) => if (acc(x)) throw mkError(x) else acc + x))
  }

  private def kindOfBuiltin(bType: BuiltinType): Kind = bType match {
    case BTInt64 | BTText | BTTimestamp | BTParty | BTBool | BTDate | BTUnit | BTAny | BTTypeRep |
        BTAnyException | BTRoundingMode | BTBigNumeric =>
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
    case PLDate(_) => TDate
    case PLRoundingMode(_) => TRoundingMode
  }

  private def tBinop(typ: Type): Type = typ ->: typ ->: typ

  protected[validation] lazy val typeOfBuiltinFunction = {
    val alpha = TVar(Name.assertFromString("$alpha$"))
    val beta = TVar(Name.assertFromString("$beta$"))
    val gamma = TVar(Name.assertFromString("$gamma$"))
    val tNumBinop = TForall(alpha.name -> KNat, tBinop(TNumeric(alpha)))
    val tMultiNumBinop =
      TForall(
        alpha.name -> KNat,
        TForall(
          beta.name -> KNat,
          TForall(gamma.name -> KNat, TNumeric(alpha) ->: TNumeric(beta) ->: TNumeric(gamma)),
        ),
      )
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
          TForall(beta.name -> KStar, (beta ->: alpha ->: beta) ->: beta ->: TList(alpha) ->: beta),
        ),
      BFoldr ->
        TForall(
          alpha.name -> KStar,
          TForall(beta.name -> KStar, (alpha ->: beta ->: beta) ->: beta ->: TList(alpha) ->: beta),
        ),
      // Maps
      BTextMapEmpty ->
        TForall(
          alpha.name -> KStar,
          TTextMap(alpha),
        ),
      BTextMapInsert ->
        TForall(
          alpha.name -> KStar,
          TText ->: alpha ->: TTextMap(alpha) ->: TTextMap(alpha),
        ),
      BTextMapLookup ->
        TForall(
          alpha.name -> KStar,
          TText ->: TTextMap(alpha) ->: TOptional(alpha),
        ),
      BTextMapDelete ->
        TForall(
          alpha.name -> KStar,
          TText ->: TTextMap(alpha) ->: TTextMap(alpha),
        ),
      BTextMapToList ->
        TForall(
          alpha.name -> KStar,
          TTextMap(alpha) ->: TList(
            TStruct(Struct.assertFromSeq(List(keyFieldName -> TText, valueFieldName -> alpha)))
          ),
        ),
      BTextMapSize ->
        TForall(
          alpha.name -> KStar,
          TTextMap(alpha) ->: TInt64,
        ),
      // GenMaps
      BGenMapEmpty ->
        TForall(alpha.name -> KStar, TForall(beta.name -> KStar, TGenMap(alpha, beta))),
      BGenMapInsert ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            alpha ->: beta ->: TGenMap(alpha, beta) ->: TGenMap(alpha, beta),
          ),
        ),
      BGenMapLookup ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            alpha ->: TGenMap(alpha, beta) ->: TOptional(beta),
          ),
        ),
      BGenMapDelete ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            alpha ->: TGenMap(alpha, beta) ->: TGenMap(alpha, beta),
          ),
        ),
      BGenMapKeys ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            TGenMap(alpha, beta) ->: TList(alpha),
          ),
        ),
      BGenMapValues ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            TGenMap(alpha, beta) ->: TList(beta),
          ),
        ),
      BGenMapSize ->
        TForall(
          alpha.name -> KStar,
          TForall(
            beta.name -> KStar,
            TGenMap(alpha, beta) ->: TInt64,
          ),
        ),
      // Text functions
      BExplodeText -> (TText ->: TList(TText)),
      BAppendText -> tBinop(TText),
      BInt64ToText -> (TInt64 ->: TText),
      BNumericToText -> TForall(alpha.name -> KNat, TNumeric(alpha) ->: TText),
      BTextToText -> (TText ->: TText),
      BTimestampToText -> (TTimestamp ->: TText),
      BPartyToText -> (TParty ->: TText),
      BDateToText -> (TDate ->: TText),
      BContractIdToText -> TForall(alpha.name -> KStar, TContractId(alpha) ->: TOptional(TText)),
      BSHA256Text -> (TText ->: TText),
      BPartyToQuotedText -> (TParty ->: TText),
      BCodePointsToText -> (TList(TInt64) ->: TText),
      BTextToParty -> (TText ->: TOptional(TParty)),
      BTextToInt64 -> (TText ->: TOptional(TInt64)),
      BTextToNumeric -> TForall(alpha.name -> KNat, TText ->: TOptional(TNumeric(alpha))),
      BTextToCodePoints -> (TText ->: TList(TInt64)),
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
          (alpha ->: alpha ->: TBool) ->: TList(alpha) ->: TList(alpha) ->: TBool,
        ),
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
          TForall(beta.name -> KStar, TContractId(alpha) ->: TContractId(beta)),
        ),
      // BigNumeric function
      BScaleBigNumeric -> (TBigNumeric ->: TInt64),
      BPrecisionBigNumeric -> (TBigNumeric ->: TInt64),
      BAddBigNumeric -> (TBigNumeric ->: TBigNumeric ->: TBigNumeric),
      BSubBigNumeric -> (TBigNumeric ->: TBigNumeric ->: TBigNumeric),
      BMulBigNumeric -> (TBigNumeric ->: TBigNumeric ->: TBigNumeric),
      BDivBigNumeric -> (TInt64 ->: TRoundingMode ->: TBigNumeric ->: TBigNumeric ->: TBigNumeric),
      BShiftRightBigNumeric -> (TInt64 ->: TBigNumeric ->: TBigNumeric),
      BBigNumericToNumeric -> TForall(alpha.name -> KNat, TBigNumeric ->: TNumeric(alpha)),
      BNumericToBigNumeric -> TForall(alpha.name -> KNat, TNumeric(alpha) ->: TBigNumeric),
      BBigNumericToText -> (TBigNumeric ->: TText),
      // Exception functions
      BAnyExceptionMessage -> (TAnyException ->: TText),
      // TypeRep functions
      BTypeRepTyConName -> (TTypeRep ->: TOptional(TText)),
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

  def checkModule(pkgInterface: PackageInterface, pkgId: PackageId, mod: Module): Unit = { // entry point
    val langVersion = handleLookup(Context.None, pkgInterface.lookupPackage(pkgId)).languageVersion
    mod.definitions.foreach {
      case (dfnName, DDataType(_, params, cons)) =>
        val env =
          Env(
            langVersion,
            pkgInterface,
            Context.DefDataType(pkgId, mod.name, dfnName),
            params.toMap,
          )
        params.values.foreach(env.checkKind)
        checkUniq[TypeVarName](params.keys, EDuplicateTypeParam(env.ctx, _))
        cons match {
          case DataRecord(fields) =>
            env.checkRecordType(fields)
          case DataVariant(fields) =>
            env.checkVariantType(fields)
          case DataEnum(values) =>
            val tyConName = TypeConName(pkgId, QualifiedName(mod.name, dfnName))
            env.checkEnumType(tyConName, params, values)
          case DataInterface =>
            val tyConName = TypeConName(pkgId, QualifiedName(mod.name, dfnName))
            env.checkInterfaceType(tyConName, params)
        }
      case (dfnName, dfn: DValue) =>
        Env(langVersion, pkgInterface, Context.DefValue(pkgId, mod.name, dfnName)).checkDValue(dfn)
      case (dfnName, DTypeSyn(params, replacementTyp)) =>
        val env =
          Env(langVersion, pkgInterface, Context.Template(pkgId, mod.name, dfnName), params.toMap)
        params.values.foreach(env.checkKind)
        checkUniq[TypeVarName](params.keys, EDuplicateTypeParam(env.ctx, _))
        env.checkType(replacementTyp, KStar)
    }
    mod.templates.foreach { case (dfnName, template) =>
      val tyConName = TypeConName(pkgId, QualifiedName(mod.name, dfnName))
      val env = Env(langVersion, pkgInterface, Context.Template(tyConName), Map.empty)
      handleLookup(env.ctx, pkgInterface.lookupDataType(tyConName)) match {
        case DDataType(_, ImmArray(), DataRecord(_)) =>
          env.checkTemplate(tyConName, template)
        case _ =>
          throw EExpectedTemplatableType(env.ctx, tyConName)
      }
    }
    mod.exceptions.foreach { case (exnName, message) =>
      val tyConName = TypeConName(pkgId, QualifiedName(mod.name, exnName))
      val env = Env(langVersion, pkgInterface, Context.DefException(tyConName), Map.empty)
      handleLookup(env.ctx, pkgInterface.lookupDataType(tyConName)) match {
        case DDataType(_, ImmArray(), DataRecord(_)) =>
          env.checkDefException(tyConName, message)
        case _ =>
          throw EExpectedExceptionableType(env.ctx, tyConName)
      }
    }
    mod.interfaces.foreach { case (ifaceName, iface) =>
      // uniquess of choice names is already checked on construction of the choice map.
      val tyConName = TypeConName(pkgId, QualifiedName(mod.name, ifaceName))
      val env = Env(langVersion, pkgInterface, Context.DefInterface(tyConName), Map.empty)
      env.checkDefIface(tyConName, iface)
    }
  }

  case class Env(
      languageVersion: LanguageVersion,
      pkgInterface: PackageInterface,
      ctx: Context,
      tVars: Map[TypeVarName, Kind] = Map.empty,
      eVars: Map[ExprVarName, Type] = Map.empty,
  ) {

    def entry_typeOf(e: Expr): Type = { // NICK: entry point
      runWork(work_typeOf(e)) // NICK: the only place which may call runWork
    }

    // This can (stack!)safely be used everwhere...
    private def typeOf[T](e: Expr)(k: Type => Work[T]): Work[T] = {
      // BindThunk(() => work_typeOf(e), k) //NICK
      Bind(Delay(() => work_typeOf(e)), k)
    }

    // NICK: kill & fix all callers to use checkExpr
    private def legacy_checkExpr(expr: Expr, typ: Type): Unit = {
      val exprType = recurse_typeOf(expr)
      if (!alphaEquiv(exprType, typ))
        throw ETypeMismatch(ctx, foundType = exprType, expectedType = typ, expr = Some(expr))
    }

    // NICK: kill & fix all callers to use typeOf
    private def recurse_typeOf(e: Expr): Type = {
      runWork(work_typeOf(e)) // NICK: nope! mustn't call runWork!

    }

    /* Env Ops */

    private def introTypeVar(v: TypeVarName, k: Kind): Env = {
      copy(tVars = tVars + (v -> k))
    }

    private def introExprVar(x: ExprVarName, t: Type): Env = copy(eVars = eVars + (x -> t))

    private def introExprVar(xOpt: Option[ExprVarName], t: Type): Env =
      xOpt.fold(this)(introExprVar(_, t))

    private def newLocation(loc: Location): Env =
      copy(ctx = Context.Location(loc))

    private def lookupExpVar(name: ExprVarName): Type =
      eVars.getOrElse(name, throw EUnknownExprVar(ctx, name))

    private def lookupTypeVar(name: TypeVarName): Kind =
      tVars.getOrElse(name, throw EUnknownTypeVar(ctx, name))

    def checkKind(kind: Kind): Unit = { // testing entry point
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

    private[NewTyping] def checkVariantType(variants: ImmArray[(VariantConName, Type)]): Unit = {
      checkUniq[VariantConName](variants.keys, EDuplicateVariantCon(ctx, _))
      variants.values.foreach(checkType(_, KStar))
    }

    private[NewTyping] def checkEnumType[X](
        tyConName: => TypeConName,
        params: ImmArray[X],
        values: ImmArray[EnumConName],
    ): Unit = {
      if (params.nonEmpty) throw EIllegalHigherEnumType(ctx, tyConName)
      checkUniq[Name](values.iterator, EDuplicateEnumCon(ctx, _))
    }

    private[NewTyping] def checkInterfaceType[X](
        tyConName: => TypeConName,
        params: ImmArray[X],
    ): Unit = {
      if (params.nonEmpty) throw EIllegalHigherInterfaceType(ctx, tyConName)
      val _ = handleLookup(ctx, pkgInterface.lookupInterface(tyConName))
    }

    private[NewTyping] def checkDValue(dfn: DValue): Unit = dfn match {
      case DValue(typ, body, isTest) =>
        checkType(typ, KStar)
        legacy_checkExpr(body, typ)
        if (isTest) {
          discard(toScenario(dropForalls(typ)))
        }
        ??? // NICK: not tested in TypingSpec
    }

    @tailrec
    private def dropForalls(typ0: Type): Type = typ0 match {
      case TForall(_, typ) => dropForalls(typ)
      case _ => typ0
    }

    private[NewTyping] def checkRecordType(fields: ImmArray[(FieldName, Type)]): Unit = {
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
              update,
            ) =>
          checkType(paramType, KStar)
          checkType(returnType, KStar)
          introExprVar(param, paramType).legacy_checkExpr(controllers, TParties)
          choiceObservers.foreach(
            introExprVar(param, paramType).legacy_checkExpr(_, TParties)
          )
          introExprVar(selfBinder, TContractId(TTyCon(tplName)))
            .introExprVar(param, paramType)
            .legacy_checkExpr(update, TUpdate(returnType))
          ()
      }

    private[NewTyping] def checkTemplate(tplName: TypeConName, template: Template): Unit = {
      val Template(
        param,
        precond,
        signatories,
        agreementText,
        choices,
        observers,
        mbKey,
        implementations,
      ) =
        template
      val env = introExprVar(param, TTyCon(tplName))
      env.legacy_checkExpr(precond, TBool)
      env.legacy_checkExpr(signatories, TParties)
      env.legacy_checkExpr(observers, TParties)
      env.legacy_checkExpr(agreementText, TText)
      choices.values.foreach(env.checkChoice(tplName, _))
      env.checkIfaceImplementations(tplName, implementations)
      mbKey.foreach { key =>
        checkType(key.typ, KStar)
        env.legacy_checkExpr(key.body, key.typ)
        legacy_checkExpr(key.maintainers, TFun(key.typ, TParties))
        ()
      }
    }

    private[NewTyping] def checkDefIface(ifaceName: TypeConName, iface: DefInterface): Unit =
      iface match {
        case DefInterface(requires, param, choices, methods, coImplements, _) =>
          val env = introExprVar(param, TTyCon(ifaceName))
          if (requires(ifaceName))
            throw ECircularInterfaceRequires(ctx, ifaceName)
          for {
            required <- requires
            requiredRequired <- handleLookup(ctx, pkgInterface.lookupInterface(required)).requires
            if !requires(requiredRequired)
          } throw ENotClosedInterfaceRequires(ctx, ifaceName, required, requiredRequired)
          methods.values.foreach(checkIfaceMethod)
          choices.values.foreach(env.checkChoice(ifaceName, _))
          env.checkIfaceCoImplementations(ifaceName, param, coImplements)
      }

    private def checkIfaceMethod(method: InterfaceMethod): Unit = {
      checkType(method.returnType, KStar)
    }

    private def alphaEquiv(t1: Type, t2: Type) = // NICK: stack-safe?
      AlphaEquiv.alphaEquiv(t1, t2) ||
        AlphaEquiv.alphaEquiv(expandTypeSynonyms(t1), expandTypeSynonyms(t2))

    private def checkGenImplementation(
        tplTcon: TypeConName,
        ifaceTcon: TypeConName,
        implMethods: List[(MethodName, Expr)],
    ): Unit = {
      val DefInterfaceSignature(requires, _, _, methods, _, _) =
        // TODO https://github.com/digital-asset/daml/issues/14112
        handleLookup(ctx, pkgInterface.lookupInterface(ifaceTcon))

      requires
        .filterNot(required =>
          pkgInterface.lookupTemplateImplementsOrInterfaceCoImplements(tplTcon, required).isRight
        )
        .foreach(required => throw EMissingRequiredInterface(ctx, tplTcon, ifaceTcon, required))

      methods.values.foreach { (method: InterfaceMethod) =>
        if (!implMethods.exists { case (name, _) => name == method.name })
          throw EMissingInterfaceMethod(ctx, tplTcon, ifaceTcon, method.name)
      }
      implMethods.foreach { case (name, value) =>
        methods.get(name) match {
          case None =>
            throw EUnknownInterfaceMethod(ctx, tplTcon, ifaceTcon, name)
          case Some(method) =>
            legacy_checkExpr(value, method.returnType)
        }
      }
    }

    private def checkIfaceImplementation(
        tplTcon: TypeConName,
        impl: TemplateImplements,
    ): Unit = {
      val ifaceTcon = impl.interfaceId
      pkgInterface
        .lookupInterfaceCoImplements(tplTcon, ifaceTcon)
        .foreach(_ => throw EConflictingImplementsCoImplements(ctx, tplTcon, ifaceTcon))
      checkGenImplementation(
        tplTcon,
        ifaceTcon,
        impl.methods.values.map(TemplateImplementsMethod.unapply(_).value).toList,
      )
    }

    private def checkIfaceImplementations(
        tplTcon: TypeConName,
        impls: Map[TypeConName, TemplateImplements],
    ): Unit =
      impls.values.foreach(checkIfaceImplementation(tplTcon, _))

    private def checkIfaceCoImplementation(
        ifaceTcon: TypeConName,
        param: ExprVarName,
        coImpl: InterfaceCoImplements,
    ): Unit = {
      val tplTcon = coImpl.templateId
      pkgInterface
        .lookupTemplateImplements(tplTcon, ifaceTcon)
        .foreach(_ => throw EConflictingImplementsCoImplements(ctx, tplTcon, ifaceTcon))

      // Note (MA): we use an empty environment and add `param : TTyCon(tplTcon)`
      Env(languageVersion, pkgInterface, Context.DefInterfaceCoImplements(tplTcon, ifaceTcon))
        .introExprVar(param, TTyCon(tplTcon))
        .checkGenImplementation(
          tplTcon,
          ifaceTcon,
          coImpl.methods.values.map(InterfaceCoImplementsMethod.unapply(_).value).toList,
        )
    }

    private def checkIfaceCoImplementations(
        ifaceTcon: TypeConName,
        param: ExprVarName,
        coImpls: Map[TypeConName, InterfaceCoImplements],
    ): Unit =
      coImpls.values.foreach(checkIfaceCoImplementation(ifaceTcon, param, _))

    private[NewTyping] def checkDefException(
        excepName: TypeConName,
        defException: DefException,
    ): Unit = {
      legacy_checkExpr(defException.message, TTyCon(excepName) ->: TText)
      ()
    }

    private def checkTypConApp(app: TypeConApp): DataCons = app match {
      case TypeConApp(tyCon, tArgs) =>
        val DDataType(_, tparams, dataCons) = handleLookup(ctx, pkgInterface.lookupDataType(tyCon))
        if (tparams.length != tArgs.length) throw ETypeConAppWrongArity(ctx, tparams.length, app)
        (tArgs.iterator zip tparams.values).foreach((checkType _).tupled)
        TypeSubst.substitute((tparams.keys zip tArgs.iterator).toMap, dataCons)
    }

    private[NewTyping] def checkType(typ: Type, kind: Kind): Unit = {
      val typKind = kindOf(typ)
      if (kind != typKind)
        throw EKindMismatch(ctx, foundKind = typKind, expectedKind = kind)
    }

    private def kindOfDataType(defDataType: DDataType): Kind =
      defDataType.params.reverse.foldLeft[Kind](KStar) { case (acc, (_, k)) => KArrow(k, acc) }

    def kindOf(typ0: Type): Kind = typ0 match { // testing entry point

      case TSynApp(syn, args) =>
        val ty = expandSynApp(syn, args)
        checkType(ty, KStar)
        KStar
      case TVar(v) =>
        lookupTypeVar(v)
      case TNat(_) =>
        KNat
      case TTyCon(tycon) =>
        kindOfDataType(handleLookup(ctx, pkgInterface.lookupDataType(tycon)))
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

    private[lf] def expandTypeSynonyms(typ0: Type): Type = typ0 match {
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
      val DTypeSyn(tparams, replacementTyp) = handleLookup(ctx, pkgInterface.lookupTypeSyn(syn))
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
          (fieldExprs zip fieldTypes).foreach { case (e, f) => legacy_checkExpr(e, f) }
        case _ =>
          throw EExpectedRecordType(ctx, typ)
      }

    private def checkVariantCon(typ: TypeConApp, con: VariantConName, conArg: Expr): Unit =
      checkTypConApp(typ) match {
        case DataVariant(variantType) =>
          legacy_checkExpr(conArg, variantType.lookup(con, EUnknownVariantCon(ctx, con)))
          ()
        case _ =>
          throw EExpectedVariantType(ctx, typ.tycon)
      }

    private def checkEnumCon(typConName: TypeConName, con: EnumConName): Unit =
      handleLookup(ctx, pkgInterface.lookupDataType(typConName)).cons match {
        case DataEnum(enumType) =>
          if (!enumType.toSeq.contains(con)) throw EUnknownEnumCon(ctx, con)
        case _ =>
          throw EExpectedEnumType(ctx, typConName)
      }

    private def typeOfRecProj(typ0: TypeConApp, field: FieldName, record: Expr): Type =
      checkTypConApp(typ0) match {
        case DataRecord(recordType) =>
          val fieldType = recordType.lookup(field, EUnknownField(ctx, field))
          legacy_checkExpr(record, typeConAppToType(typ0))
          fieldType
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfRecUpd(typ0: TypeConApp, field: FieldName, record: Expr, update: Expr): Type =
      checkTypConApp(typ0) match {
        case DataRecord(recordType) =>
          val typ1 = typeConAppToType(typ0)
          legacy_checkExpr(record, typ1)
          legacy_checkExpr(update, recordType.lookup(field, EUnknownField(ctx, field)))
          typ1
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfStructCon(fields: ImmArray[(FieldName, Expr)]): Work[Type] =
      Ret(
        Struct
          .fromSeq(fields.iterator.map { case (f, x) => f -> recurse_typeOf(x) }.toSeq)
          .fold(name => throw EDuplicateField(ctx, name), TStruct)
      )

    private def typeOfStructProj(proj: EStructProj): Work[Type] =
      Ret(toStruct(recurse_typeOf(proj.struct)).fields.get(proj.field) match {
        case Some(typ) => typ
        case None => throw EUnknownField(ctx, proj.field)
      })

    private def typeOfStructUpd(upd: EStructUpd): Work[Type] = {
      val structType = toStruct(recurse_typeOf(upd.struct))
      structType.fields.get(upd.field) match {
        case Some(updateType) =>
          legacy_checkExpr(upd.update, updateType)
          Ret(structType)
        case None => throw EUnknownField(ctx, upd.field)
      }
    }

    private def typeOfTmApp(fun: Expr, arg: Expr): Work[Type] = {
      typeOf(fun) { ty =>
        val (argType, resType) = toFunction(ty)
        checkExpr(arg, argType) {
          Ret(resType)
        }
      }
    }

    private def typeOfTyApp(expr: Expr, typs: List[Type]): Work[Type] = {
      @tailrec
      def loopForall(body0: Type, typs: List[Type], acc: Map[TypeVarName, Type]): Type =
        typs match {
          case head :: tail =>
            toForall(body0) match {
              case TForall((v, k), body) =>
                checkType(head, k)
                loopForall(body, tail, acc.updated(v, head))
              case otherwise =>
                throw EExpectedUniversalType(ctx, otherwise)
            }
          case Nil =>
            TypeSubst.substitute(acc, body0)
        }

      Ret(loopForall(recurse_typeOf(expr), typs, Map.empty))
    }

    private def typeOfTmLam(x: ExprVarName, typ: Type, body: Expr): Work[Type] = {
      checkType(typ, KStar) // NICK, todo, or maybe not!
      introExprVar(x, typ).typeOf(body) { tyBody =>
        Ret(typ ->: tyBody)
      }
    }

    private def typeofTyLam(tVar: TypeVarName, kind: Kind, expr: Expr): Work[Type] = {
      checkKind(kind)
      introTypeVar(tVar, kind).typeOf(expr) { ty =>
        Ret(TForall(tVar -> kind, ty))
      }
    }

    private[this] def introPatternVariant(
        scrutTCon: TypeConName,
        scrutTArgs: ImmArray[Type],
        tparams: ImmArray[TypeVarName],
        cons: ImmArray[(VariantConName, Type)],
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
          val rank = handleLookup(ctx, pkgInterface.lookupVariantConstructor(tycon, variant)).rank
          SomeRanks(ranks + rank)
        case CPEnum(tycon, constructor) =>
          val rank = handleLookup(ctx, pkgInterface.lookupEnumConstructor(tycon, constructor))
          SomeRanks(ranks + rank)
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

    private[this] def typeOfCase(scrut: Expr, alts: ImmArray[CaseAlt]): Work[Type] = {
      val scrutType = recurse_typeOf(scrut)
      val (expectedPatterns, introPattern) = scrutType match {
        case TTyConApp(scrutTCon, scrutTArgs) =>
          handleLookup(ctx, pkgInterface.lookupDataType(scrutTCon)) match {
            case DDataType(_, dataParams, dataCons) =>
              dataCons match {
                case DataRecord(_) =>
                  (defaultExpectedPatterns, introOnlyPatternDefault(scrutType))
                case DataVariant(cons) =>
                  (
                    variantExpectedPatterns(scrutTCon, cons),
                    introPatternVariant(scrutTCon, scrutTArgs, dataParams.map(_._1), cons),
                  )
                case DataEnum(cons) =>
                  (
                    enumExpectedPatterns(scrutTCon, cons),
                    introPatternEnum(scrutTCon, cons),
                  )
                case DataInterface =>
                  (defaultExpectedPatterns, introOnlyPatternDefault(scrutType))
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

      val types = alts.iterator.map { case CaseAlt(patn, rhs) =>
        introPattern(patn).recurse_typeOf(rhs)
      }.toList

      types match {
        case t :: ts =>
          ts.foreach(otherType =>
            if (!alphaEquiv(t, otherType)) throw ETypeMismatch(ctx, otherType, t, None)
          )
          checkPatternExhaustiveness(expectedPatterns, alts, scrutType)
          Ret(t)
        case Nil =>
          throw EEmptyCase(ctx)
      }
    }

    private def typeOfLet(binding: Binding, body: Expr): Work[Type] = binding match {
      case Binding(Some(vName), typ0, expr) =>
        checkType(typ0, KStar)
        resolveExprType(expr, typ0) { typ1 =>
          introExprVar(vName, typ1).typeOf(body) { ty =>
            Ret(ty)
          }
        }
      case Binding(None, typ0, bound) =>
        checkType(typ0, KStar)
        resolveExprType(bound, typ0) { _ =>
          typeOf(body) { ty =>
            Ret(ty)
          }
        }
    }

    private[this] def typOfExprInterface(expr: ExprInterface): Type = expr match {
      case EToInterface(iface, tpl, value) =>
        checkImplements(tpl, iface)
        legacy_checkExpr(value, TTyCon(tpl))
        TTyCon(iface)
      case EFromInterface(iface, tpl, value) =>
        checkImplements(tpl, iface)
        legacy_checkExpr(value, TTyCon(iface))
        TOptional(TTyCon(tpl))
      case EUnsafeFromInterface(iface, tpl, cid, value) =>
        checkImplements(tpl, iface)
        legacy_checkExpr(cid, TContractId(TTyCon(iface)))
        legacy_checkExpr(value, TTyCon(iface))
        TTyCon(tpl)
      case EToRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
        val requiringIface = handleLookup(ctx, pkgInterface.lookupInterface(requiringIfaceId))
        if (!requiringIface.requires.contains(requiredIfaceId))
          throw EWrongInterfaceRequirement(ctx, requiringIfaceId, requiredIfaceId)
        legacy_checkExpr(body, TTyCon(requiringIfaceId))
        TTyCon(requiredIfaceId)
      case EFromRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
        val requiringIface = handleLookup(ctx, pkgInterface.lookupInterface(requiringIfaceId))
        if (!requiringIface.requires.contains(requiredIfaceId))
          throw EWrongInterfaceRequirement(ctx, requiringIfaceId, requiredIfaceId)
        legacy_checkExpr(body, TTyCon(requiredIfaceId))
        TOptional(TTyCon(requiringIfaceId))
      case EUnsafeFromRequiredInterface(requiredIfaceId, requiringIfaceId, cid, body) =>
        val requiringIface = handleLookup(ctx, pkgInterface.lookupInterface(requiringIfaceId))
        if (!requiringIface.requires.contains(requiredIfaceId))
          throw EWrongInterfaceRequirement(ctx, requiringIfaceId, requiredIfaceId)
        legacy_checkExpr(cid, TContractId(TTyCon(requiredIfaceId)))
        legacy_checkExpr(body, TTyCon(requiredIfaceId))
        TTyCon(requiringIfaceId)
      case ECallInterface(iface, methodName, value) =>
        val method = handleLookup(ctx, pkgInterface.lookupInterfaceMethod(iface, methodName))
        legacy_checkExpr(value, TTyCon(iface))
        method.returnType
      case EInterfaceTemplateTypeRep(ifaceId, body) =>
        discard(handleLookup(ctx, pkgInterface.lookupInterface(ifaceId)))
        legacy_checkExpr(body, TTyCon(ifaceId))
        TTypeRep
      case ESignatoryInterface(ifaceId, body) =>
        discard(handleLookup(ctx, pkgInterface.lookupInterface(ifaceId)))
        legacy_checkExpr(body, TTyCon(ifaceId))
        TList(TParty)
      case EObserverInterface(ifaceId, body) =>
        discard(handleLookup(ctx, pkgInterface.lookupInterface(ifaceId)))
        legacy_checkExpr(body, TTyCon(ifaceId))
        TList(TParty)
      case EViewInterface(ifaceId, expr) =>
        val iface = handleLookup(ctx, pkgInterface.lookupInterface(ifaceId))
        legacy_checkExpr(expr, TTyCon(ifaceId))
        iface.view
    }

    private def checkCons(elemType: Type, front: ImmArray[Expr], tailExpr: Expr): Unit = { // NICK: Work!
      checkType(elemType, KStar)
      if (front.isEmpty) throw EEmptyConsFront(ctx)
      front.foreach(legacy_checkExpr(_, elemType))
      legacy_checkExpr(tailExpr, TList(elemType))
      ()
    }

    private def checkPure(typ: Type, expr: Expr): Unit = {
      checkType(typ, KStar)
      legacy_checkExpr(expr, typ)
      ()
    }

    private def typeOfScenarioBlock(bindings: ImmArray[Binding], body: Expr): Type = {
      val env = bindings.foldLeft(this) { case (env, Binding(vName, typ, bound)) =>
        env.checkType(typ, KStar)
        env.legacy_checkExpr(bound, TScenario(typ))
        env.introExprVar(vName, typ)
      }
      toScenario(env.recurse_typeOf(body))
    }

    private def typeOfUpdateBlock(bindings: ImmArray[Binding], body: Expr): Type = {
      val env = bindings.foldLeft(this) { case (env, Binding(vName, typ, bound)) =>
        env.checkType(typ, KStar)
        env.legacy_checkExpr(bound, TUpdate(typ))
        env.introExprVar(vName, typ)
      }
      toUpdate(env.recurse_typeOf(body))
    }

    private def typeOfCreate(tpl: TypeConName, arg: Expr): Type = {
      discard(handleLookup(ctx, pkgInterface.lookupTemplate(tpl)))
      legacy_checkExpr(arg, TTyCon(tpl))
      TUpdate(TContractId(TTyCon(tpl)))
    }

    private def typeOfCreateInterface(iface: TypeConName, arg: Expr): Type = {
      discard(handleLookup(ctx, pkgInterface.lookupInterface(iface)))
      legacy_checkExpr(arg, TTyCon(iface))
      TUpdate(TContractId(TTyCon(iface)))
    }

    private def typeOfExercise(
        tpl: TypeConName,
        chName: ChoiceName,
        cid: Expr,
        arg: Expr,
    ): Type = {
      val choice = handleLookup(ctx, pkgInterface.lookupTemplateChoice(tpl, chName))
      legacy_checkExpr(cid, TContractId(TTyCon(tpl)))
      legacy_checkExpr(arg, choice.argBinder._2)
      TUpdate(choice.returnType)
    }

    private def typeOfExerciseInterface(
        interfaceId: TypeConName,
        chName: ChoiceName,
        cid: Expr,
        arg: Expr,
        guard: Option[Expr],
    ): Type = {
      legacy_checkExpr(cid, TContractId(TTyCon(interfaceId)))
      val choice = handleLookup(ctx, pkgInterface.lookupInterfaceChoice(interfaceId, chName))
      legacy_checkExpr(arg, choice.argBinder._2)
      guard.foreach(legacy_checkExpr(_, TFun(TTyCon(interfaceId), TBool)))
      TUpdate(choice.returnType)
    }

    private def typeOfExerciseByKey(
        tmplId: TypeConName,
        chName: ChoiceName,
        key: Expr,
        arg: Expr,
    ): Type = {
      checkByKey(tmplId, key)
      val choice = handleLookup(ctx, pkgInterface.lookupTemplateChoice(tmplId, chName))
      legacy_checkExpr(arg, choice.argBinder._2)
      TUpdate(choice.returnType)
    }

    private def typeOfFetchTemplate(tpl: TypeConName, cid: Expr): Type = {
      discard(handleLookup(ctx, pkgInterface.lookupTemplate(tpl)))
      legacy_checkExpr(cid, TContractId(TTyCon(tpl)))
      TUpdate(TTyCon(tpl))
    }

    private def typeOfFetchInterface(tpl: TypeConName, cid: Expr): Type = {
      discard(handleLookup(ctx, pkgInterface.lookupInterface(tpl)))
      legacy_checkExpr(cid, TContractId(TTyCon(tpl)))
      TUpdate(TTyCon(tpl))
    }

    private def checkImplements(tpl: TypeConName, iface: TypeConName): Unit = {
      discard(handleLookup(ctx, pkgInterface.lookupInterface(iface)))
      discard(handleLookup(ctx, pkgInterface.lookupTemplate(tpl)))
      if (pkgInterface.lookupTemplateImplementsOrInterfaceCoImplements(tpl, iface).isLeft) {
        throw ETemplateDoesNotImplementInterface(ctx, tpl, iface)
      }
    }

    private def checkByKey(tmplId: TypeConName, key: Expr): Unit = {
      val tmplKey = handleLookup(ctx, pkgInterface.lookupTemplateKey(tmplId))
      legacy_checkExpr(key, tmplKey.typ)
      ()
    }

    private def typeOfUpdate(update: Update): Type = update match { // NICK: Work!
      case UpdatePure(typ, expr) =>
        checkPure(typ, expr)
        TUpdate(typ)
      case UpdateBlock(bindings, body) =>
        typeOfUpdateBlock(bindings, body)
      case UpdateCreate(tpl, arg) =>
        typeOfCreate(tpl, arg)
      case UpdateCreateInterface(iface, arg) =>
        typeOfCreateInterface(iface, arg)
      case UpdateExercise(tpl, choice, cid, arg) =>
        typeOfExercise(tpl, choice, cid, arg)
      case UpdateExerciseInterface(tpl, choice, cid, arg, guard) =>
        typeOfExerciseInterface(tpl, choice, cid, arg, guard)
      case UpdateExerciseByKey(tpl, choice, key, arg) =>
        typeOfExerciseByKey(tpl, choice, key, arg)
      case UpdateFetchTemplate(tpl, cid) =>
        typeOfFetchTemplate(tpl, cid)
      case UpdateFetchInterface(tpl, cid) =>
        typeOfFetchInterface(tpl, cid)
      case UpdateGetTime =>
        TUpdate(TTimestamp)
      case UpdateEmbedExpr(typ, exp) =>
        legacy_checkExpr(exp, TUpdate(typ))
        TUpdate(typ)
      case UpdateFetchByKey(retrieveByKey) =>
        checkByKey(retrieveByKey.templateId, retrieveByKey.key)
        // fetches return the contract id and the contract itself
        TUpdate(
          TStruct(
            Struct.assertFromSeq(
              List(
                contractIdFieldName -> TContractId(TTyCon(retrieveByKey.templateId)),
                contractFieldName -> TTyCon(retrieveByKey.templateId),
              )
            )
          )
        )
      case UpdateLookupByKey(retrieveByKey) =>
        checkByKey(retrieveByKey.templateId, retrieveByKey.key)
        TUpdate(TOptional(TContractId(TTyCon(retrieveByKey.templateId))))
      case UpdateTryCatch(typ, body, binder, handler) =>
        checkType(typ, KStar)
        val updTyp = TUpdate(typ)
        legacy_checkExpr(body, updTyp)
        introExprVar(binder, TAnyException).legacy_checkExpr(handler, TOptional(updTyp))
        updTyp
    }

    private def typeOfCommit(typ: Type, party: Expr, update: Expr): Work[Type] = {
      checkType(typ, KStar)
      checkExpr(party, TParty) {
        checkExpr(update, TUpdate(typ)) {
          Ret(TScenario(typ))
        }
      }
    }

    private def typeOfMustFailAt(typ: Type, party: Expr, update: Expr): Work[Type] = {
      checkType(typ, KStar)
      checkExpr(party, TParty) {
        checkExpr(update, TUpdate(typ)) {
          Ret(TScenario(TUnit))
        }
      }
    }

    private def typeOfScenario(scenario: Scenario): Work[Type] = scenario match {
      case ScenarioPure(typ, expr) =>
        checkPure(typ, expr)
        Ret(TScenario(typ))
      case ScenarioBlock(bindings, body) =>
        Ret(typeOfScenarioBlock(bindings, body))
      case ScenarioCommit(party, update, typ) =>
        typeOfCommit(typ, party, update)
      case ScenarioMustFailAt(party, update, typ) =>
        typeOfMustFailAt(typ, party, update)
      case ScenarioPass(delta) =>
        checkExpr(delta, TInt64) {
          Ret(TScenario(TTimestamp))
        }
      case ScenarioGetTime =>
        Ret(TScenario(TTimestamp))
      case ScenarioGetParty(name) =>
        checkExpr(name, TText) {
          Ret(TScenario(TParty))
        }
      case ScenarioEmbedExpr(typ, exp) =>
        resolveExprType(exp, TScenario(typ)) { ty =>
          Ret(ty)
        }
    }

    // checks that typ contains neither variables, nor quantifiers, nor synonyms
    private def checkAnyType_(typ: Type): Unit = {
      // No expansion here because we forbid TSynApp
      typ match {
        case TVar(_) | TForall(_, _) | TSynApp(_, _) =>
          throw EExpectedAnyType(ctx, typ)
        case _ =>
          TypeIterable(typ).foreach(checkAnyType_)
      }
    }

    private def checkAnyType(typ: Type): Unit = {
      checkAnyType_(typ)
      checkType(typ, KStar)
    }

    private def checkExceptionType(typ: Type): Unit = typ match {
      case TTyCon(tyCon) =>
        discard(handleLookup(ctx, pkgInterface.lookupException(tyCon)))
      case _ => throw EExpectedExceptionType(ctx, typ)
    }

    private def typeOfAtomic(expr: ExprAtomic): Work[Type] = expr match {
      case EVar(name) =>
        Ret(lookupExpVar(name))
      case EVal(ref) =>
        Ret(handleLookup(ctx, pkgInterface.lookupValue(ref)).typ)
      case EBuiltin(fun) =>
        Ret(typeOfBuiltinFunction(fun))
      case EPrimCon(con) =>
        Ret(typeOfPRimCon(con))
      case EPrimLit(lit) =>
        Ret(typeOfPrimLit(lit))
      case EEnumCon(tyCon, constructor) =>
        checkEnumCon(tyCon, constructor)
        Ret(TTyCon(tyCon))
      case ENil(typ) =>
        checkType(typ, KStar)
        Ret(TList(typ))
      case ENone(typ) =>
        checkType(typ, KStar)
        Ret(TOptional(typ))
    }

    private def work_typeOf(e: Expr): Work[Type] = e match { // NICK: loose "work_" prefix
      case expr: ExprAtomic =>
        typeOfAtomic(expr)
      case ERecCon(tycon, fields) =>
        checkRecCon(tycon, fields)
        Ret(typeConAppToType(tycon))
      case ERecProj(tycon, field, record) =>
        Ret(typeOfRecProj(tycon, field, record))
      case ERecUpd(tycon, field, record, update) =>
        Ret(typeOfRecUpd(tycon, field, record, update))
      case EVariantCon(tycon, variant, arg) =>
        checkVariantCon(tycon, variant, arg)
        Ret(typeConAppToType(tycon))
      case EStructCon(fields) =>
        typeOfStructCon(fields)
      case proj: EStructProj =>
        typeOfStructProj(proj)
      case upd: EStructUpd =>
        typeOfStructUpd(upd)
      case EApp(fun, arg) =>
        typeOfTmApp(fun, arg)
      case ETyApp(expr0, typ) =>
        // Typechecking multiple applications in one go allows us to
        // only substitute once which is a bit faster.
        val (expr, typs) = destructETyApp(expr0, List(typ))
        typeOfTyApp(expr, typs)
      case EAbs((varName, typ), body, _) =>
        typeOfTmLam(varName, typ, body)
      case ETyAbs((vName, kind), body) =>
        typeofTyLam(vName, kind, body)
      case ECase(scruct, alts) =>
        typeOfCase(scruct, alts)
      case ELet(binding, body) =>
        typeOfLet(binding, body)
      case ECons(typ, front, tail) =>
        checkCons(typ, front, tail)
        Ret(TList(typ))
      case EUpdate(update) =>
        Ret(typeOfUpdate(update))
      case EScenario(scenario) =>
        typeOfScenario(scenario)
      case ELocation(loc, expr) =>
        val _ = Ret(newLocation(loc).recurse_typeOf(expr)) // orig
        // newLocation(loc).typeOf(expr) { ty => Ret(ty) } //NICK: this! but...
        ??? // NICK: damm, no callers in TypingSpec
      case ESome(typ, body) =>
        checkType(typ, KStar) // NICK: or is this known to be stack-safe?
        checkExpr(body, typ) {
          Ret(TOptional(typ))
        }
      case EToAny(typ, body) =>
        checkAnyType(typ)
        checkExpr(body, typ) {
          Ret(TAny)
        }
      case EFromAny(typ, body) =>
        checkAnyType(typ)
        checkExpr(body, TAny) {
          Ret(TOptional(typ))
        }
      case ETypeRep(typ) =>
        checkAnyType(typ)
        Ret(TTypeRep)
      case EThrow(returnTyp, excepTyp, body) =>
        checkType(returnTyp, KStar)
        checkExceptionType(excepTyp)
        checkExpr(body, excepTyp) {
          Ret(returnTyp)
        }
      case EToAnyException(typ, value) =>
        checkExceptionType(typ)
        checkExpr(value, typ) {
          Ret(TAnyException)
        }
      case EFromAnyException(typ, value) =>
        checkExceptionType(typ)
        checkExpr(value, TAnyException) {
          Ret(TOptional(typ))
        }
      case expr: ExprInterface =>
        Ret(typOfExprInterface(expr))
      case EExperimental(_, typ) =>
        Ret(typ)
    }

    private def resolveExprType[T](expr: Expr, typ: Type)(k: Type => Work[T]): Work[T] = {
      typeOf(expr) { exprType =>
        if (!alphaEquiv(exprType, typ))
          throw ETypeMismatch(ctx, foundType = exprType, expectedType = typ, expr = Some(expr))
        k(exprType)
      }
    }

    private def checkExpr[T](expr: Expr, typ0: Type)(work: Work[T]): Work[T] = {
      resolveExprType(expr, typ0) { _ =>
        work
      }
    }

    private def toStruct(t: Type): TStruct =
      t match {
        case s: TStruct => s
        case _ =>
          expandTypeSynonyms(t) match {
            case s: TStruct => s
            case _ => throw EExpectedStructType(ctx, t)
          }
      }

    private def toFunction(t: Type): (Type, Type) =
      t match {
        case TApp(TApp(TBuiltin(BTArrow), argType), resType) => (argType, resType)
        case _ =>
          expandTypeSynonyms(t) match {
            case TApp(TApp(TBuiltin(BTArrow), argType), resType) => (argType, resType)
            case _ => throw EExpectedFunctionType(ctx, t)
          }
      }

    private def toScenario(t: Type): Type =
      t match {
        case s @ TScenario(_) => s
        case _ =>
          expandTypeSynonyms(t) match {
            case s @ TScenario(_) => s
            case _ => throw EExpectedScenarioType(ctx, t)
          }
      }

    private def toForall(t: Type): TForall =
      t match {
        case f @ TForall(_, _) => f
        case _ =>
          expandTypeSynonyms(t) match {
            case f @ TForall(_, _) => f
            case _ => throw EExpectedUniversalType(ctx, t)
          }
      }

    private def toUpdate(t: Type): Type =
      t match {
        case t @ TUpdate(_) => t
        case _ =>
          expandTypeSynonyms(t) match {
            case t @ TUpdate(_) => t
            case _ => throw EExpectedUpdateType(ctx, t)
          }
      }
  }

  /* Utils */

  private implicit final class TypeOp(val rightType: Type) extends AnyVal {
    private[NewTyping] def ->:(leftType: Type) = TFun(leftType, rightType)
  }

  private def typeConAppToType(app: TypeConApp): Type = app match {
    case TypeConApp(tcon, targs) => targs.foldLeft[Type](TTyCon(tcon))(TApp)
  }

  private[this] class ExpectedPatterns(val number: Int, patterns: => Iterator[CasePat]) {
    private[NewTyping] def missingPatterns(ranks: Set[Int]): List[CasePat] =
      patterns.zipWithIndex.collect { case (p, i) if !ranks(i) => p }.toList
  }
  private[this] object ExpectedPatterns {
    private[NewTyping] def apply(patterns: CasePat*) =
      new ExpectedPatterns(patterns.length, patterns.iterator)
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
