// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.validation

import com.daml.lf.data.{ImmArray, Numeric, Struct}
import com.daml.lf.data.TemplateOrInterface
import com.daml.lf.data.Ref._
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{LanguageVersion, PackageInterface, Reference}
import com.daml.lf.validation.Util._
import com.daml.lf.validation.iterable.TypeIterable
import com.daml.scalautil.Statement.discard

import scala.annotation.tailrec

private[validation] object Typing {

  // stack-safety achieved via a Work trampoline.
  private sealed abstract class Work[A]
  private object Work {
    final case class Ret[A](v: A) extends Work[A]
    final case class Delay[A](thunk: () => Work[A]) extends Work[A]
    final case class Bind[A, X](work: Work[X], k: X => Work[A]) extends Work[A]
  }

  import Work.{Ret, Delay, Bind}

  private def sequenceWork[A, T](works: List[Work[T]])(k: List[T] => Work[A]): Work[A] = {
    def loop(acc: List[T], works: List[Work[T]]): Work[A] = {
      works match {
        case Nil => k(acc.reverse)
        case work :: works =>
          Bind(work, { x: T => loop(x :: acc, works) })
      }
    }
    loop(Nil, works)
  }

  private def runWork[R](work: Work[R]): R = {
    // calls to runWork must never be nested
    @tailrec
    def loop[A](work: Work[A]): A = work match {
      case Ret(v) => v
      case Delay(thunk) => loop(thunk())
      case Bind(work, k) =>
        loop(work match {
          case Ret(x) => k(x)
          case Delay(thunk) => Bind(thunk(), k)
          case Bind(work1, k1) => Bind(work1, ((x: Any) => Bind(k1(x), k)))
        })
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

  private def typeOfBuiltinLit(lit: BuiltinLit): Type = lit match {
    case BLInt64(_) => TInt64
    case BLNumeric(s) => TNumeric(TNat(Numeric.scale(s)))
    case BLText(_) => TText
    case BLTimestamp(_) => TTimestamp
    case BLDate(_) => TDate
    case BLRoundingMode(_) => TRoundingMode
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
          TForall(
            gamma.name -> KNat,
            TNumeric(gamma) ->: TNumeric(alpha) ->: TNumeric(beta) ->: TNumeric(gamma),
          ),
        ),
      )
    val tNumConversion =
      TForall(
        alpha.name -> KNat,
        TForall(beta.name -> KNat, TNumeric(beta) ->: TNumeric(alpha) ->: TNumeric(beta)),
      )
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
      BInt64ToNumeric -> TForall(
        alpha.name -> KNat,
        TNumeric(alpha) ->: TInt64 ->: TNumeric(alpha),
      ),
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
      BTimestampToText -> (TTimestamp ->: TText),
      BPartyToText -> (TParty ->: TText),
      BDateToText -> (TDate ->: TText),
      BContractIdToText -> TForall(alpha.name -> KStar, TContractId(alpha) ->: TOptional(TText)),
      BSHA256Text -> (TText ->: TText),
      BPartyToQuotedText -> (TParty ->: TText),
      BCodePointsToText -> (TList(TInt64) ->: TText),
      BTextToParty -> (TText ->: TOptional(TParty)),
      BTextToInt64 -> (TText ->: TOptional(TInt64)),
      BTextToNumericLegacy -> TForall(alpha.name -> KNat, TText ->: TOptional(TNumeric(alpha))),
      BTextToNumeric -> TForall(
        alpha.name -> KNat,
        TNumeric(alpha) ->: TText ->: TOptional(TNumeric(alpha)),
      ),
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
      BBigNumericToNumeric -> TForall(
        alpha.name -> KNat,
        TNumeric(alpha) ->: TBigNumeric ->: TNumeric(alpha),
      ),
      BNumericToBigNumeric -> TForall(alpha.name -> KNat, TNumeric(alpha) ->: TBigNumeric),
      BBigNumericToText -> (TBigNumeric ->: TText),
      // Exception functions
      BAnyExceptionMessage -> (TAnyException ->: TText),
      // TypeRep functions
      BTypeRepTyConName -> (TTypeRep ->: TOptional(TText)),
    )
  }

  private def typeOfPRimCon(con: BuiltinCon): Type = con match {
    case BCTrue => TBool
    case BCFalse => TBool
    case BCUnit => TUnit
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
            env.checkRecordTypeTop(fields)
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

    private[lf] def kindOf(typ: Type): Kind = { // testing entry point
      // must *NOT* be used for sub-types
      runWork(kindOfType(typ))
    }

    private[Typing] def checkType(typ: Type, kind: Kind): Unit = {
      // must *NOT* be used for sub-types
      runWork(nestedCheckType(typ, kind) { Ret(()) })
    }

    // continuation style is for convenience of caller
    private def typeOf[T](e: Expr)(k: Type => Work[T]): Work[T] = {
      // stack-safe type-computation for sub-expressions
      Bind(Delay(() => typeOfExpr(e)), k)
    }

    private def checkTopExpr(expr: Expr, typ: Type): Unit = {
      // must *NOT* be used for sub-expressions
      val exprType = typeOfTopExpr(expr)
      if (!alphaEquiv(exprType, typ))
        throw ETypeMismatch(ctx, foundType = exprType, expectedType = typ, expr = Some(expr))
    }

    private[lf] def typeOfTopExpr(exp: Expr): Type = { // testing entry point
      // stack-safe type-computation for TOP-LEVEL expressions
      // must *NOT* be used for sub-expressions
      runWork(typeOfExpr(exp))
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

    private[Typing] def checkVariantType(variants: ImmArray[(VariantConName, Type)]): Unit = {
      checkUniq[VariantConName](variants.keys, EDuplicateVariantCon(ctx, _))
      variants.values.foreach(checkType(_, KStar))
    }

    private[Typing] def checkEnumType[X](
        tyConName: => TypeConName,
        params: ImmArray[X],
        values: ImmArray[EnumConName],
    ): Unit = {
      if (params.nonEmpty) throw EIllegalHigherEnumType(ctx, tyConName)
      checkUniq[Name](values.iterator, EDuplicateEnumCon(ctx, _))
    }

    private[Typing] def checkInterfaceType[X](
        tyConName: => TypeConName,
        params: ImmArray[X],
    ): Unit = {
      if (params.nonEmpty) throw EIllegalHigherInterfaceType(ctx, tyConName)
      val _ = handleLookup(ctx, pkgInterface.lookupInterface(tyConName))
    }

    private[Typing] def checkDValue(dfn: DValue): Unit = dfn match {
      case DValue(typ, body, isTest) =>
        checkType(typ, KStar)
        checkTopExpr(body, typ)
        if (isTest) {
          discard(toScenario(dropForalls(typ)))
        }
    }

    @tailrec
    private def dropForalls(typ0: Type): Type = typ0 match {
      case TForall(_, typ) => dropForalls(typ)
      case _ => typ0
    }

    private[Typing] def checkRecordTypeTop(fields: ImmArray[(FieldName, Type)]): Unit = {
      // must *NOT* be used when nested with a type
      runWork(checkRecordType(fields) { Ret(()) })
    }

    private def checkChoice(tplName: TypeConName, choice: TemplateChoice): Unit =
      choice match {
        case TemplateChoice(
              name @ _,
              consuming @ _,
              controllers,
              choiceObservers,
              choiceAuthorizers,
              selfBinder,
              (param, paramType),
              returnType,
              update,
            ) =>
          checkType(paramType, KStar)
          checkType(returnType, KStar)
          introExprVar(param, paramType).checkTopExpr(controllers, TParties)
          choiceObservers.foreach(
            introExprVar(param, paramType).checkTopExpr(_, TParties)
          )
          choiceAuthorizers.foreach(
            introExprVar(param, paramType).checkTopExpr(_, TParties)
          )
          introExprVar(selfBinder, TContractId(TTyCon(tplName)))
            .introExprVar(param, paramType)
            .checkTopExpr(update, TUpdate(returnType))
          ()
      }

    private[Typing] def checkTemplate(tplName: TypeConName, template: Template): Unit = {
      val Template(
        param,
        precond,
        signatories,
        choices,
        observers,
        mbKey,
        implementations,
      ) =
        template
      val env = introExprVar(param, TTyCon(tplName))
      env.checkTopExpr(precond, TBool)
      env.checkTopExpr(signatories, TParties)
      env.checkTopExpr(observers, TParties)
      choices.values.foreach(env.checkChoice(tplName, _))
      implementations.values.foreach { impl =>
        checkInterfaceInstance(
          tmplParam = param,
          interfaceId = impl.interfaceId,
          templateId = tplName,
          iiBody = impl.body,
        )
      }
      mbKey.foreach { key =>
        checkType(key.typ, KStar)
        env.checkTopExpr(key.body, key.typ)
        checkTopExpr(key.maintainers, TFun(key.typ, TParties))
        ()
      }
    }

    private[Typing] def checkDefIface(ifaceName: TypeConName, iface: DefInterface): Unit =
      iface match {
        case DefInterface(requires, param, choices, methods, view) =>
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
          view match {
            case TTyCon(tycon) => {
              val DDataType(_, args, dataCon) =
                handleLookup(env.ctx, pkgInterface.lookupDataType(tycon))
              if (args.length > 0)
                throw EViewTypeHasVars(env.ctx, view)
              dataCon match {
                case DataRecord(_) =>
                case _ =>
                  throw EViewTypeConNotRecord(env.ctx, dataCon, view)
              }
            }
            case TApp(_, _) =>
              throw EViewTypeHasVars(env.ctx, view)
            case _ =>
              throw EViewTypeHeadNotCon(env.ctx, view, view)
          }
      }

    private def checkIfaceMethod(method: InterfaceMethod): Unit = {
      checkType(method.returnType, KStar)
    }

    private def alphaEquiv(t1: Type, t2: Type) =
      AlphaEquiv.alphaEquiv(t1, t2) ||
        AlphaEquiv.alphaEquiv(expandTypeSynonyms(t1), expandTypeSynonyms(t2))

    private def checkInterfaceInstance(
        interfaceId: TypeConName,
        templateId: TypeConName,
    ): Unit = {
      discard(handleLookup(ctx, pkgInterface.lookupInterface(interfaceId)))
      discard(handleLookup(ctx, pkgInterface.lookupInterfaceInstance(interfaceId, templateId)))
    }

    private def checkInterfaceInstance(
        tmplParam: ExprVarName,
        interfaceId: TypeConName,
        templateId: TypeConName,
        iiBody: InterfaceInstanceBody,
    ): Unit = {
      val ctx = Context.Reference(Reference.InterfaceInstance(interfaceId, templateId))
      val DefInterfaceSignature(requires, _, _, methods, view) =
        handleLookup(ctx, pkgInterface.lookupInterface(interfaceId))

      // Note (MA): we use an empty environment and add `tmplParam : TTyCon(templateId)`
      val env = Env(languageVersion, pkgInterface, ctx)
        .introExprVar(tmplParam, TTyCon(templateId))

      requires
        .foreach(required =>
          if (pkgInterface.lookupInterfaceInstance(required, templateId).isLeft)
            throw EMissingRequiredInterfaceInstance(
              ctx,
              interfaceId,
              Reference.InterfaceInstance(required, templateId),
            )
        )

      methods.values.foreach { (method: InterfaceMethod) =>
        if (!iiBody.methods.exists { case (name, _) => name == method.name })
          throw EMissingMethodInInterfaceInstance(ctx, method.name)
      }
      iiBody.methods.values.foreach { case InterfaceInstanceMethod(name, value) =>
        methods.get(name) match {
          case None =>
            throw EUnknownMethodInInterfaceInstance(ctx, name, interfaceId, templateId)
          case Some(method) =>
            try env.checkTopExpr(value, method.returnType)
            catch {
              case e: ETypeMismatch =>
                throw EMethodTypeMismatch(
                  e.context,
                  interfaceId,
                  templateId,
                  name,
                  e.foundType,
                  e.expectedType,
                  e.expr,
                )
            }
        }
      }

      try env.checkTopExpr(iiBody.view, view)
      catch {
        case e: ETypeMismatch =>
          throw EViewTypeMismatch(
            e.context,
            interfaceId,
            templateId,
            e.foundType,
            e.expectedType,
            e.expr,
          )
      }
    }

    private[Typing] def checkDefException(
        excepName: TypeConName,
        defException: DefException,
    ): Unit = {
      checkTopExpr(defException.message, TTyCon(excepName) ->: TText)
      ()
    }

    private def checkTypConApp(app: TypeConApp): DataCons = app match {
      case TypeConApp(tyCon, tArgs) =>
        val DDataType(_, tparams, dataCons) = handleLookup(ctx, pkgInterface.lookupDataType(tyCon))
        if (tparams.length != tArgs.length) throw ETypeConAppWrongArity(ctx, tparams.length, app)
        (tArgs.iterator zip tparams.values).foreach((checkType _).tupled)
        TypeSubst.substitute((tparams.keys zip tArgs.iterator).toMap, dataCons)
    }

    private def nestedCheckType[T](typ: Type, kind: Kind)(work: => Work[T]): Work[T] = {
      nestedKindOf(typ) { typKind =>
        if (kind != typKind) {
          throw EKindMismatch(ctx, foundKind = typKind, expectedKind = kind)
        }
        work
      }
    }

    private def nestedKindOf[T](typ: Type)(k: Kind => Work[T]): Work[T] = {
      Bind(Delay(() => kindOfType(typ)), k)
    }

    private def kindOfDataType(defDataType: DDataType): Kind =
      defDataType.params.reverse.foldLeft[Kind](KStar) { case (acc, (_, k)) => KArrow(k, acc) }

    private def kindOfType(typ0: Type): Work[Kind] = typ0 match {
      case TSynApp(syn, args) =>
        val ty = expandSynApp(syn, args)
        nestedCheckType(ty, KStar) {
          Ret(KStar)
        }
      case TVar(v) =>
        Ret(lookupTypeVar(v))
      case TNat(_) =>
        Ret(KNat)
      case TTyCon(tycon) =>
        Ret(kindOfDataType(handleLookup(ctx, pkgInterface.lookupDataType(tycon))))
      case TApp(tFun, tArg) =>
        nestedKindOf(tFun) {
          case KStar | KNat => throw EExpectedHigherKind(ctx, KStar)
          case KArrow(argKind, resKind) =>
            nestedCheckType(tArg, argKind) {
              Ret(resKind)
            }
        }
      case TBuiltin(bType) =>
        Ret(kindOfBuiltin(bType))
      case TForall((v, k), b) =>
        checkKind(k)
        introTypeVar(v, k).nestedCheckType(b, KStar) {
          Ret(KStar)
        }
      case TStruct(fields) =>
        checkRecordType(fields.toImmArray) {
          Ret(KStar)
        }
    }

    private def checkRecordType[T](
        fields: ImmArray[(FieldName, Type)]
    )(work: => Work[T]): Work[T] = {
      checkUniq[FieldName](fields.keys, EDuplicateField(ctx, _))
      sequenceWork(fields.values.toList.map { ty =>
        nestedCheckType(ty, KStar) {
          Ret(())
        }
      }) { _ =>
        work
      }
    }

    // TODO https://github.com/digital-asset/daml/issues/13410 -- make expandTypeSynonyms be stack-safe
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

    private def checkRecCon[T](typ: TypeConApp, recordExpr: ImmArray[(FieldName, Expr)])(
        work: => Work[T]
    ): Work[T] =
      checkTypConApp(typ) match {
        case DataRecord(recordType) =>
          val (exprFieldNames, fieldExprs) = recordExpr.unzip
          val (typeFieldNames, fieldTypes) = recordType.unzip
          if (exprFieldNames != typeFieldNames) throw EFieldMismatch(ctx, typ, recordExpr)
          checkExprList((fieldExprs zip fieldTypes).toList) {
            work
          }
        case _ =>
          throw EExpectedRecordType(ctx, typ)
      }

    private def checkVariantCon[T](typ: TypeConApp, con: VariantConName, conArg: Expr)(
        work: => Work[T]
    ): Work[T] =
      checkTypConApp(typ) match {
        case DataVariant(variantType) =>
          checkExpr(conArg, variantType.lookup(con, EUnknownVariantCon(ctx, con))) {
            work
          }
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

    private def typeOfRecProj(typ0: TypeConApp, field: FieldName, record: Expr): Work[Type] =
      checkTypConApp(typ0) match {
        case DataRecord(recordType) =>
          val typ1 = typeConAppToType(typ0)
          val fieldType = recordType.lookup(field, EUnknownField(ctx, field, typ1))
          checkExpr(record, typ1) {
            Ret(fieldType)
          }
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfRecUpd(
        typ0: TypeConApp,
        field: FieldName,
        record: Expr,
        update: Expr,
    ): Work[Type] =
      checkTypConApp(typ0) match {
        case DataRecord(recordType) =>
          val typ1 = typeConAppToType(typ0)
          checkExpr(record, typ1) {
            try
              checkExpr(update, recordType.lookup(field, EUnknownField(ctx, field, typ1))) {
                Ret(typ1)
              }
            catch {
              case e: ETypeMismatch =>
                throw EFieldTypeMismatch(
                  ctx,
                  fieldName = field,
                  targetRecord = typ1,
                  foundType = e.foundType,
                  expectedType = e.expectedType,
                  expr = e.expr,
                )
            }
          }
        case _ =>
          throw EExpectedRecordType(ctx, typ0)
      }

    private def typeOfStructCon(fields: ImmArray[(FieldName, Expr)]): Work[Type] = {
      sequenceWork(fields.map { case (f, x) =>
        typeOf(x) { ty =>
          Ret(f -> ty)
        }
      }.toList) { xs =>
        Ret(Struct.fromSeq(xs).fold(name => throw EDuplicateField(ctx, name), TStruct))
      }
    }

    private def typeOfStructProj(proj: EStructProj): Work[Type] =
      typeOf(proj.struct) { ty =>
        toStruct(ty).fields.get(proj.field) match {
          case Some(typ) => Ret(typ)
          case None => throw EUnknownField(ctx, proj.field, ty)
        }
      }

    private def typeOfStructUpd(upd: EStructUpd): Work[Type] =
      typeOf(upd.struct) { ty =>
        val structType = toStruct(ty)
        structType.fields.get(upd.field) match {
          case Some(updateType) =>
            checkExpr(upd.update, updateType) {
              Ret(structType)
            }
          case None => throw EUnknownField(ctx, upd.field, ty)
        }
      }

    private def typeOfTmApp(fun: Expr, arg: Expr): Work[Type] =
      typeOf(fun) { ty =>
        val (argType, resType) = toFunction(ty)
        checkExpr(arg, argType) {
          Ret(resType)
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

      typeOf(expr) { ty =>
        Ret(loopForall(ty, typs, Map.empty))
      }
    }

    private def typeOfTmLam(x: ExprVarName, typ: Type, body: Expr): Work[Type] = {
      checkType(typ, KStar)
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
        case CPBuiltinCon(pc) =>
          pc match {
            case BCFalse | BCUnit => SomeRanks(ranks + 1)
            case BCTrue => SomeRanks(ranks + 0)
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

    private[this] def typeOfCase(scrut: Expr, alts: ImmArray[CaseAlt]): Work[Type] =
      typeOf(scrut) { scrutType =>
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

        sequenceWork(alts.map { case CaseAlt(patn, rhs) =>
          introPattern(patn).typeOf(rhs) { ty => Ret(ty) }
        }.toList) {
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

    private[this] def typOfExprInterface(expr: ExprInterface): Work[Type] = expr match {
      case EToInterface(iface, tpl, value) =>
        checkInterfaceInstance(iface, tpl)
        checkExpr(value, TTyCon(tpl)) {
          Ret(TTyCon(iface))
        }
      case EFromInterface(iface, tpl, value) =>
        checkInterfaceInstance(iface, tpl)
        checkExpr(value, TTyCon(iface)) {
          Ret(TOptional(TTyCon(tpl)))
        }
      case EUnsafeFromInterface(iface, tpl, cid, value) =>
        checkInterfaceInstance(iface, tpl)
        checkExpr(cid, TContractId(TTyCon(iface))) {
          checkExpr(value, TTyCon(iface)) {
            Ret(TTyCon(tpl))
          }
        }
      case EToRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
        val requiringIface = handleLookup(ctx, pkgInterface.lookupInterface(requiringIfaceId))
        if (!requiringIface.requires.contains(requiredIfaceId))
          throw EWrongInterfaceRequirement(ctx, requiringIfaceId, requiredIfaceId)
        checkExpr(body, TTyCon(requiringIfaceId)) {
          Ret(TTyCon(requiredIfaceId))
        }
      case EFromRequiredInterface(requiredIfaceId, requiringIfaceId, body) =>
        val requiringIface = handleLookup(ctx, pkgInterface.lookupInterface(requiringIfaceId))
        if (!requiringIface.requires.contains(requiredIfaceId))
          throw EWrongInterfaceRequirement(ctx, requiringIfaceId, requiredIfaceId)
        checkExpr(body, TTyCon(requiredIfaceId)) {
          Ret(TOptional(TTyCon(requiringIfaceId)))
        }
      case EUnsafeFromRequiredInterface(requiredIfaceId, requiringIfaceId, cid, body) =>
        val requiringIface = handleLookup(ctx, pkgInterface.lookupInterface(requiringIfaceId))
        if (!requiringIface.requires.contains(requiredIfaceId))
          throw EWrongInterfaceRequirement(ctx, requiringIfaceId, requiredIfaceId)
        checkExpr(cid, TContractId(TTyCon(requiredIfaceId))) {
          checkExpr(body, TTyCon(requiredIfaceId)) {
            Ret(TTyCon(requiringIfaceId))
          }
        }
      case ECallInterface(iface, methodName, value) =>
        val method = handleLookup(ctx, pkgInterface.lookupInterfaceMethod(iface, methodName))
        checkExpr(value, TTyCon(iface)) {
          Ret(method.returnType)
        }
      case EInterfaceTemplateTypeRep(ifaceId, body) =>
        discard(handleLookup(ctx, pkgInterface.lookupInterface(ifaceId)))
        checkExpr(body, TTyCon(ifaceId)) {
          Ret(TTypeRep)
        }
      case ESignatoryInterface(ifaceId, body) =>
        discard(handleLookup(ctx, pkgInterface.lookupInterface(ifaceId)))
        checkExpr(body, TTyCon(ifaceId)) {
          Ret(TList(TParty))
        }
      case EObserverInterface(ifaceId, body) =>
        discard(handleLookup(ctx, pkgInterface.lookupInterface(ifaceId)))
        checkExpr(body, TTyCon(ifaceId)) {
          Ret(TList(TParty))
        }
      case EViewInterface(ifaceId, expr) =>
        val iface = handleLookup(ctx, pkgInterface.lookupInterface(ifaceId))
        checkExpr(expr, TTyCon(ifaceId)) {
          Ret(iface.view)
        }
    }

    private def checkCons[T](elemType: Type, front: ImmArray[Expr], tailExpr: Expr)(
        work: => Work[T]
    ): Work[T] = {
      checkType(elemType, KStar)
      if (front.isEmpty) throw EEmptyConsFront(ctx)
      checkExprList(front.toList.map(x => (x, elemType))) {
        checkExpr(tailExpr, TList(elemType)) {
          work
        }
      }
    }

    private def checkPure[T](typ: Type, expr: Expr)(work: => Work[T]): Work[T] = {
      checkType(typ, KStar)
      checkExpr(expr, typ) {
        work
      }
    }

    private def typeOfScenarioBlock(bindings: ImmArray[Binding], body: Expr): Work[Type] = {
      def loop(env: Env, bindings0: List[Binding]): Work[Type] = bindings0 match {
        case Binding(vName, typ, bound) :: bindings =>
          env.checkType(typ, KStar)
          env.checkExpr(bound, TScenario(typ)) {
            loop(env.introExprVar(vName, typ), bindings)
          }
        case Nil =>
          env.typeOf(body) { ty =>
            Ret(toScenario(ty))
          }
      }
      loop(this, bindings.toList)
    }

    private def typeOfUpdateBlock(bindings: ImmArray[Binding], body: Expr): Work[Type] = {
      def loop(env: Env, bindings0: List[Binding]): Work[Type] = bindings0 match {
        case Binding(vName, typ, bound) :: bindings =>
          env.checkType(typ, KStar)
          env.checkExpr(bound, TUpdate(typ)) {
            loop(env.introExprVar(vName, typ), bindings)
          }
        case Nil =>
          env.typeOf(body) { ty =>
            Ret(toUpdate(ty))
          }
      }
      loop(this, bindings.toList)
    }

    private def typeOfCreate(tpl: TypeConName, arg: Expr): Work[Type] = {
      discard(handleLookup(ctx, pkgInterface.lookupTemplate(tpl)))
      checkExpr(arg, TTyCon(tpl)) {
        Ret(TUpdate(TContractId(TTyCon(tpl))))
      }
    }

    private def typeOfCreateInterface(iface: TypeConName, arg: Expr): Work[Type] = {
      discard(handleLookup(ctx, pkgInterface.lookupInterface(iface)))
      checkExpr(arg, TTyCon(iface)) {
        Ret(TUpdate(TContractId(TTyCon(iface))))
      }
    }

    private def typeOfExercise(
        tpl: TypeConName,
        chName: ChoiceName,
        cid: Expr,
        arg: Expr,
    ): Work[Type] = {
      val choice = handleLookup(ctx, pkgInterface.lookupTemplateChoice(tpl, chName))
      checkExpr(cid, TContractId(TTyCon(tpl))) {
        checkExpr(arg, choice.argBinder._2) {
          Ret(TUpdate(choice.returnType))
        }
      }
    }

    private def typeOfExerciseInterface(
        interfaceId: TypeConName,
        chName: ChoiceName,
        cid: Expr,
        arg: Expr,
        guard: Option[Expr],
    ): Work[Type] = {
      checkExpr(cid, TContractId(TTyCon(interfaceId))) {
        val choice = handleLookup(ctx, pkgInterface.lookupInterfaceChoice(interfaceId, chName))
        checkExpr(arg, choice.argBinder._2) {
          guard match {
            case None =>
              Ret(TUpdate(choice.returnType))
            case Some(guard) =>
              checkExpr(guard, TFun(TTyCon(interfaceId), TBool)) {
                Ret(TUpdate(choice.returnType))
              }
          }
        }
      }
    }

    private def typeOfExerciseByKey(
        tmplId: TypeConName,
        chName: ChoiceName,
        key: Expr,
        arg: Expr,
    ): Work[Type] = {
      checkByKey(tmplId, key) {
        val choice = handleLookup(ctx, pkgInterface.lookupTemplateChoice(tmplId, chName))
        checkExpr(arg, choice.argBinder._2) {
          Ret(TUpdate(choice.returnType))
        }
      }
    }

    private def typeOfFetchTemplate(tpl: TypeConName, cid: Expr): Work[Type] = {
      discard(handleLookup(ctx, pkgInterface.lookupTemplate(tpl)))
      checkExpr(cid, TContractId(TTyCon(tpl))) {
        Ret(TUpdate(TTyCon(tpl)))
      }
    }

    private def typeOfSoftFetchTemplate(tpl: TypeConName, cid: Expr): Work[Type] = {
      discard(handleLookup(ctx, pkgInterface.lookupTemplate(tpl)))
      pkgInterface.lookupTemplateKey(tpl) match {
        case Right(_) => throw ESoftFetchTemplateWithKey(ctx, tpl)
        case Left(_) =>
      }
      checkExpr(cid, TContractId(TTyCon(tpl))) {
        Ret(TUpdate(TTyCon(tpl)))
      }
    }

    private def typeOfFetchInterface(tpl: TypeConName, cid: Expr): Work[Type] = {
      discard(handleLookup(ctx, pkgInterface.lookupInterface(tpl)))
      checkExpr(cid, TContractId(TTyCon(tpl))) {
        Ret(TUpdate(TTyCon(tpl)))
      }
    }

    private def checkByKey[T](tmplId: TypeConName, key: Expr)(work: => Work[T]): Work[T] = {
      val tmplKey = handleLookup(ctx, pkgInterface.lookupTemplateKey(tmplId))
      checkExpr(key, tmplKey.typ) {
        work
      }
    }

    private def typeOfUpdate(update: Update): Work[Type] = update match {
      case UpdatePure(typ, expr) =>
        checkPure(typ, expr) {
          Ret(TUpdate(typ))
        }
      case UpdateBlock(bindings, body) =>
        typeOfUpdateBlock(bindings, body)
      case UpdateCreate(tpl, arg) =>
        typeOfCreate(tpl, arg)
      case UpdateCreateInterface(iface, arg) =>
        typeOfCreateInterface(iface, arg)
      case UpdateExercise(tpl, choice, cid, arg) =>
        typeOfExercise(tpl, choice, cid, arg)
      case UpdateSoftExercise(tpl, choice, cid, arg) =>
        // TODO: https://github.com/digital-asset/daml/issues/16151
        // want typeOfSoftExercise
        typeOfExercise(tpl, choice, cid, arg)
      case UpdateDynamicExercise(tpl, choice, cid, arg) =>
        typeOfExercise(tpl, choice, cid, arg)
      case UpdateExerciseInterface(tpl, choice, cid, arg, guard) =>
        typeOfExerciseInterface(tpl, choice, cid, arg, guard)
      case UpdateExerciseByKey(tpl, choice, key, arg) =>
        typeOfExerciseByKey(tpl, choice, key, arg)
      case UpdateFetchTemplate(tpl, cid) =>
        typeOfFetchTemplate(tpl, cid)
      case UpdateSoftFetchTemplate(tpl, cid) =>
        typeOfSoftFetchTemplate(tpl, cid)
      case UpdateFetchInterface(tpl, cid) =>
        typeOfFetchInterface(tpl, cid)
      case UpdateGetTime =>
        Ret(TUpdate(TTimestamp))
      case UpdateEmbedExpr(typ, exp) =>
        checkExpr(exp, TUpdate(typ)) {
          Ret(TUpdate(typ))
        }
      case UpdateFetchByKey(retrieveByKey) =>
        checkByKey(retrieveByKey.templateId, retrieveByKey.key) {
          // fetches return the contract id and the contract itself
          val ty = TUpdate(
            TStruct(
              Struct.assertFromSeq(
                List(
                  contractIdFieldName -> TContractId(TTyCon(retrieveByKey.templateId)),
                  contractFieldName -> TTyCon(retrieveByKey.templateId),
                )
              )
            )
          )
          Ret(ty)
        }
      case UpdateLookupByKey(retrieveByKey) =>
        checkByKey(retrieveByKey.templateId, retrieveByKey.key) {
          Ret(TUpdate(TOptional(TContractId(TTyCon(retrieveByKey.templateId)))))
        }
      case UpdateTryCatch(typ, body, binder, handler) =>
        checkType(typ, KStar)
        val updTyp = TUpdate(typ)
        checkExpr(body, updTyp) {
          introExprVar(binder, TAnyException).checkExpr(handler, TOptional(updTyp)) {
            Ret(updTyp)
          }
        }
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
        checkPure(typ, expr) {
          Ret(TScenario(typ))
        }
      case ScenarioBlock(bindings, body) =>
        typeOfScenarioBlock(bindings, body)
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
      case EBuiltinFun(fun) =>
        Ret(typeOfBuiltinFunction(fun))
      case EBuiltinCon(con) =>
        Ret(typeOfPRimCon(con))
      case EBuiltinLit(lit) =>
        Ret(typeOfBuiltinLit(lit))
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

    private[Typing] def typeOfChoiceControllerOrObserver(
        ty: TypeConName,
        choiceName: ChoiceName,
        contract: Expr,
        choiceArg: Expr,
    ): Work[Type] = {
      val choice = handleLookup(ctx, pkgInterface.lookupTemplateOrInterface(ty)) match {
        case TemplateOrInterface.Template(_) =>
          handleLookup(ctx, pkgInterface.lookupTemplateChoice(ty, choiceName))
        case TemplateOrInterface.Interface(_) =>
          handleLookup(ctx, pkgInterface.lookupInterfaceChoice(ty, choiceName))
      }
      checkExpr(contract, TTyCon(ty)) {
        checkExpr(choiceArg, choice.argBinder._2) {
          Ret(TParties)
        }
      }
    }

    private[Typing] def typeOfExpr(e: Expr): Work[Type] = e match {
      case expr: ExprAtomic =>
        typeOfAtomic(expr)
      case ERecCon(tycon, fields) =>
        checkRecCon(tycon, fields) {
          Ret(typeConAppToType(tycon))
        }
      case ERecProj(tycon, field, record) =>
        typeOfRecProj(tycon, field, record)
      case ERecUpd(tycon, field, record, update) =>
        typeOfRecUpd(tycon, field, record, update)
      case EVariantCon(tycon, variant, arg) =>
        checkVariantCon(tycon, variant, arg) {
          Ret(typeConAppToType(tycon))
        }
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
        checkCons(typ, front, tail) {
          Ret(TList(typ))
        }
      case EUpdate(update) =>
        typeOfUpdate(update)
      case EScenario(scenario) =>
        typeOfScenario(scenario)
      case ELocation(loc, expr) =>
        newLocation(loc).typeOf(expr) { ty =>
          Ret(ty)
        }
      case ESome(typ, body) =>
        checkType(typ, KStar)
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
      case EChoiceController(ty, ch, expr1, expr2) =>
        typeOfChoiceControllerOrObserver(ty, ch, expr1, expr2)
      case EChoiceObserver(ty, ch, expr1, expr2) =>
        typeOfChoiceControllerOrObserver(ty, ch, expr1, expr2)
      case expr: ExprInterface =>
        typOfExprInterface(expr)
      case EExperimental(_, typ) =>
        Ret(typ)
    }

    private def resolveExprType[T](expr: Expr, typ: Type)(k: Type => Work[T]): Work[T] = {
      typeOf(expr) { exprType =>
        if (!alphaEquiv(exprType, typ))
          expr match {
            case e: ERecProj =>
              throw EFieldTypeMismatch(
                ctx,
                fieldName = e.field,
                targetRecord = typeConAppToType(e.tycon),
                foundType = exprType,
                expectedType = typ,
                expr = Some(expr),
              )
            case _ =>
              throw ETypeMismatch(ctx, foundType = exprType, expectedType = typ, expr = Some(expr))
          }
        k(exprType)
      }
    }

    private def checkExpr[T](expr: Expr, typ0: Type)(work: => Work[T]): Work[T] = {
      resolveExprType(expr, typ0) { _ =>
        work
      }
    }

    private def checkExprList[T](xs: List[(Expr, Type)])(work: => Work[T]): Work[T] = {
      sequenceWork(xs.map { case (e, f) =>
        checkExpr(e, f) {
          Ret(())
        }
      }) { _ =>
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
    private[Typing] def ->:(leftType: Type) = TFun(leftType, rightType)
  }

  private def typeConAppToType(app: TypeConApp): Type = app match {
    case TypeConApp(tcon, targs) => targs.foldLeft[Type](TTyCon(tcon))(TApp)
  }

  private[this] class ExpectedPatterns(val number: Int, patterns: => Iterator[CasePat]) {
    private[Typing] def missingPatterns(ranks: Set[Int]): List[CasePat] =
      patterns.zipWithIndex.collect { case (p, i) if !ranks(i) => p }.toList
  }
  private[this] object ExpectedPatterns {
    private[Typing] def apply(patterns: CasePat*) =
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
