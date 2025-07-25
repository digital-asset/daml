// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.archive
package testing

import com.digitalasset.daml.lf.archive.{DamlLf2 => PLF}
import com.digitalasset.daml.lf.archive.testing.Encode.{EncodeError, expect}
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{LanguageVersion => LV, Util => AstUtil}

import scala.Ordering.Implicits.infixOrderingOps
import scala.annotation.tailrec
import scala.collection.mutable
import scala.language.implicitConversions

// Important: do not use this in production code. It is designed for testing only.
@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
private[daml] class EncodeV2(minorLanguageVersion: LV.Minor) {

  import EncodeV2._
  import Name.ordering

  private val languageVersion = LV(LV.Major.V2, minorLanguageVersion)

  def encodePackage(pkgId: PackageId, pkg: Package): PLF.Package =
    new PackageEncoder(pkgId).encode(pkg)

  private[this] class PackageEncoder(selfPkgId: PackageId) {

    private[this] val stringsTable =
      new EncodeV2.TableBuilder[String, String] {
        override def toProto(x: String): String = x
      }
    private[this] val dottedNameTable =
      new EncodeV2.TableBuilder[DottedName, PLF.InternedDottedName] {
        override def toProto(dottedName: DottedName): PLF.InternedDottedName = {
          val builder = PLF.InternedDottedName.newBuilder()
          dottedName.segments.foreach { segment =>
            builder.addSegmentsInternedStr(stringsTable.insert(segment))
            ()
          }
          builder.build()
        }
      }
    private[this] val typeTable = new EncodeV2.TableBuilder[Type, PLF.Type] {
      override def toProto(typ: Type): PLF.Type =
        encodeTypeBuilder(typ).build()
    }

    def encode(pkg: Package): PLF.Package = {
      val builder = PLF.Package.newBuilder()
      pkg.modules.sortByKey.values.foreach(m => builder.addModules(encodeModule(m)))

      val metadataBuilder = PLF.PackageMetadata.newBuilder
      metadataBuilder.setNameInternedStr(stringsTable.insert(pkg.metadata.name))
      metadataBuilder.setVersionInternedStr(stringsTable.insert(pkg.metadata.version.toString))
      pkg.metadata.upgradedPackageId match {
        case None =>
        case Some(pid) =>
          metadataBuilder.setUpgradedPackageId(
            PLF.UpgradedPackageId.newBuilder
              .setUpgradedPackageIdInternedStr(stringsTable.insert(pid))
              .build
          )
      }
      builder.setMetadata(metadataBuilder.build)

      typeTable.build.foreach(builder.addInternedTypes)
      dottedNameTable.build.foreach(builder.addInternedDottedNames)
      stringsTable.build.foreach(builder.addInternedStrings)

      builder.build()
    }

    private[this] def encodeModule(module: Module): PLF.Module = {

      def addDefinition(
          builder: PLF.Module.Builder,
          nameWithDefinition: (DottedName, Definition),
      ): PLF.Module.Builder = {
        val (name, definition) = nameWithDefinition
        definition match {
          case dataType: DDataType =>
            builder.addDataTypes(name -> dataType)
          case value: DValue =>
            builder.addValues(name -> value)
          case synonym: DTypeSyn =>
            builder.addSynonyms(name -> synonym)
        }
        builder
      }

      val builder = PLF.Module.newBuilder()
      setDottedName_(module.name, builder.setNameInternedDname)
      builder.setFlags(
        PLF.FeatureFlags
          .newBuilder()
          .setForbidPartyLiterals(true)
          .setDontDivulgeContractIdsInCreateArguments(true)
          .setDontDiscloseNonConsumingChoicesToObservers(true)
      )
      builder.accumulateLeft(module.definitions.sortByKey)(addDefinition)
      builder.accumulateLeft(module.templates.sortByKey) { (a, b) => a.addTemplates(b) }
      builder.accumulateLeft(module.exceptions.sortByKey) { (a, b) => a.addExceptions(b) }
      builder.accumulateLeft(module.interfaces.sortByKey) { (a, b) => a.addInterfaces(b) }
      builder.build()
    }

    /** * Encode Reference **
      */
    private val unit = PLF.Unit.newBuilder().build()

    private val protoSelfPgkId =
      PLF.SelfOrImportedPackageId.newBuilder().setSelfPackageId(unit).build()

    private implicit def encodePackageId(pkgId: PackageId): PLF.SelfOrImportedPackageId =
      if (pkgId == selfPkgId)
        protoSelfPgkId
      else {
        val builder = PLF.SelfOrImportedPackageId.newBuilder()
        setString(pkgId, builder.setImportedPackageIdInternedStr)
        builder.build()
      }

    private implicit def encodeModuleId(modId: (PackageId, ModuleName)): PLF.ModuleId = {
      val (pkgId, modName) = modId
      val builder = PLF.ModuleId.newBuilder()
      builder.setPackageId(pkgId)
      setDottedName_(modName, builder.setModuleNameInternedDname)
      builder.build()
    }

    private implicit def encodeTypeConId(identifier: Identifier): PLF.TypeConId = {
      val builder = PLF.TypeConId.newBuilder()
      builder.setModule(identifier.moduleRef)
      setDottedName_(identifier.name, builder.setNameInternedDname)
      builder.build()
    }

    private implicit def encodeTypeSynId(identifier: Identifier): PLF.TypeSynId = {
      val builder = PLF.TypeSynId.newBuilder()
      builder.setModule(identifier.moduleRef)
      setDottedName_(identifier.name, builder.setNameInternedDname)
      builder.build()
    }

    private implicit def encodeValId(identifier: Identifier): PLF.ValueId = {
      val b = PLF.ValueId.newBuilder()
      b.setModule(identifier.moduleRef)
      setDottedName(identifier.name, b.setNameInternedDname)
      b.build()
    }

    /** * Encoding of Kinds **
      */
    private val kStar =
      PLF.Kind.newBuilder().setStar(PLF.Unit.newBuilder()).build()
    private val kNat =
      PLF.Kind.newBuilder().setNat(PLF.Unit.newBuilder()).build()
    private val KArrows = RightRecMatcher[Kind, Kind]({ case KArrow(param, result) =>
      (param, result)
    })

    private implicit def encodeKind(k: Kind): PLF.Kind =
      // KArrows breaks the exhaustiveness checker.
      (k: @unchecked) match {
        case KArrows(params, result) =>
          expect(result == KStar)
          PLF.Kind
            .newBuilder()
            .setArrow(
              PLF.Kind.Arrow
                .newBuilder()
                .accumulateLeft(params)(_ addParams encodeKind(_))
                .setResult(kStar)
            )
            .build()
        case KStar =>
          kStar
        case KNat =>
          kNat
      }

    /** * Encoding of types **
      */
    private val builtinTypeInfoMap =
      DecodeV2.builtinTypeInfos
        .filter(info => info.minVersion <= languageVersion)
        .map(info => info.bTyp -> info)
        .toMap

    @inline
    private implicit def encodeTypeBinder(binder: (String, Kind)): PLF.TypeVarWithKind = {
      val (varName, kind) = binder
      val b = PLF.TypeVarWithKind.newBuilder()
      setString(varName, b.setVarInternedStr)
      b.setKind(kind)
      b.build()
    }

    @inline
    private implicit def encodeFieldWithType(nameWithType: (String, Type)): PLF.FieldWithType = {
      val (name, typ) = nameWithType
      val b = PLF.FieldWithType.newBuilder()
      setString(name, b.setFieldInternedStr)
      b.setType(typ).build()
    }

    private val TForalls = RightRecMatcher[(TypeVarName, Kind), Type]({
      case TForall(binder, body) => binder -> body
    })
    private val TApps = LeftRecMatcher[Type, Type]({ case TApp(fun, arg) =>
      fun -> arg
    })

    private implicit def encodeType(typ: Type): PLF.Type =
      PLF.Type.newBuilder().setInterned(typeTable.insert(typ)).build()

    private def encodeTypeBuilder(typ0: Type): PLF.Type.Builder = {
      val (typ, args) =
        typ0 match {
          case TApps(typ1, args1) => typ1 -> args1
          case _ => typ0 -> ImmArray.Empty
        }
      val builder = PLF.Type.newBuilder()
      // Be warned: Both the use of the unapply pattern TForalls and the pattern
      //    case TBuiltin(BTArrow) if versionIsOlderThan(LV.Features.arrowType) =>
      // cause scala's exhaustivty checking to be disabled in the following match.
      (typ: @unchecked) match {
        case TVar(varName) =>
          val b = PLF.Type.Var.newBuilder()
          setString(varName, b.setVarInternedStr)
          args.foldLeft(b)(_ addArgs _)
          builder.setVar(b)
        case TNat(n) =>
          builder.setNat(n.toLong)
        case TTyCon(tycon) =>
          builder.setCon(
            PLF.Type.Con.newBuilder().setTycon(tycon).accumulateLeft(args)(_ addArgs _)
          )
        case TBuiltin(bType) =>
          val (proto, typs) =
            builtinTypeInfoMap(bType).proto -> args
          builder.setBuiltin(
            PLF.Type.Builtin.newBuilder().setBuiltin(proto).accumulateLeft(typs)(_ addArgs _)
          )
        case TApp(_, _) =>
          sys.error("unexpected error")
        case TForalls(binders, body) =>
          expect(args.isEmpty)
          builder.setForall(
            PLF.Type.Forall.newBuilder().accumulateLeft(binders)(_ addVars _).setBody(body)
          )
        case TStruct(fields) =>
          expect(args.isEmpty)
          builder.setStruct(
            PLF.Type.Struct.newBuilder().accumulateLeft(fields.toImmArray)(_ addFields _)
          )
        case TSynApp(name, args) =>
          val b = PLF.Type.Syn.newBuilder()
          b.setTysyn(name)
          b.accumulateLeft(args)(_ addArgs _)
          builder.setSyn(b)
      }
    }

    /** * Encoding Expression **
      */
    private val builtinFunctionInfos =
      DecodeV2.builtinFunctionInfos
        .filter(info =>
          info.minVersion <= languageVersion && info.maxVersion.forall(languageVersion < _)
        )

    private val directBuiltinFunctionMap =
      builtinFunctionInfos
        .filter(_.implicitParameters.isEmpty)
        .map(info => info.builtin -> info)
        .toMap

    private val indirectBuiltinFunctionMap =
      builtinFunctionInfos
        .filter(_.implicitParameters.nonEmpty)
        .groupBy(_.builtin)
        .transform((_, infos) => infos.map(info => info.implicitParameters -> info).toMap)

    @inline
    private implicit def encodeBuiltins(builtinFunction: BuiltinFunction): PLF.BuiltinFunction =
      directBuiltinFunctionMap(builtinFunction).proto

    private implicit def encodeTyConApp(tyCon: TypeConApp): PLF.Type.Con =
      PLF.Type.Con
        .newBuilder()
        .setTycon(tyCon.tycon)
        .accumulateLeft(tyCon.args)(_ addArgs _)
        .build()

    @inline
    private implicit def encodeFieldWithExpr(fieldWithExpr: (Name, Expr)): PLF.FieldWithExpr = {
      val (name, expr) = fieldWithExpr
      val b = PLF.FieldWithExpr.newBuilder()
      setString(name, b.setFieldInternedStr)
      b.setExpr(expr).build()
    }

    @inline
    private implicit def encodeExprBinder(binder: (String, Type)): PLF.VarWithType = {
      val (varName, typ) = binder
      val b = PLF.VarWithType.newBuilder()
      setString(varName, b.setVarInternedStr)
      b.setType(typ).build()
    }

    private implicit def encodeLocation(loc: Location): PLF.Location = {
      val Location(packageId, module, definition @ _, (startLine, startCol), (endLine, endCol)) =
        loc
      PLF.Location
        .newBuilder()
        .setModule(packageId -> module)
        .setRange(
          PLF.Location.Range
            .newBuilder()
            .setStartLine(startLine)
            .setStartCol(startCol)
            .setEndLine(endLine)
            .setEndCol(endCol)
        )
        .build()
    }

    private implicit def encodeExpr(expr0: Expr): PLF.Expr =
      encodeExprBuilder(expr0).build()

    private implicit def encodeBinding(binding: Binding): PLF.Binding =
      PLF.Binding
        .newBuilder()
        .setBinder(binding.binder.getOrElse("") -> binding.typ)
        .setBound(binding.bound)
        .build()

    private implicit def encodeRetrieveByKey(tmplId: TypeConId): PLF.Update.RetrieveByKey =
      PLF.Update.RetrieveByKey.newBuilder().setTemplate(tmplId).build()

    private implicit def encodeUpdate(upd0: Update): PLF.Update = {
      val builder = PLF.Update.newBuilder()
      upd0 match {
        case UpdatePure(typ, expr) =>
          builder.setPure(PLF.Pure.newBuilder().setType(typ).setExpr(expr))
        case UpdateBlock(binding, body) =>
          builder.setBlock(
            PLF.Block.newBuilder().accumulateLeft(binding)(_ addBindings _).setBody(body)
          )
        case UpdateCreate(templateId, arg) =>
          builder.setCreate(PLF.Update.Create.newBuilder().setTemplate(templateId).setExpr(arg))
        case UpdateCreateInterface(interface, arg) =>
          builder.setCreateInterface(
            PLF.Update.CreateInterface.newBuilder().setInterface(interface).setExpr(arg)
          )
        case UpdateFetchTemplate(templateId, contractId) =>
          builder.setFetch(PLF.Update.Fetch.newBuilder().setTemplate(templateId).setCid(contractId))
        case UpdateFetchInterface(interface, contractId) =>
          builder.setFetchInterface(
            PLF.Update.FetchInterface.newBuilder().setInterface(interface).setCid(contractId)
          )
        case UpdateExercise(templateId, choice, cid, arg) =>
          val b = PLF.Update.Exercise.newBuilder()
          b.setTemplate(templateId)
          setString(choice, b.setChoiceInternedStr)
          b.setCid(cid)
          b.setArg(arg)
          builder.setExercise(b)
        case UpdateExerciseInterface(interface, choice, cid, arg, guard) =>
          val b = PLF.Update.ExerciseInterface.newBuilder()
          b.setInterface(interface)
          setInternedString(choice, b.setChoiceInternedStr)
          b.setCid(cid)
          b.setArg(arg)
          guard.foreach { g =>
            assertSince(LV.Features.extendedInterfaces, "ExerciseInterface.guard")
            b.setGuard(g)
          }
          builder.setExerciseInterface(b)
        case UpdateExerciseByKey(templateId, choice, key, arg) =>
          val b = PLF.Update.ExerciseByKey.newBuilder()
          b.setTemplate(templateId)
          b.setChoiceInternedStr(stringsTable.insert(choice))
          b.setKey(key)
          b.setArg(arg)
          builder.setExerciseByKey(b)
        case UpdateGetTime =>
          builder.setGetTime(unit)
        case UpdateLedgerTimeLT(time) =>
          builder.setLedgerTimeLt(time)
        case UpdateFetchByKey(rbk) =>
          builder.setFetchByKey(rbk)
        case UpdateLookupByKey(rbk) =>
          builder.setLookupByKey(rbk)
        case UpdateEmbedExpr(typ, body) =>
          builder.setEmbedExpr(PLF.Update.EmbedExpr.newBuilder().setType(typ).setBody(body))
        case UpdateTryCatch(retTy, tryExpr, binder, catchExpr) =>
          val b = PLF.Update.TryCatch.newBuilder()
          b.setReturnType(retTy)
          b.setTryExpr(tryExpr)
          b.setVarInternedStr(stringsTable.insert(binder))
          b.setCatchExpr(catchExpr)
          builder.setTryCatch(b)
      }
      builder.build()
    }

    private implicit def encodeBuiltinCon(builtinCon: BuiltinCon): PLF.BuiltinCon =
      builtinCon match {
        case BCTrue => PLF.BuiltinCon.CON_TRUE
        case BCFalse => PLF.BuiltinCon.CON_FALSE
        case BCUnit => PLF.BuiltinCon.CON_UNIT
      }

    private implicit def encodeBuiltinLit(builtinLit: BuiltinLit): PLF.BuiltinLit = {
      val builder = PLF.BuiltinLit.newBuilder()
      builtinLit match {
        case BLInt64(value) =>
          builder.setInt64(value)
        case BLNumeric(value) =>
          builder.setNumericInternedStr(stringsTable.insert(Numeric.toString(value)))
        case BLText(value) =>
          setString(value, builder.setTextInternedStr)
        case BLTimestamp(value) =>
          builder.setTimestamp(value.micros)
        case BLDate(date) =>
          builder.setDate(date.days)
        case BLRoundingMode(rounding) =>
          builder.setRoundingModeValue(rounding.ordinal())
        case BLFailureCategory(category) =>
          builder.setFailureCategory(category match {
            case FCInvalidIndependentOfSystemState =>
              PLF.BuiltinLit.FailureCategory.INVALID_INDEPENDENT_OF_SYSTEM_STATE
            case FCInvalidGivenCurrentSystemStateOther =>
              PLF.BuiltinLit.FailureCategory.INVALID_GIVEN_CURRENT_SYSTEM_STATE_OTHER
          })
      }
      builder.build()
    }

    private implicit def encodeCaseAlternative(alt: CaseAlt): PLF.CaseAlt = {
      val builder = PLF.CaseAlt.newBuilder().setBody(alt.expr)
      alt.pattern match {
        case CPVariant(tyCon, variant, binder) =>
          val b = PLF.CaseAlt.Variant.newBuilder()
          b.setCon(tyCon)
          setString(variant, b.setVariantInternedStr)
          setString(binder, b.setBinderInternedStr)
          builder.setVariant(b)
        case CPEnum(tyCon, con) =>
          val b = PLF.CaseAlt.Enum.newBuilder()
          b.setCon(tyCon)
          setString(con, b.setConstructorInternedStr)
          builder.setEnum(b)
        case CPBuiltinCon(builtinCon) =>
          builder.setBuiltinCon(builtinCon)
        case CPNil =>
          builder.setNil(unit)
        case CPCons(head, tail) =>
          val b = PLF.CaseAlt.Cons.newBuilder()
          setString(head, b.setVarHeadInternedStr)
          setString(tail, b.setVarTailInternedStr)
          builder.setCons(b)
        case CPNone =>
          builder.setOptionalNone(unit)
        case CPSome(x) =>
          val b = PLF.CaseAlt.OptionalSome.newBuilder()
          setString(x, b.setVarBodyInternedStr)
          builder.setOptionalSome(b)
        case CPDefault =>
          builder.setDefault(unit)
      }
      builder.build()
    }

    private val EApps = LeftRecMatcher[Expr, Expr]({ case EApp(fun, arg) =>
      fun -> arg
    })
    private val ETyApps = LeftRecMatcher[Expr, Type]({ case ETyApp(exp, typ) =>
      exp -> typ
    })
    private val EAbss = RightRecMatcher[(ExprVarName, Type), Expr]({ case EAbs(binder, body) =>
      binder -> body
    })
    private val ETyAbss = RightRecMatcher[(TypeVarName, Kind), Expr]({ case ETyAbs(binder, body) =>
      binder -> body
    })

    private def encodeExprBuilder(
        expr0: Expr,
        builder: PLF.Expr.Builder = PLF.Expr.newBuilder(),
    ): builder.type = {

      // EAbss breaks the exhaustiveness checker.
      (expr0: @unchecked) match {
        case EVar(value) =>
          setString(value, builder.setVarInternedStr)
        case EVal(value) =>
          builder.setVal(value)
        case EBuiltinFun(value) =>
          builder.setBuiltin(value)
        case EBuiltinCon(builtinCon) =>
          builder.setBuiltinCon(builtinCon)
        case EBuiltinLit(builtinLit) =>
          builder.setBuiltinLit(builtinLit)
        case ERecCon(tyCon, fields) =>
          builder.setRecCon(
            PLF.Expr.RecCon.newBuilder().setTycon(tyCon).accumulateLeft(fields)(_ addFields _)
          )
        case ERecProj(tycon, field, expr) =>
          val b = PLF.Expr.RecProj.newBuilder()
          b.setTycon(tycon)
          setString(field, b.setFieldInternedStr)
          b.setRecord(expr)
          builder.setRecProj(b)
        case ERecUpd(tyCon, field, expr, update) =>
          val b = PLF.Expr.RecUpd.newBuilder()
          b.setTycon(tyCon)
          setString(field, b.setFieldInternedStr)
          b.setRecord(expr)
          b.setUpdate(update)
          builder.setRecUpd(b)
        case EVariantCon(tycon, variant, arg) =>
          val b = PLF.Expr.VariantCon.newBuilder()
          b.setTycon(tycon)
          setString(variant, b.setVariantConInternedStr)
          b.setVariantArg(arg)
          builder.setVariantCon(b)
        case EEnumCon(tyCon, con) =>
          val b = PLF.Expr.EnumCon.newBuilder().setTycon(tyCon)
          setString(con, b.setEnumConInternedStr)
          builder.setEnumCon(b.build())
        case EStructCon(fields) =>
          builder.setStructCon(
            PLF.Expr.StructCon.newBuilder().accumulateLeft(fields)(_ addFields _)
          )
        case EStructProj(field, expr) =>
          val b = PLF.Expr.StructProj.newBuilder()
          setString(field, b.setFieldInternedStr)
          b.setStruct(expr)
          builder.setStructProj(b)
        case EStructUpd(field, struct, update) =>
          val b = PLF.Expr.StructUpd.newBuilder()
          setString(field, b.setFieldInternedStr)
          b.setStruct(struct)
          b.setUpdate(update)
          builder.setStructUpd(b)
        case EApps(fun, args) =>
          builder.setApp(PLF.Expr.App.newBuilder().setFun(fun).accumulateLeft(args)(_ addArgs _))
        case ETyApps(expr: Expr, typs0) =>
          expr match {
            case EBuiltinFun(builtin) if indirectBuiltinFunctionMap.contains(builtin) =>
              val typs = typs0.toSeq.toList
              builder.setBuiltin(indirectBuiltinFunctionMap(builtin)(typs).proto)
            case _ =>
              builder.setTyApp(
                PLF.Expr.TyApp.newBuilder().setExpr(expr).accumulateLeft(typs0)(_ addTypes _)
              )
          }
        case ETyApps(expr, typs) =>
          builder.setTyApp(
            PLF.Expr.TyApp.newBuilder().setExpr(expr).accumulateLeft(typs)(_ addTypes _)
          )
        case EAbss(binders, body) =>
          builder.setAbs(
            PLF.Expr.Abs.newBuilder().accumulateLeft(binders)(_ addParam _).setBody(body)
          )
        case ETyAbss(binders, body) =>
          builder.setTyAbs(
            PLF.Expr.TyAbs.newBuilder().accumulateLeft(binders)(_ addParam _).setBody(body)
          )
        case ECase(scrut, alts) =>
          builder.setCase(PLF.Case.newBuilder().setScrut(scrut).accumulateLeft(alts)(_ addAlts _))
        case ELet(binding, body) =>
          builder.setLet(
            PLF.Block
              .newBuilder()
              .accumulateLeft(List(binding))(_ addBindings _)
              .setBody(body)
          )
        case ENil(typ) =>
          builder.setNil(PLF.Expr.Nil.newBuilder().setType(typ))
        case ECons(typ, front, tail) =>
          builder.setCons(
            PLF.Expr.Cons
              .newBuilder()
              .setType(typ)
              .accumulateLeft(front)(_ addFront _)
              .setTail(tail)
          )
        case ENone(typ) =>
          builder.setOptionalNone(PLF.Expr.OptionalNone.newBuilder().setType(typ))
        case ESome(typ, x) =>
          builder.setOptionalSome(PLF.Expr.OptionalSome.newBuilder().setType(typ).setValue(x))
        case ELocation(loc, expr) =>
          encodeExprBuilder(expr, builder).setLocation(loc)
        case EUpdate(u) =>
          builder.setUpdate(u)
        case EToAny(ty, body) =>
          builder.setToAny(PLF.Expr.ToAny.newBuilder().setType(ty).setExpr(body))
        case EFromAny(ty, body) =>
          builder.setFromAny(PLF.Expr.FromAny.newBuilder().setType(ty).setExpr(body))
        case ETypeRep(ty) =>
          builder.setTypeRep(ty)
        case EThrow(retTy, excTy, exc) =>
          assertSince(LV.Features.exceptions, "Expr.Throw")
          builder.setThrow(
            PLF.Expr.Throw
              .newBuilder()
              .setReturnType(retTy)
              .setExceptionType(excTy)
              .setExceptionExpr(exc)
          )
        case EToAnyException(ty, body) =>
          assertSince(LV.Features.exceptions, "Expr.ToAnyException")
          builder.setToAnyException(PLF.Expr.ToAnyException.newBuilder().setType(ty).setExpr(body))
        case EFromAnyException(ty, body) =>
          assertSince(LV.Features.exceptions, "Expr.FromAnyException")
          builder.setFromAnyException(
            PLF.Expr.FromAnyException.newBuilder().setType(ty).setExpr(body)
          )
        case EToInterface(iface, tpl, value) =>
          builder.setToInterface(
            PLF.Expr.ToInterface
              .newBuilder()
              .setInterfaceType(iface)
              .setTemplateType(tpl)
              .setTemplateExpr(value)
          )
        case EFromInterface(iface, tpl, value) =>
          builder.setFromInterface(
            PLF.Expr.FromInterface
              .newBuilder()
              .setInterfaceType(iface)
              .setTemplateType(tpl)
              .setInterfaceExpr(value)
          )
        case EUnsafeFromInterface(iface, tpl, cid, value) =>
          builder.setUnsafeFromInterface(
            PLF.Expr.UnsafeFromInterface
              .newBuilder()
              .setInterfaceType(iface)
              .setTemplateType(tpl)
              .setContractIdExpr(cid)
              .setInterfaceExpr(value)
          )
        case EToRequiredInterface(superIface, iface, value) =>
          builder.setToRequiredInterface(
            PLF.Expr.ToRequiredInterface
              .newBuilder()
              .setRequiredInterface(superIface)
              .setRequiringInterface(iface)
              .setExpr(value)
          )
        case EFromRequiredInterface(superIface, iface, value) =>
          builder.setFromRequiredInterface(
            PLF.Expr.FromRequiredInterface
              .newBuilder()
              .setRequiredInterface(superIface)
              .setRequiringInterface(iface)
              .setExpr(value)
          )
        case EUnsafeFromRequiredInterface(superIface, iface, cid, value) =>
          builder.setUnsafeFromRequiredInterface(
            PLF.Expr.UnsafeFromRequiredInterface
              .newBuilder()
              .setRequiredInterface(superIface)
              .setRequiringInterface(iface)
              .setContractIdExpr(cid)
              .setInterfaceExpr(value)
          )
        case EInterfaceTemplateTypeRep(iface, value) =>
          builder.setInterfaceTemplateTypeRep(
            PLF.Expr.InterfaceTemplateTypeRep
              .newBuilder()
              .setInterface(iface)
              .setExpr(value)
          )
        case ESignatoryInterface(iface, value) =>
          builder.setSignatoryInterface(
            PLF.Expr.SignatoryInterface
              .newBuilder()
              .setInterface(iface)
              .setExpr(value)
          )
        case EObserverInterface(iface, value) =>
          builder.setObserverInterface(
            PLF.Expr.ObserverInterface
              .newBuilder()
              .setInterface(iface)
              .setExpr(value)
          )
        case EChoiceController(ty, choiceName, contract, choiceArg) =>
          assertSince(LV.Features.choiceFuncs, "Expr.ChoiceController")
          val b = PLF.Expr.ChoiceController
            .newBuilder()
            .setTemplate(ty)
            .setContractExpr(contract)
            .setChoiceArgExpr(choiceArg)
          setInternedString(choiceName, b.setChoiceInternedStr)
          builder.setChoiceController(b)
        case EChoiceObserver(ty, choiceName, contract, choiceArg) =>
          assertSince(LV.Features.choiceFuncs, "Expr.ChoiceObserver")
          val b = PLF.Expr.ChoiceObserver
            .newBuilder()
            .setTemplate(ty)
            .setContractExpr(contract)
            .setChoiceArgExpr(choiceArg)
          setInternedString(choiceName, b.setChoiceInternedStr)
          builder.setChoiceObserver(b)
        case EExperimental(name, ty) =>
          assertSince(LV.Features.unstable, "Expr.experimental")
          builder.setExperimental(PLF.Expr.Experimental.newBuilder().setName(name).setType(ty))

        case ECallInterface(ty, methodName, expr) =>
          val b = PLF.Expr.CallInterface.newBuilder()
          b.setInterfaceType(ty)
          b.setInterfaceExpr(expr)
          b.setMethodInternedName(stringsTable.insert(methodName))
          builder.setCallInterface(b)

        case EViewInterface(iface, expr) =>
          builder.setViewInterface(
            PLF.Expr.ViewInterface
              .newBuilder()
              .setInterface(iface)
              .setExpr(expr)
          )
      }
      builder
    }

    private implicit def encodeDataDef(nameWithDef: (DottedName, DDataType)): PLF.DefDataType = {
      val (dottedName, dataType) = nameWithDef
      val builder = PLF.DefDataType.newBuilder()
      setDottedName_(dottedName, builder.setNameInternedDname)
      builder.accumulateLeft(dataType.params)(_ addParams _)
      builder.setSerializable(dataType.serializable)

      dataType.cons match {
        case DataRecord(fields) =>
          builder.setRecord(
            PLF.DefDataType.Fields.newBuilder().accumulateLeft(fields)(_ addFields _)
          )
        case DataVariant(variants) =>
          builder.setVariant(
            PLF.DefDataType.Fields.newBuilder().accumulateLeft(variants)(_ addFields _)
          )
        case DataEnum(constructors) =>
          val b = PLF.DefDataType.EnumConstructors.newBuilder()
          constructors.foreach(setString(_, b.addConstructorsInternedStr))
          builder.setEnum(b)
        case DataInterface =>
          builder.setInterface(PLF.Unit.newBuilder())
      }
      builder.build()
    }

    private implicit def encodeException(
        nameWithDef: (DottedName, DefException)
    ): PLF.DefException = {
      val (dottedName, exception) = nameWithDef
      val builder = PLF.DefException.newBuilder()
      builder.setNameInternedDname(dottedNameTable.insert(dottedName))
      builder.setMessage(exception.message)
      builder.build()
    }

    private implicit def encodeInterfaceDef(
        nameWithDef: (DottedName, DefInterface)
    ): PLF.DefInterface = {
      val (dottedName, interface) = nameWithDef
      val builder = PLF.DefInterface.newBuilder()
      builder.setTyconInternedDname(dottedNameTable.insert(dottedName))
      builder.setParamInternedStr(stringsTable.insert(interface.param))
      builder.accumulateLeft(interface.choices.sortByKey)(_ addChoices _)
      builder.accumulateLeft(interface.methods.sortByKey)(_ addMethods _)
      if (interface.requires.nonEmpty) {
        builder.accumulateLeft(interface.requires)(_ addRequires _)
      }
      builder.setView(interface.view)
      builder.build()
    }

    private implicit def encodeInterfaceMethod(
        nameWithMethod: (MethodName, InterfaceMethod)
    ): PLF.InterfaceMethod = {
      val (name, method) = nameWithMethod
      val b = PLF.InterfaceMethod.newBuilder()
      b.setMethodInternedName(stringsTable.insert(name))
      b.setType(method.returnType)
      b.build()
    }

    private implicit def encodeSynonymDef(nameWithDef: (DottedName, DTypeSyn)): PLF.DefTypeSyn = {
      val (dottedName, typeSyn) = nameWithDef
      val builder = PLF.DefTypeSyn.newBuilder()
      setDottedName_(dottedName, builder.setNameInternedDname)
      builder.accumulateLeft(typeSyn.params)(_ addParams _)
      builder.setType(typeSyn.typ)
      builder.build()
    }

    private implicit def encodeNameWithType(
        nameWithType: (DottedName, Type)
    ): PLF.DefValue.NameWithType = {
      val (name, typ) = nameWithType
      val b = PLF.DefValue.NameWithType.newBuilder
      setDottedName(name, b.setNameInternedDname)
      b.setType(typ)
      b.build()
    }

    private implicit def encodeValueDef(nameWithDef: (DottedName, DValue)): PLF.DefValue = {
      val (dottedName, value) = nameWithDef
      PLF.DefValue
        .newBuilder()
        .setNameWithType(dottedName -> value.typ)
        .setExpr(value.body)
        .build()
    }

    private implicit def encodeChoice(
        nameWithChoice: (ChoiceName, TemplateChoice)
    ): PLF.TemplateChoice = {
      val (name, choice) = nameWithChoice
      val b = PLF.TemplateChoice.newBuilder()
      setString(name, b.setNameInternedStr)
      b.setConsuming(choice.consuming)
      b.setControllers(choice.controllers)
      choice.choiceObservers match {
        case Some(value) =>
          b.setObservers(value)
        case None =>
          b.setObservers(ENil(AstUtil.TParty))
      }
      choice.choiceAuthorizers match {
        case Some(value) =>
          assertSince(LV.Features.choiceAuthority, "TemplateChoice.authority")
          b.setAuthorizers(value)
        case None =>
      }
      b.setArgBinder(choice.argBinder._1 -> choice.argBinder._2)
      b.setRetType(choice.returnType)
      b.setUpdate(choice.update)
      setString(choice.selfBinder, b.setSelfBinderInternedStr)
      b.build()
    }

    private implicit def encodeTemplateKey(key: TemplateKey): PLF.DefTemplate.DefKey =
      PLF.DefTemplate.DefKey
        .newBuilder()
        .setType(key.typ)
        .setKeyExpr(key.body)
        .setMaintainers(key.maintainers)
        .build()

    private implicit def encodeTemplate(
        nameWithTemplate: (DottedName, Template)
    ): PLF.DefTemplate = {
      val (name, template) = nameWithTemplate
      val b = PLF.DefTemplate.newBuilder()
      setDottedName_(name, b.setTyconInternedDname)
      setString(template.param, b.setParamInternedStr)
      b.setPrecond(template.precond)
      b.setSignatories(template.signatories)
      b.accumulateLeft(template.choices.sortByKey)(_ addChoices _)
      b.setObservers(template.observers)
      template.key.foreach(b.setKey(_))
      b.accumulateLeft(template.implements.sortByKey)(_ addImplements _)
      b.build()
    }

    private implicit def encodeTemplateImplements(
        interfaceWithImplements: (TypeConId, TemplateImplements)
    ): PLF.DefTemplate.Implements = {
      val (interface, implements) = interfaceWithImplements
      val b = PLF.DefTemplate.Implements.newBuilder()
      b.setInterface(interface)
      b.setBody(implements.body)
      b.build()
    }

    private implicit def encodeInterfaceInstanceBody(
        iiBody: InterfaceInstanceBody
    ): PLF.InterfaceInstanceBody = {
      val InterfaceInstanceBody(methods, view) = iiBody
      val b = PLF.InterfaceInstanceBody.newBuilder()
      b.accumulateLeft(methods.sortByKey)(_ addMethods _)
      b.setView(view)
      b.build()
    }

    private implicit def encodeInterfaceInstanceMethod(
        nameWithMethod: (MethodName, InterfaceInstanceMethod)
    ): PLF.InterfaceInstanceBody.InterfaceInstanceMethod = {
      val (name, method) = nameWithMethod
      val b = PLF.InterfaceInstanceBody.InterfaceInstanceMethod.newBuilder()
      b.setMethodInternedName(stringsTable.insert(name))
      b.setValue(method.value)
      b.build()
    }

    private def setString[X](s: String, setThroughTable: Int => X): Unit = {
      setThroughTable(stringsTable.insert(s))
      ()
    }

    private def setInternedString[X](s: String, setThroughTable: Int => X) = {
      setThroughTable(stringsTable.insert(s))
      ()
    }

    private def setDottedName[X](
        name: Ref.DottedName,
        setThroughTable: Int => X,
    ) = {
      setThroughTable(dottedNameTable.insert(name))
      ()
    }

    private def setDottedName_[X](
        name: Ref.DottedName,
        setThroughTable: Int => X,
    ) = {
      setThroughTable(dottedNameTable.insert(name))
      ()
    }

  }

  private def assertSince(minVersion: LV, description: String): Unit =
    if (languageVersion < minVersion)
      throw EncodeError(s"$description is not supported by Daml-LF $languageVersion")

}

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
object EncodeV2 {

  private sealed abstract class LeftRecMatcher[L, R] {
    def unapply(arg: L): Option[(L, ImmArray[R])]
  }

  private object LeftRecMatcher {
    def apply[L, R](
        split: PartialFunction[L, (L, R)]
    ): LeftRecMatcher[L, R] = new LeftRecMatcher[L, R] {
      @tailrec
      private def go(
          x: L,
          stack: FrontStack[R] = FrontStack.empty,
      ): Option[(L, ImmArray[R])] =
        if (split.isDefinedAt(x)) {
          val (left, right) = split(x)
          go(left, right +: stack)
        } else if (stack.nonEmpty) {
          Some(x -> stack.toImmArray)
        } else {
          None
        }

      @inline
      final def unapply(arg: L): Option[(L, ImmArray[R])] = go(x = arg)
    }
  }

  private sealed abstract class RightRecMatcher[L, R] {
    def unapply(arg: R): Option[(ImmArray[L], R)]
  }

  private object RightRecMatcher {
    def apply[L, R](
        split: PartialFunction[R, (L, R)]
    ): RightRecMatcher[L, R] = new RightRecMatcher[L, R] {

      @tailrec
      private def go(
          stack: BackStack[L] = BackStack.empty,
          x: R,
      ): Option[(ImmArray[L], R)] =
        if (split.isDefinedAt(x)) {
          val (left, right) = split(x)
          go(stack :+ left, right)
        } else if (stack.nonEmpty) {
          Some(stack.toImmArray -> x)
        } else {
          None
        }

      @inline
      final def unapply(arg: R): Option[(ImmArray[L], R)] = go(x = arg)
    }
  }

  private final implicit class Acc[X](val x: X) extends AnyVal {
    @inline
    def accumulateLeft[Y](iterable: Iterable[Y])(f: (X, Y) => X): X = iterable.foldLeft(x)(f)
    @inline
    def accumulateLeft[Y](array: ImmArray[Y])(f: (X, Y) => X): X = array.foldLeft(x)(f)
    @inline
    def accumulateLeft[Y](option: Option[Y])(f: (X, Y) => X): X = option.fold(x)(f(x, _))
  }

  private implicit class IdentifierOps(val identifier: Identifier) extends AnyVal {
    import identifier._
    @inline
    def moduleRef: (PackageId, ModuleName) = packageId -> qualifiedName.module
    @inline
    def name: DottedName = qualifiedName.name
  }

  private implicit class IterableOps[X, Y](val iterable: Iterable[(X, Y)]) extends AnyVal {
    def sortByKey(implicit ordering: Ordering[X]): List[(X, Y)] = iterable.toList.sortBy(_._1)
    def values: Iterable[Y] = iterable.map(_._2)
  }

  private abstract class TableBuilder[Scala, Proto] {
    private val map = mutable.Map.empty[Scala, Int]
    private val buffer = List.newBuilder[Proto]
    def insert(x: Scala): Int = map.getOrElseUpdate(x, { buffer += toProto(x); map.size })
    def build: Iterable[Proto] = buffer.result()
    def toProto(x: Scala): Proto
  }
}
