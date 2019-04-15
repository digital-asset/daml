// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package lfpackage

import scala.collection.JavaConverters._
import com.digitalasset.daml.lf.archive.LanguageMajorVersion.V1
import com.digitalasset.daml.lf.archive.{LanguageMinorVersion, LanguageVersion}
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Util._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{Decimal, ImmArray, Time}
import com.digitalasset.daml.lf.lfpackage.Decode.ParseError
import com.digitalasset.daml_lf.{DamlLf1 => PLF}

import scala.collection.mutable

private[lf] class DecodeV1(minor: LanguageMinorVersion) extends Decode.OfPackage[PLF.Package] {

  import Decode._

  private val languageVersion = LanguageVersion(V1, minor)

  override def decodePackage(packageId: SimpleString, lfPackage: PLF.Package): Package =
    Package(lfPackage.getModulesList.asScala.map(ModuleDecoder(packageId, _).decode))

  private[this] def eitherToParseError[A](x: Either[String, A]): A = {
    x.fold(err => throw new ParseError(err), identity)
  }

  private[this] def decodeSegments(segments: ImmArray[String]): DottedName = {
    DottedName.fromSegments(segments.toSeq) match {
      case Left(err) => throw new ParseError(err)
      case Right(x) => x
    }
  }

  case class ModuleDecoder(val packageId: SimpleString, val lfModule: PLF.Module) {
    val moduleName = eitherToParseError(
      ModuleName.fromSegments(lfModule.getName.getSegmentsList.asScala))

    // FIXME(JM): rewrite.
    var currentDefinitionRef: Option[DefinitionRef[PackageId]] = None

    def decode(): Module = {
      val defs = mutable.ArrayBuffer[(DottedName, Definition)]()
      val templates = mutable.ArrayBuffer[(DottedName, Template)]()

      // collect data types
      lfModule.getDataTypesList.asScala.foreach { defn =>
        val defName =
          eitherToParseError(DottedName.fromSegments(defn.getName.getSegmentsList.asScala))
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        val d = decodeDefDataType(defn)
        defs += (defName -> d)
      }

      // collect values
      lfModule.getValuesList.asScala.foreach { defn =>
        val defName =
          decodeSegments(ImmArray(defn.getNameWithType.getNameList.asScala))
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        val d = decodeDefValue(defn)
        defs += (defName -> d)
      }

      // collect templates
      lfModule.getTemplatesList.asScala.foreach { defn =>
        val defName =
          eitherToParseError(DottedName.fromSegments(defn.getTycon.getSegmentsList.asScala))
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        templates += ((defName, decodeTemplate(defn)))
      }

      Module(moduleName, defs, templates, languageVersion, decodeFeatureFlags(lfModule.getFlags))
    }

    // -----------------------------------------------------------------------

    private[this] def decodeFeatureFlags(flags: PLF.FeatureFlags): FeatureFlags = {
      // NOTE(JM, #157): We disallow loading packages with these flags because they impact the Ledger API in
      // ways that would currently make it quite complicated to support them.
      if (!flags.getDontDivulgeContractIdsInCreateArguments || !flags.getDontDiscloseNonConsumingChoicesToObservers) {
        throw new ParseError("Deprecated feature flag settings detected, refusing to parse package")
      }
      FeatureFlags(
        forbidPartyLiterals = flags.getForbidPartyLiterals,
      )
    }

    private[this] def decodeDefDataType(lfDataType: PLF.DefDataType): DDataType = {
      DDataType(
        lfDataType.getSerializable,
        ImmArray(lfDataType.getParamsList.asScala)
          .map(decodeTypeVarWithKind(_)),
        lfDataType.getDataConsCase match {
          case PLF.DefDataType.DataConsCase.RECORD =>
            DataRecord(decodeFields(ImmArray(lfDataType.getRecord.getFieldsList.asScala)), None)
          case PLF.DefDataType.DataConsCase.VARIANT =>
            DataVariant(decodeFields(ImmArray(lfDataType.getVariant.getFieldsList.asScala)))
          case PLF.DefDataType.DataConsCase.DATACONS_NOT_SET =>
            throw ParseError("DefDataType.DATACONS_NOT_SET")

        }
      )
    }

    private[this] def decodeFields(
        lfFields: ImmArray[PLF.FieldWithType]): ImmArray[(String, Type)] =
      lfFields.map { field =>
        checkIdentifier(field.getField)
        (field.getField, decodeType(field.getType))
      }

    private[this] def decodeDefValue(lfValue: PLF.DefValue): DValue = {
      DValue(
        typ = decodeType(lfValue.getNameWithType.getType),
        noPartyLiterals = lfValue.getNoPartyLiterals,
        body = decodeExpr(lfValue.getExpr),
        isTest = lfValue.getIsTest
      )
    }

    private def decodeLocation(lfExpr: PLF.Expr): Option[Location] =
      if (lfExpr.hasLocation && lfExpr.getLocation.hasRange) {
        val loc = lfExpr.getLocation
        val (pkgId, module) =
          if (loc.hasModule)
            decodeModuleRef(loc.getModule)
          else
            (packageId, moduleName)

        val range = loc.getRange
        Some(
          Location(
            pkgId,
            module,
            (range.getStartLine, range.getStartCol),
            (range.getEndLine, range.getEndCol)))
      } else {
        None
      }

    private[this] def decodeTemplateKey(
        key: PLF.DefTemplate.DefKey,
        tplVar: ExprVarName): TemplateKey = {
      assertSince("3", "DefTemplate.DefKey")
      TemplateKey(
        decodeType(key.getType),
        decodeKeyExpr(key.getKey, tplVar),
        decodeExpr(key.getMaintainers)
      )
    }

    private[this] def decodeKeyExpr(expr: PLF.KeyExpr, tplVar: ExprVarName): Expr = {
      expr.getSumCase match {
        case PLF.KeyExpr.SumCase.RECORD =>
          val recCon = expr.getRecord
          ERecCon(
            tycon = decodeTypeConApp(recCon.getTycon),
            fields = ImmArray(recCon.getFieldsList.asScala).map { field =>
              checkIdentifier(field.getField)
              (field.getField, decodeKeyExpr(field.getExpr, tplVar))
            }
          )

        case PLF.KeyExpr.SumCase.PROJECTIONS =>
          val lfProjs = expr.getProjections.getProjectionsList.asScala
          lfProjs.foldLeft(EVar(tplVar): Expr) {
            case (acc, lfProj) =>
              ERecProj(decodeTypeConApp(lfProj.getTycon), lfProj.getField, acc)
          }

        case PLF.KeyExpr.SumCase.SUM_NOT_SET =>
          throw ParseError("KeyExpr.SUM_NOT_SET")
      }
    }

    private[this] def decodeTemplate(lfTempl: PLF.DefTemplate): Template =
      Template(
        param = lfTempl.getParam,
        precond = if (lfTempl.hasPrecond) decodeExpr(lfTempl.getPrecond) else EPrimCon(PCTrue),
        signatories = decodeExpr(lfTempl.getSignatories),
        agreementText = decodeExpr(lfTempl.getAgreement),
        choices = lfTempl.getChoicesList.asScala
          .map(decodeChoice)
          .map(ch => (ch.name, ch)),
        observers = decodeExpr(lfTempl.getObservers),
        key =
          if (lfTempl.hasKey) Some(decodeTemplateKey(lfTempl.getKey, lfTempl.getParam)) else None
      )

    private[this] def decodeChoice(lfChoice: PLF.TemplateChoice): TemplateChoice = {
      checkIdentifier(lfChoice.getName)
      checkIdentifier(lfChoice.getSelfBinder)
      val (v, t) = decodeBinder(lfChoice.getArgBinder)
      TemplateChoice(
        name = lfChoice.getName,
        consuming = lfChoice.getConsuming,
        controllers = decodeExpr(lfChoice.getControllers),
        selfBinder = lfChoice.getSelfBinder,
        argBinder = Some(v) -> t,
        returnType = decodeType(lfChoice.getRetType),
        update = decodeExpr(lfChoice.getUpdate)
      )
    }

    private[this] def decodeKind(lfKind: PLF.Kind): Kind =
      lfKind.getSumCase match {
        case PLF.Kind.SumCase.STAR => KStar
        case PLF.Kind.SumCase.ARROW =>
          val kArrow = lfKind.getArrow
          val params = kArrow.getParamsList.asScala
          checkNonEmpty(params, "params")
          (params :\ decodeKind(kArrow.getResult))((param, kind) => KArrow(decodeKind(param), kind))
        case PLF.Kind.SumCase.SUM_NOT_SET =>
          throw ParseError("Kind.SUM_NOT_SET")
      }

    private[this] def decodeType(lfType: PLF.Type): Type =
      lfType.getSumCase match {
        case PLF.Type.SumCase.VAR =>
          val tvar = lfType.getVar
          checkIdentifier(tvar.getVar)
          tvar.getArgsList.asScala
            .foldLeft[Type](TVar(tvar.getVar))((typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.CON =>
          val tcon = lfType.getCon
          (TTyCon(decodeTypeConName(tcon.getTycon)) /: [Type] tcon.getArgsList.asScala)(
            (typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.PRIM =>
          val prim = lfType.getPrim
          val (tPrim, minVersion) = DecodeV1.primTypeTable(prim.getPrim)
          assertSince(minVersion, prim.getPrim.getValueDescriptor.getFullName)
          (TBuiltin(tPrim) /: [Type] prim.getArgsList.asScala)((typ, arg) =>
            TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.FUN =>
          assertUntil("0", "Type.Fun")
          val tFun = lfType.getFun
          val params = tFun.getParamsList.asScala
          checkNonEmpty(params, "params")
          (params :\ decodeType(tFun.getResult))((param, res) => TFun(decodeType(param), res))
        case PLF.Type.SumCase.FORALL =>
          val tForall = lfType.getForall
          val vars = tForall.getVarsList.asScala
          checkNonEmpty(vars, "vars")
          (vars :\ decodeType(tForall.getBody))((binder, acc) =>
            TForall(decodeTypeVarWithKind(binder), acc))
        case PLF.Type.SumCase.TUPLE =>
          val tuple = lfType.getTuple
          val fields = tuple.getFieldsList.asScala
          checkNonEmpty(fields, "fields")
          TTuple(
            ImmArray(fields.map { ft =>
              checkIdentifier(ft.getField)
              ft.getField -> decodeType(ft.getType)
            })
          )

        case PLF.Type.SumCase.SUM_NOT_SET =>
          throw ParseError("Type.SUM_NOT_SET")
      }

    private[this] def decodeModuleRef(lfRef: PLF.ModuleRef): (PackageId, ModuleName) = {
      val modName = eitherToParseError(
        ModuleName.fromSegments(lfRef.getModuleName.getSegmentsList.asScala))
      lfRef.getPackageRef.getSumCase match {
        case PLF.PackageRef.SumCase.SELF =>
          (this.packageId, modName)
        case PLF.PackageRef.SumCase.PACKAGE_ID =>
          val pkgId = SimpleString
            .fromString(lfRef.getPackageRef.getPackageId)
            .getOrElse(throw ParseError(s"invalid packageId '${lfRef.getPackageRef.getPackageId}'"))
          (pkgId, modName)
        case PLF.PackageRef.SumCase.SUM_NOT_SET =>
          throw ParseError("PackageRef.SUM_NOT_SET")
      }
    }

    private[this] def decodeValName(lfVal: PLF.ValName): ValueRef = {
      val (packageId, module) = decodeModuleRef(lfVal.getModule)
      val name = decodeSegments(ImmArray(lfVal.getNameList.asScala))
      ValueRef(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConName(lfTyConName: PLF.TypeConName): TypeConName = {
      val (packageId, module) = decodeModuleRef(lfTyConName.getModule)
      val name = eitherToParseError(
        DottedName.fromSegments(lfTyConName.getName.getSegmentsList.asScala))
      TypeConName(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConApp(lfTyConApp: PLF.Type.Con): TypeConApp =
      TypeConApp(
        decodeTypeConName(lfTyConApp.getTycon),
        ImmArray(lfTyConApp.getArgsList.asScala.map(decodeType))
      )

    private[this] def decodeExpr(lfExpr: PLF.Expr): Expr =
      decodeLocation(lfExpr) match {
        case None => decodeExprBody(lfExpr)
        case Some(loc) => ELocation(loc, decodeExprBody(lfExpr))
      }

    private[this] def decodeExprBody(lfExpr: PLF.Expr): Expr =
      lfExpr.getSumCase match {
        case PLF.Expr.SumCase.VAR =>
          checkIdentifier(lfExpr.getVar)
          EVar(lfExpr.getVar)

        case PLF.Expr.SumCase.VAL =>
          EVal(decodeValName(lfExpr.getVal))

        case PLF.Expr.SumCase.PRIM_LIT =>
          EPrimLit(decodePrimLit(lfExpr.getPrimLit))

        case PLF.Expr.SumCase.PRIM_CON =>
          lfExpr.getPrimCon match {
            case PLF.PrimCon.CON_UNIT => EPrimCon(PCUnit)
            case PLF.PrimCon.CON_FALSE => EPrimCon(PCFalse)
            case PLF.PrimCon.CON_TRUE => EPrimCon(PCTrue)
            case PLF.PrimCon.UNRECOGNIZED =>
              throw ParseError("PrimCon.UNRECOGNIZED")
          }

        case PLF.Expr.SumCase.BUILTIN =>
          val (builtin, minVersion) = DecodeV1.builtinFunctionMap(lfExpr.getBuiltin)
          assertSince(minVersion, lfExpr.getBuiltin.getValueDescriptor.getFullName)
          EBuiltin(builtin)

        case PLF.Expr.SumCase.REC_CON =>
          val recCon = lfExpr.getRecCon
          ERecCon(
            tycon = decodeTypeConApp(recCon.getTycon),
            fields = ImmArray(recCon.getFieldsList.asScala).map { field =>
              checkIdentifier(field.getField)
              (field.getField, decodeExpr(field.getExpr))
            }
          )

        case PLF.Expr.SumCase.REC_PROJ =>
          val recProj = lfExpr.getRecProj
          checkIdentifier(recProj.getField)
          ERecProj(
            tycon = decodeTypeConApp(recProj.getTycon),
            field = recProj.getField,
            record = decodeExpr(recProj.getRecord))

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          checkIdentifier(recUpd.getField)
          ERecUpd(
            tycon = decodeTypeConApp(recUpd.getTycon),
            field = recUpd.getField,
            record = decodeExpr(recUpd.getRecord),
            update = decodeExpr(recUpd.getUpdate))

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          checkIdentifier(varCon.getVariantCon)
          EVariantCon(
            decodeTypeConApp(varCon.getTycon),
            varCon.getVariantCon,
            decodeExpr(varCon.getVariantArg))

        case PLF.Expr.SumCase.TUPLE_CON =>
          val tupleCon = lfExpr.getTupleCon
          ETupleCon(
            ImmArray(tupleCon.getFieldsList.asScala).map { field =>
              checkIdentifier(field.getField)
              (field.getField, decodeExpr(field.getExpr))
            }
          )

        case PLF.Expr.SumCase.TUPLE_PROJ =>
          val tupleProj = lfExpr.getTupleProj
          checkIdentifier(tupleProj.getField)
          ETupleProj(tupleProj.getField, decodeExpr(tupleProj.getTuple))

        case PLF.Expr.SumCase.TUPLE_UPD =>
          val tupleUpd = lfExpr.getTupleUpd
          checkIdentifier(tupleUpd.getField)
          ETupleUpd(
            field = tupleUpd.getField,
            tuple = decodeExpr(tupleUpd.getTuple),
            update = decodeExpr(tupleUpd.getUpdate))

        case PLF.Expr.SumCase.APP =>
          val app = lfExpr.getApp
          val args = app.getArgsList.asScala
          checkNonEmpty(args, "args")
          (decodeExpr(app.getFun) /: args)((e, arg) => EApp(e, decodeExpr(arg)))

        case PLF.Expr.SumCase.ABS =>
          val lfAbs = lfExpr.getAbs
          val params = lfAbs.getParamList.asScala
          checkNonEmpty(params, "params")
          // val params = lfAbs.getParamList.asScala.map(decodeBinder)
          (params :\ decodeExpr(lfAbs.getBody))((param, e) =>
            EAbs(decodeBinder(param), e, currentDefinitionRef))

        case PLF.Expr.SumCase.TY_APP =>
          val tyapp = lfExpr.getTyApp
          val args = tyapp.getTypesList.asScala
          checkNonEmpty(args, "args")
          (decodeExpr(tyapp.getExpr) /: args)((e, arg) => ETyApp(e, decodeType(arg)))

        case PLF.Expr.SumCase.TY_ABS =>
          val lfTyAbs = lfExpr.getTyAbs
          val params = lfTyAbs.getParamList.asScala
          checkNonEmpty(params, "params")
          (params :\ decodeExpr(lfTyAbs.getBody))((param, e) =>
            ETyAbs(decodeTypeVarWithKind(param), e))

        case PLF.Expr.SumCase.LET =>
          val lfLet = lfExpr.getLet
          val bindings = lfLet.getBindingsList.asScala
          checkNonEmpty(bindings, "bindings")
          (bindings :\ decodeExpr(lfLet.getBody))((binding, e) => {
            val (v, t) = decodeBinder(binding.getBinder)
            ELet(Binding(Some(v), t, decodeExpr(binding.getBound)), e)
          })

        case PLF.Expr.SumCase.NIL =>
          ENil(decodeType(lfExpr.getNil.getType))

        case PLF.Expr.SumCase.CONS =>
          val cons = lfExpr.getCons
          val front = cons.getFrontList.asScala
          checkNonEmpty(front, "front")
          val typ = decodeType(cons.getType)
          ECons(typ, ImmArray(front.map(decodeExpr)), decodeExpr(cons.getTail))

        case PLF.Expr.SumCase.CASE =>
          val case_ = lfExpr.getCase
          ECase(
            decodeExpr(case_.getScrut),
            ImmArray(case_.getAltsList.asScala).map(decodeCaseAlt(_)))

        case PLF.Expr.SumCase.UPDATE =>
          EUpdate(decodeUpdate(lfExpr.getUpdate))

        case PLF.Expr.SumCase.SCENARIO =>
          EScenario(decodeScenario(lfExpr.getScenario))

        case PLF.Expr.SumCase.NONE =>
          assertSince("1", "Expr.None")
          ENone(decodeType(lfExpr.getNone.getType))

        case PLF.Expr.SumCase.SOME =>
          assertSince("1", "Expr.Some")
          val some = lfExpr.getSome
          ESome(decodeType(some.getType), decodeExpr(some.getBody))

        case PLF.Expr.SumCase.SUM_NOT_SET =>
          throw ParseError("Expr.SUM_NOT_SET")
      }

    private[this] def decodeCaseAlt(lfCaseAlt: PLF.CaseAlt): CaseAlt = {
      val pat: CasePat = lfCaseAlt.getSumCase match {
        case PLF.CaseAlt.SumCase.DEFAULT =>
          CPDefault
        case PLF.CaseAlt.SumCase.VARIANT =>
          val variant = lfCaseAlt.getVariant
          checkIdentifier(variant.getBinder)
          checkIdentifier(variant.getBinder)
          CPVariant(decodeTypeConName(variant.getCon), variant.getVariant, variant.getBinder)
        case PLF.CaseAlt.SumCase.PRIM_CON =>
          CPPrimCon(decodePrimCon(lfCaseAlt.getPrimCon))
        case PLF.CaseAlt.SumCase.NIL =>
          CPNil
        case PLF.CaseAlt.SumCase.CONS =>
          val cons = lfCaseAlt.getCons
          checkIdentifier(cons.getVarHead)
          checkIdentifier(cons.getVarTail)
          CPCons(cons.getVarHead, cons.getVarTail)

        case PLF.CaseAlt.SumCase.NONE =>
          assertSince("1", "CaseAlt.None")
          CPNone

        case PLF.CaseAlt.SumCase.SOME =>
          checkIdentifier(lfCaseAlt.getSome.getVarBody)
          assertSince("1", "CaseAlt.Some")

          CPSome(lfCaseAlt.getSome.getVarBody)

        case PLF.CaseAlt.SumCase.SUM_NOT_SET =>
          throw ParseError("CaseAlt.SUM_NOT_SET")
      }
      CaseAlt(pat, decodeExpr(lfCaseAlt.getBody))
    }

    private[this] def decodeRetrieveByKey(value: PLF.Update.RetrieveByKey): RetrieveByKey = {
      RetrieveByKey(
        decodeTypeConName(value.getTemplate),
        decodeExpr(value.getKey),
      )
    }

    private[this] def decodeUpdate(lfUpdate: PLF.Update): Update =
      lfUpdate.getSumCase match {

        case PLF.Update.SumCase.PURE =>
          val pure = lfUpdate.getPure
          UpdatePure(decodeType(pure.getType), decodeExpr(pure.getExpr))

        case PLF.Update.SumCase.BLOCK =>
          val block = lfUpdate.getBlock
          UpdateBlock(
            bindings = ImmArray(block.getBindingsList.asScala.map(decodeBinding)),
            body = decodeExpr(block.getBody))

        case PLF.Update.SumCase.CREATE =>
          val create = lfUpdate.getCreate
          UpdateCreate(
            templateId = decodeTypeConName(create.getTemplate),
            arg = decodeExpr(create.getExpr))

        case PLF.Update.SumCase.EXERCISE =>
          val exercise = lfUpdate.getExercise
          UpdateExercise(
            templateId = decodeTypeConName(exercise.getTemplate),
            choice = exercise.getChoice,
            cidE = decodeExpr(exercise.getCid),
            actorsE = decodeExpr(exercise.getActor),
            argE = decodeExpr(exercise.getArg)
          )

        case PLF.Update.SumCase.GET_TIME =>
          UpdateGetTime

        case PLF.Update.SumCase.FETCH =>
          val fetch = lfUpdate.getFetch
          UpdateFetch(
            templateId = decodeTypeConName(fetch.getTemplate),
            contractId = decodeExpr(fetch.getCid))

        case PLF.Update.SumCase.FETCH_BY_KEY =>
          assertSince("2", "fetchByKey")
          UpdateFetchByKey(decodeRetrieveByKey(lfUpdate.getFetchByKey))

        case PLF.Update.SumCase.LOOKUP_BY_KEY =>
          assertSince("2", "lookupByKey")
          UpdateLookupByKey(decodeRetrieveByKey(lfUpdate.getLookupByKey))

        case PLF.Update.SumCase.EMBED_EXPR =>
          val embedExpr = lfUpdate.getEmbedExpr
          UpdateEmbedExpr(decodeType(embedExpr.getType), decodeExpr(embedExpr.getBody))

        case PLF.Update.SumCase.SUM_NOT_SET =>
          throw ParseError("Update.SUM_NOT_SET")
      }

    private[this] def decodeScenario(lfScenario: PLF.Scenario): Scenario =
      lfScenario.getSumCase match {
        case PLF.Scenario.SumCase.PURE =>
          val pure = lfScenario.getPure
          ScenarioPure(decodeType(pure.getType), decodeExpr(pure.getExpr))

        case PLF.Scenario.SumCase.COMMIT =>
          val commit = lfScenario.getCommit
          ScenarioCommit(
            decodeExpr(commit.getParty),
            decodeExpr(commit.getExpr),
            decodeType(commit.getRetType))

        case PLF.Scenario.SumCase.MUSTFAILAT =>
          val commit = lfScenario.getMustFailAt
          ScenarioMustFailAt(
            decodeExpr(commit.getParty),
            decodeExpr(commit.getExpr),
            decodeType(commit.getRetType))

        case PLF.Scenario.SumCase.BLOCK =>
          val block = lfScenario.getBlock
          ScenarioBlock(
            bindings = ImmArray(block.getBindingsList.asScala).map(decodeBinding(_)),
            body = decodeExpr(block.getBody))

        case PLF.Scenario.SumCase.GET_TIME =>
          ScenarioGetTime

        case PLF.Scenario.SumCase.PASS =>
          ScenarioPass(decodeExpr(lfScenario.getPass))

        case PLF.Scenario.SumCase.GET_PARTY =>
          ScenarioGetParty(decodeExpr(lfScenario.getGetParty))

        case PLF.Scenario.SumCase.EMBED_EXPR =>
          val embedExpr = lfScenario.getEmbedExpr
          ScenarioEmbedExpr(decodeType(embedExpr.getType), decodeExpr(embedExpr.getBody))

        case PLF.Scenario.SumCase.SUM_NOT_SET =>
          throw ParseError("Scenario.SUM_NOT_SET")
      }

    private[this] def decodeTypeVarWithKind(
        lfTypeVarWithKind: PLF.TypeVarWithKind): (TypeVarName, Kind) = {
      checkIdentifier(lfTypeVarWithKind.getVar)
      lfTypeVarWithKind.getVar -> decodeKind(lfTypeVarWithKind.getKind)
    }

    private[this] def decodeBinding(lfBinding: PLF.Binding): Binding = {
      val (binder, typ) = decodeBinder(lfBinding.getBinder)
      Binding(Some(binder), typ, decodeExpr(lfBinding.getBound))
    }

    private[this] def decodeBinder(lfBinder: PLF.VarWithType): (ExprVarName, Type) = {
      checkIdentifier(lfBinder.getVar)
      lfBinder.getVar -> decodeType(lfBinder.getType)
    }

    private[this] def decodePrimCon(lfPrimCon: PLF.PrimCon): PrimCon =
      lfPrimCon match {
        case PLF.PrimCon.CON_UNIT =>
          PCUnit
        case PLF.PrimCon.CON_FALSE =>
          PCFalse
        case PLF.PrimCon.CON_TRUE =>
          PCTrue
        case _ => throw ParseError("Unknown PrimCon: " + lfPrimCon.toString)
      }

    private[this] def decodePrimLit(lfPrimLit: PLF.PrimLit): PrimLit =
      lfPrimLit.getSumCase match {
        case PLF.PrimLit.SumCase.INT64 =>
          PLInt64(lfPrimLit.getInt64)
        case PLF.PrimLit.SumCase.DECIMAL =>
          checkDecimal(lfPrimLit.getDecimal)
          val d = Decimal.fromString(lfPrimLit.getDecimal)
          d.fold(e => throw ParseError("error parsing decimal: " + e), PLDecimal)
        case PLF.PrimLit.SumCase.TEXT =>
          PLText(lfPrimLit.getText)
        case PLF.PrimLit.SumCase.PARTY =>
          val p = SimpleString
            .fromString(lfPrimLit.getParty)
            .getOrElse(throw ParseError(s"invalid party '${lfPrimLit.getParty}'"))
          PLParty(p)
        case PLF.PrimLit.SumCase.TIMESTAMP =>
          val t = Time.Timestamp.fromLong(lfPrimLit.getTimestamp)
          t.fold(e => throw ParseError("error decoding timestamp: " + e), PLTimestamp)
        case PLF.PrimLit.SumCase.DATE =>
          val d = Time.Date.fromDaysSinceEpoch(lfPrimLit.getDate)
          d.fold(e => throw ParseError("error decoding date: " + e), PLDate)
        case unknown =>
          throw ParseError("Unknown PrimLit: " + unknown.toString)
      }
  }

  private def assertUntil(maxMinorVersion: LanguageMinorVersion, description: String): Unit =
    if (V1.minorVersionOrdering.gt(minor, maxMinorVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertSince(minMinorVersion: LanguageMinorVersion, description: String): Unit =
    if (V1.minorVersionOrdering.lt(minor, minMinorVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def checkNonEmpty(s: Seq[_], description: String): Unit =
    if (s.isEmpty)
      throw ParseError(s"Unexpected empty $description")

}

object DecodeV1 {

  protected[lfpackage] val primTypeTable: Map[PLF.PrimType, (BuiltinType, LanguageMinorVersion)] = {
    import PLF.PrimType._

    Map(
      UNIT -> (BTUnit -> "0"),
      BOOL -> (BTBool -> "0"),
      TEXT -> (BTText -> "0"),
      INT64 -> (BTInt64 -> "0"),
      DECIMAL -> (BTDecimal -> "0"),
      TIMESTAMP -> (BTTimestamp -> "0"),
      PARTY -> (BTParty -> "0"),
      LIST -> (BTList -> "0"),
      UPDATE -> (BTUpdate -> "0"),
      SCENARIO -> (BTScenario -> "0"),
      CONTRACT_ID -> (BTContractId -> "0"),
      DATE -> (BTDate -> "0"),
      OPTIONAL -> (BTOptional -> "1"),
      MAP -> (BTMap -> "3"),
      ARROW -> (BTArrow -> "1"),
    )
  }

  protected[lfpackage] val builtinFunctionMap = {
    import PLF.BuiltinFunction._

    Map[PLF.BuiltinFunction, (Ast.BuiltinFunction, LanguageMinorVersion)](
      ADD_DECIMAL -> (BAddDecimal -> "0"),
      SUB_DECIMAL -> (BSubDecimal -> "0"),
      MUL_DECIMAL -> (BMulDecimal -> "0"),
      DIV_DECIMAL -> (BDivDecimal -> "0"),
      ROUND_DECIMAL -> (BRoundDecimal -> "0"),
      ADD_INT64 -> (BAddInt64 -> "0"),
      SUB_INT64 -> (BSubInt64 -> "0"),
      MUL_INT64 -> (BMulInt64 -> "0"),
      DIV_INT64 -> (BDivInt64 -> "0"),
      MOD_INT64 -> (BModInt64 -> "0"),
      EXP_INT64 -> (BExpInt64 -> "0"),
      INT64_TO_DECIMAL -> (BInt64ToDecimal -> "0"),
      DECIMAL_TO_INT64 -> (BDecimalToInt64 -> "0"),
      FOLDL -> (BFoldl -> "0"),
      FOLDR -> (BFoldr -> "0"),
      MAP_EMPTY -> (BMapEmpty -> "3"),
      MAP_INSERT -> (BMapInsert -> "3"),
      MAP_LOOKUP -> (BMapLookup -> "3"),
      MAP_DELETE -> (BMapDelete -> "3"),
      MAP_TO_LIST -> (BMapToList -> "3"),
      MAP_SIZE -> (BMapSize -> "3"),
      APPEND_TEXT -> (BAppendText -> "0"),
      ERROR -> (BError -> "0"),
      LEQ_INT64 -> (BLessEqInt64 -> "0"),
      LEQ_DECIMAL -> (BLessEqDecimal -> "0"),
      LEQ_TEXT -> (BLessEqText -> "0"),
      LEQ_TIMESTAMP -> (BLessEqTimestamp -> "0"),
      LEQ_PARTY -> (BLessEqParty -> "1"),
      GEQ_INT64 -> (BGreaterEqInt64 -> "0"),
      GEQ_DECIMAL -> (BGreaterEqDecimal -> "0"),
      GEQ_TEXT -> (BGreaterEqText -> "0"),
      GEQ_TIMESTAMP -> (BGreaterEqTimestamp -> "0"),
      GEQ_PARTY -> (BGreaterEqParty -> "1"),
      LESS_INT64 -> (BLessInt64 -> "0"),
      LESS_DECIMAL -> (BLessDecimal -> "0"),
      LESS_TEXT -> (BLessText -> "0"),
      LESS_TIMESTAMP -> (BLessTimestamp -> "0"),
      LESS_PARTY -> (BLessParty -> "1"),
      GREATER_INT64 -> (BGreaterInt64 -> "0"),
      GREATER_DECIMAL -> (BGreaterDecimal -> "0"),
      GREATER_TEXT -> (BGreaterText -> "0"),
      GREATER_TIMESTAMP -> (BGreaterTimestamp -> "0"),
      GREATER_PARTY -> (BGreaterParty -> "1"),
      TO_TEXT_INT64 -> (BToTextInt64 -> "0"),
      TO_TEXT_DECIMAL -> (BToTextDecimal -> "0"),
      TO_TEXT_TIMESTAMP -> (BToTextTimestamp -> "0"),
      TO_TEXT_PARTY -> (BToTextParty -> "2"),
      TO_TEXT_TEXT -> (BToTextText -> "0"),
      TO_QUOTED_TEXT_PARTY -> (BToQuotedTextParty -> "0"),
      FROM_TEXT_PARTY -> (BFromTextParty -> "2"),
      SHA256_TEXT -> (BSHA256Text -> "2"),
      DATE_TO_UNIX_DAYS -> (BDateToUnixDays -> "0"),
      EXPLODE_TEXT -> (BExplodeText -> "0"),
      IMPLODE_TEXT -> (BImplodeText -> "0"),
      GEQ_DATE -> (BGreaterEqDate -> "0"),
      LEQ_DATE -> (BLessEqDate -> "0"),
      LESS_DATE -> (BLessDate -> "0"),
      TIMESTAMP_TO_UNIX_MICROSECONDS -> (BTimestampToUnixMicroseconds -> "0"),
      TO_TEXT_DATE -> (BToTextDate -> "0"),
      UNIX_DAYS_TO_DATE -> (BUnixDaysToDate -> "0"),
      UNIX_MICROSECONDS_TO_TIMESTAMP -> (BUnixMicrosecondsToTimestamp -> "0"),
      GREATER_DATE -> (BGreaterDate -> "0"),
      EQUAL_INT64 -> (BEqualInt64 -> "0"),
      EQUAL_DECIMAL -> (BEqualDecimal -> "0"),
      EQUAL_TEXT -> (BEqualText -> "0"),
      EQUAL_TIMESTAMP -> (BEqualTimestamp -> "0"),
      EQUAL_DATE -> (BEqualDate -> "0"),
      EQUAL_PARTY -> (BEqualParty -> "0"),
      EQUAL_BOOL -> (BEqualBool -> "0"),
      EQUAL_LIST -> (BEqualList -> "0"),
      EQUAL_CONTRACT_ID -> (BEqualContractId -> "0"),
      TRACE -> (BTrace -> "0"),
    ).withDefault(_ => throw ParseError("BuiltinFunction.UNRECOGNIZED"))
  }

}
