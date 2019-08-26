// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import com.digitalasset.daml.lf.archive.Decode.ParseError
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{Decimal, ImmArray, Numeric, Time}
import ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language.{LanguageVersion => LV}
import com.digitalasset.daml_lf.{DamlLf1 => PLF}
import com.google.protobuf.CodedInputStream

import scala.collection.JavaConverters._
import scala.collection.{breakOut, mutable}

private[archive] class DecodeV1(minor: LV.Minor) extends Decode.OfPackage[PLF.Package] {

  import Decode._, DecodeV1._

  private val languageVersion = LV(LV.Major.V1, minor)

  private def name(s: String): Name = eitherToParseError(Name.fromString(s))

  override def decodePackage(
      packageId: PackageId,
      lfPackage: PLF.Package,
      onlySerializableDataDefs: Boolean
  ): Package = {
    val interned = decodeInternedPackageIds(lfPackage.getInternedPackageIdsList.asScala)
    Package(
      lfPackage.getModulesList.asScala
        .map(ModuleDecoder(packageId, interned, _, onlySerializableDataDefs).decode))
  }

  type ProtoModule = PLF.Module

  override def protoModule(cis: CodedInputStream): ProtoModule =
    PLF.Module.parser().parseFrom(cis)

  override def decodeScenarioModule(packageId: PackageId, lfModule: ProtoModule): Module =
    ModuleDecoder(packageId, ImmArraySeq.empty, lfModule, onlySerializableDataDefs = false).decode()

  private[this] def eitherToParseError[A](x: Either[String, A]): A =
    x.fold(err => throw new ParseError(err), identity)

  private[this] def decodeInternedPackageIds(internedList: Seq[String]): ImmArraySeq[PackageId] = {
    if (internedList.nonEmpty)
      assertSince(LV.Features.internedIds, "interned package ID table")
    internedList.map(s => eitherToParseError(PackageId.fromString(s)))(breakOut)
  }

  private[this] def decodeSegments(segments: ImmArray[String]): DottedName =
    DottedName.fromSegments(segments.toSeq) match {
      case Left(err) => throw new ParseError(err)
      case Right(x) => x
    }

  case class ModuleDecoder(
      packageId: PackageId,
      internedPackageIds: ImmArraySeq[PackageId],
      lfModule: PLF.Module,
      onlySerializableDataDefs: Boolean
  ) {

    val moduleName = eitherToParseError(
      ModuleName.fromSegments(lfModule.getName.getSegmentsList.asScala))

    // FIXME(JM): rewrite.
    var currentDefinitionRef: Option[DefinitionRef] = None

    def decode(): Module = {
      val defs = mutable.ArrayBuffer[(DottedName, Definition)]()
      val templates = mutable.ArrayBuffer[(DottedName, Template)]()

      // collect data types
      lfModule.getDataTypesList.asScala
        .filter(!onlySerializableDataDefs || _.getSerializable)
        .foreach { defn =>
          val defName =
            eitherToParseError(DottedName.fromSegments(defn.getName.getSegmentsList.asScala))
          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = decodeDefDataType(defn)
          defs += (defName -> d)
        }

      if (!onlySerializableDataDefs) {
        // collect values
        lfModule.getValuesList.asScala.foreach { defn =>
          val defName =
            decodeSegments(ImmArray(defn.getNameWithType.getNameList.asScala))
          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = decodeDefValue(defn)
          defs += (defName -> d)
        }
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
        forbidPartyLiterals = flags.getForbidPartyLiterals
      )
    }

    private[this] def decodeDefDataType(lfDataType: PLF.DefDataType): DDataType = {
      val params = ImmArray(lfDataType.getParamsList.asScala).map(decodeTypeVarWithKind)
      DDataType(
        lfDataType.getSerializable,
        params,
        lfDataType.getDataConsCase match {
          case PLF.DefDataType.DataConsCase.RECORD =>
            DataRecord(decodeFields(ImmArray(lfDataType.getRecord.getFieldsList.asScala)), None)
          case PLF.DefDataType.DataConsCase.VARIANT =>
            DataVariant(decodeFields(ImmArray(lfDataType.getVariant.getFieldsList.asScala)))
          case PLF.DefDataType.DataConsCase.ENUM =>
            assertSince(LV.Features.enum, "DefDataType.DataCons.Enum")
            assertEmpty(params.toSeq, "params")
            DataEnum(decodeEnumCons(ImmArray(lfDataType.getEnum.getConstructorsList.asScala)))
          case PLF.DefDataType.DataConsCase.DATACONS_NOT_SET =>
            throw ParseError("DefDataType.DATACONS_NOT_SET")

        }
      )
    }

    private[this] def decodeFields(lfFields: ImmArray[PLF.FieldWithType]): ImmArray[(Name, Type)] =
      lfFields.map(field => name(field.getField) -> decodeType(field.getType))

    private[this] def decodeEnumCons(cons: ImmArray[String]): ImmArray[EnumConName] =
      cons.map(name)

    private[this] def decodeDefValue(lfValue: PLF.DefValue): DValue = {
      val definition = lfValue.getNameWithType.getNameList.asScala.mkString(".")
      DValue(
        typ = decodeType(lfValue.getNameWithType.getType),
        noPartyLiterals = lfValue.getNoPartyLiterals,
        body = decodeExpr(lfValue.getExpr, definition),
        isTest = lfValue.getIsTest
      )
    }

    private def decodeLocation(lfExpr: PLF.Expr, definition: String): Option[Location] =
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
            definition,
            (range.getStartLine, range.getStartCol),
            (range.getEndLine, range.getEndCol)))
      } else {
        None
      }

    private[this] def decodeTemplateKey(
        tpl: String,
        key: PLF.DefTemplate.DefKey,
        tplVar: ExprVarName): TemplateKey = {
      assertSince(LV.Features.contractKeys, "DefTemplate.DefKey")
      val keyExpr = key.getKeyExprCase match {
        case PLF.DefTemplate.DefKey.KeyExprCase.KEY =>
          decodeKeyExpr(key.getKey, tplVar)
        case PLF.DefTemplate.DefKey.KeyExprCase.COMPLEX_KEY => {
          assertSince(LV.Features.complexContactKeys, "DefTemplate.DefKey.complex_key")
          decodeExpr(key.getComplexKey, s"${tpl}:key")
        }
        case PLF.DefTemplate.DefKey.KeyExprCase.KEYEXPR_NOT_SET =>
          throw ParseError("DefKey.KEYEXPR_NOT_SET")
      }
      TemplateKey(
        decodeType(key.getType),
        keyExpr,
        maintainers = decodeExpr(key.getMaintainers, s"${tpl}:maintainer")
      )
    }

    private[this] def decodeKeyExpr(expr: PLF.KeyExpr, tplVar: ExprVarName): Expr = {
      expr.getSumCase match {
        case PLF.KeyExpr.SumCase.RECORD =>
          val recCon = expr.getRecord
          ERecCon(
            tycon = decodeTypeConApp(recCon.getTycon),
            fields = ImmArray(recCon.getFieldsList.asScala).map(field =>
              name(field.getField) -> decodeKeyExpr(field.getExpr, tplVar))
          )

        case PLF.KeyExpr.SumCase.PROJECTIONS =>
          val lfProjs = expr.getProjections.getProjectionsList.asScala
          lfProjs.foldLeft(EVar(tplVar): Expr)((acc, lfProj) =>
            ERecProj(decodeTypeConApp(lfProj.getTycon), name(lfProj.getField), acc))

        case PLF.KeyExpr.SumCase.SUM_NOT_SET =>
          throw ParseError("KeyExpr.SUM_NOT_SET")
      }
    }

    private[this] def decodeTemplate(lfTempl: PLF.DefTemplate): Template = {
      val tpl = lfTempl.getTycon.getSegmentsList.asScala.mkString(".")

      Template(
        param = name(lfTempl.getParam),
        precond = if (lfTempl.hasPrecond) decodeExpr(lfTempl.getPrecond, s"$tpl:ensure") else ETrue,
        signatories = decodeExpr(lfTempl.getSignatories, s"$tpl.signatory"),
        agreementText = decodeExpr(lfTempl.getAgreement, s"$tpl:agreement"),
        choices = lfTempl.getChoicesList.asScala
          .map(decodeChoice(tpl, _))
          .map(ch => (ch.name, ch)),
        observers = decodeExpr(lfTempl.getObservers, s"$tpl:observer"),
        key =
          if (lfTempl.hasKey) Some(decodeTemplateKey(tpl, lfTempl.getKey, name(lfTempl.getParam)))
          else None
      )
    }

    private[this] def decodeChoice(tpl: String, lfChoice: PLF.TemplateChoice): TemplateChoice = {
      val (v, t) = decodeBinder(lfChoice.getArgBinder)
      val chName = lfChoice.getName

      TemplateChoice(
        name = name(chName),
        consuming = lfChoice.getConsuming,
        controllers = decodeExpr(lfChoice.getControllers, s"$tpl:$chName:controller"),
        selfBinder = name(lfChoice.getSelfBinder),
        argBinder = Some(v) -> t,
        returnType = decodeType(lfChoice.getRetType),
        update = decodeExpr(lfChoice.getUpdate, s"$tpl:$chName:choice")
      )
    }

    private[lf] def decodeKind(lfKind: PLF.Kind): Kind =
      lfKind.getSumCase match {
        case PLF.Kind.SumCase.STAR => KStar
        case PLF.Kind.SumCase.NAT =>
          assertSince(LV.Features.numeric, "Kind.NAT")
          KNat
        case PLF.Kind.SumCase.ARROW =>
          val kArrow = lfKind.getArrow
          val params = kArrow.getParamsList.asScala
          assertNonEmpty(params, "params")
          (params :\ decodeKind(kArrow.getResult))((param, kind) => KArrow(decodeKind(param), kind))
        case PLF.Kind.SumCase.SUM_NOT_SET =>
          throw ParseError("Kind.SUM_NOT_SET")
      }

    private[lf] def decodeType(lfType: PLF.Type): Type =
      lfType.getSumCase match {
        case PLF.Type.SumCase.VAR =>
          val tvar = lfType.getVar
          tvar.getArgsList.asScala
            .foldLeft[Type](TVar(name(tvar.getVar)))((typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.NAT =>
          assertSince(LV.Features.numeric, "Type.NAT")
          if (0 <= lfType.getNat && lfType.getNat <= Numeric.maxPrecision)
            TNat(lfType.getNat.toInt)
          else
            throw ParseError(
              s"TNat must be between 0 and ${Numeric.maxPrecision}, found ${lfType.getNat}")
        case PLF.Type.SumCase.CON =>
          val tcon = lfType.getCon
          (TTyCon(decodeTypeConName(tcon.getTycon)) /: [Type] tcon.getArgsList.asScala)(
            (typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.PRIM =>
          val prim = lfType.getPrim
          val baseType =
            if (prim.getPrim == PLF.PrimType.DECIMAL) {
              // FixMe: https://github.com/digital-asset/daml/issues/2289
              //   enable the check once the compiler produces proper DAML-LF 1.dev
              // assertUntil(LV.Features.numeric, "PrimType.DECIMAL")
              TDecimal
            } else {
              val info = builtinTypeInfoMap(prim.getPrim)
              assertSince(info.minVersion, prim.getPrim.getValueDescriptor.getFullName)
              TBuiltin(info.bTyp)
            }
          (baseType /: [Type] prim.getArgsList.asScala)((typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.FUN =>
          assertUntil(LV.Features.arrowType, "Type.Fun")
          val tFun = lfType.getFun
          val params = tFun.getParamsList.asScala
          assertNonEmpty(params, "params")
          (params :\ decodeType(tFun.getResult))((param, res) => TFun(decodeType(param), res))
        case PLF.Type.SumCase.FORALL =>
          val tForall = lfType.getForall
          val vars = tForall.getVarsList.asScala
          assertNonEmpty(vars, "vars")
          (vars :\ decodeType(tForall.getBody))((binder, acc) =>
            TForall(decodeTypeVarWithKind(binder), acc))
        case PLF.Type.SumCase.TUPLE =>
          val tuple = lfType.getTuple
          val fields = tuple.getFieldsList.asScala
          assertNonEmpty(fields, "fields")
          TTuple(
            ImmArray(fields.map(ft => name(ft.getField) -> decodeType(ft.getType)))
          )

        case PLF.Type.SumCase.SUM_NOT_SET =>
          throw ParseError("Type.SUM_NOT_SET")
      }

    private[this] def decodeModuleRef(lfRef: PLF.ModuleRef): (PackageId, ModuleName) = {
      val modName = eitherToParseError(
        ModuleName.fromSegments(lfRef.getModuleName.getSegmentsList.asScala))
      import PLF.PackageRef.{SumCase => SC}
      val pkgId = lfRef.getPackageRef.getSumCase match {
        case SC.SELF =>
          this.packageId
        case SC.PACKAGE_ID =>
          val rawPid = lfRef.getPackageRef.getPackageId
          PackageId
            .fromString(rawPid)
            .getOrElse(throw ParseError(s"invalid packageId '$rawPid'"))
        case SC.INTERNED_ID =>
          assertSince(LV.Features.internedIds, "interned package ID")
          val iidl = lfRef.getPackageRef.getInternedId
          def outOfRange = ParseError(s"invalid package ID table index $iidl")
          val iid = iidl.toInt
          if (iidl != iid.toLong) throw outOfRange
          internedPackageIds.lift(iid).getOrElse(throw outOfRange)
        case SC.SUM_NOT_SET =>
          throw ParseError("PackageRef.SUM_NOT_SET")
      }
      (pkgId, modName)
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
      Identifier(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConApp(lfTyConApp: PLF.Type.Con): TypeConApp =
      TypeConApp(
        decodeTypeConName(lfTyConApp.getTycon),
        ImmArray(lfTyConApp.getArgsList.asScala.map(decodeType))
      )

    private[lf] def decodeExpr(lfExpr: PLF.Expr, definition: String): Expr =
      decodeLocation(lfExpr, definition) match {
        case None => decodeExprBody(lfExpr, definition)
        case Some(loc) => ELocation(loc, decodeExprBody(lfExpr, definition))
      }

    private[this] def decodeExprBody(lfExpr: PLF.Expr, definition: String): Expr =
      lfExpr.getSumCase match {
        case PLF.Expr.SumCase.VAR =>
          EVar(name(lfExpr.getVar))

        case PLF.Expr.SumCase.VAL =>
          EVal(decodeValName(lfExpr.getVal))

        case PLF.Expr.SumCase.PRIM_LIT =>
          EPrimLit(decodePrimLit(lfExpr.getPrimLit))

        case PLF.Expr.SumCase.PRIM_CON =>
          lfExpr.getPrimCon match {
            case PLF.PrimCon.CON_UNIT => EUnit
            case PLF.PrimCon.CON_FALSE => EFalse
            case PLF.PrimCon.CON_TRUE => ETrue
            case PLF.PrimCon.UNRECOGNIZED =>
              throw ParseError("PrimCon.UNRECOGNIZED")
          }

        case PLF.Expr.SumCase.BUILTIN =>
          val info = DecodeV1.builtinInfoMap(lfExpr.getBuiltin)
          assertSince(info.minVersion, lfExpr.getBuiltin.getValueDescriptor.getFullName)

          if (info.handleLegacyDecimal) {
            // FixMe: https://github.com/digital-asset/daml/issues/2289
            //   enable the check once the compiler produces proper DAML-LF 1.dev
            // info.maxVersion.foreach(assertUntil(_, lfExpr.getBuiltin.getValueDescriptor.getFullName))
            ETyApp(EBuiltin(info.builtin), TDecimalScale)
          } else {
            info.maxVersion.foreach(
              assertUntil(_, lfExpr.getBuiltin.getValueDescriptor.getFullName))
            EBuiltin(info.builtin)
          }

        case PLF.Expr.SumCase.REC_CON =>
          val recCon = lfExpr.getRecCon
          ERecCon(
            tycon = decodeTypeConApp(recCon.getTycon),
            fields = ImmArray(recCon.getFieldsList.asScala).map(field =>
              name(field.getField) -> decodeExpr(field.getExpr, definition))
          )

        case PLF.Expr.SumCase.REC_PROJ =>
          val recProj = lfExpr.getRecProj
          ERecProj(
            tycon = decodeTypeConApp(recProj.getTycon),
            field = name(recProj.getField),
            record = decodeExpr(recProj.getRecord, definition))

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          ERecUpd(
            tycon = decodeTypeConApp(recUpd.getTycon),
            field = name(recUpd.getField),
            record = decodeExpr(recUpd.getRecord, definition),
            update = decodeExpr(recUpd.getUpdate, definition)
          )

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          EVariantCon(
            decodeTypeConApp(varCon.getTycon),
            name(varCon.getVariantCon),
            decodeExpr(varCon.getVariantArg, definition))

        case PLF.Expr.SumCase.ENUM_CON =>
          assertSince(LV.Features.enum, "Expr.Enum")
          val enumCon = lfExpr.getEnumCon
          EEnumCon(
            decodeTypeConName(enumCon.getTycon),
            name(enumCon.getEnumCon)
          )

        case PLF.Expr.SumCase.TUPLE_CON =>
          val tupleCon = lfExpr.getTupleCon
          ETupleCon(
            ImmArray(tupleCon.getFieldsList.asScala).map(field =>
              name(field.getField) -> decodeExpr(field.getExpr, definition))
          )

        case PLF.Expr.SumCase.TUPLE_PROJ =>
          val tupleProj = lfExpr.getTupleProj
          ETupleProj(name(tupleProj.getField), decodeExpr(tupleProj.getTuple, definition))

        case PLF.Expr.SumCase.TUPLE_UPD =>
          val tupleUpd = lfExpr.getTupleUpd
          ETupleUpd(
            field = name(tupleUpd.getField),
            tuple = decodeExpr(tupleUpd.getTuple, definition),
            update = decodeExpr(tupleUpd.getUpdate, definition))

        case PLF.Expr.SumCase.APP =>
          val app = lfExpr.getApp
          val args = app.getArgsList.asScala
          assertNonEmpty(args, "args")
          (decodeExpr(app.getFun, definition) /: args)((e, arg) =>
            EApp(e, decodeExpr(arg, definition)))

        case PLF.Expr.SumCase.ABS =>
          val lfAbs = lfExpr.getAbs
          val params = lfAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          // val params = lfAbs.getParamList.asScala.map(decodeBinder)
          (params :\ decodeExpr(lfAbs.getBody, definition))((param, e) =>
            EAbs(decodeBinder(param), e, currentDefinitionRef))

        case PLF.Expr.SumCase.TY_APP =>
          val tyapp = lfExpr.getTyApp
          val args = tyapp.getTypesList.asScala
          assertNonEmpty(args, "args")
          (decodeExpr(tyapp.getExpr, definition) /: args)((e, arg) => ETyApp(e, decodeType(arg)))

        case PLF.Expr.SumCase.TY_ABS =>
          val lfTyAbs = lfExpr.getTyAbs
          val params = lfTyAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          (params :\ decodeExpr(lfTyAbs.getBody, definition))((param, e) =>
            ETyAbs(decodeTypeVarWithKind(param), e))

        case PLF.Expr.SumCase.LET =>
          val lfLet = lfExpr.getLet
          val bindings = lfLet.getBindingsList.asScala
          assertNonEmpty(bindings, "bindings")
          (bindings :\ decodeExpr(lfLet.getBody, definition))((binding, e) => {
            val (v, t) = decodeBinder(binding.getBinder)
            ELet(Binding(Some(v), t, decodeExpr(binding.getBound, definition)), e)
          })

        case PLF.Expr.SumCase.NIL =>
          ENil(decodeType(lfExpr.getNil.getType))

        case PLF.Expr.SumCase.CONS =>
          val cons = lfExpr.getCons
          val front = cons.getFrontList.asScala
          assertNonEmpty(front, "front")
          val typ = decodeType(cons.getType)
          ECons(
            typ,
            ImmArray(front.map(decodeExpr(_, definition))),
            decodeExpr(cons.getTail, definition))

        case PLF.Expr.SumCase.CASE =>
          val case_ = lfExpr.getCase
          ECase(
            decodeExpr(case_.getScrut, definition),
            ImmArray(case_.getAltsList.asScala).map(decodeCaseAlt(_, definition))
          )

        case PLF.Expr.SumCase.UPDATE =>
          EUpdate(decodeUpdate(lfExpr.getUpdate, definition))

        case PLF.Expr.SumCase.SCENARIO =>
          EScenario(decodeScenario(lfExpr.getScenario, definition))

        case PLF.Expr.SumCase.OPTIONAL_NONE =>
          assertSince(LV.Features.optional, "Expr.OptionalNone")
          ENone(decodeType(lfExpr.getOptionalNone.getType))

        case PLF.Expr.SumCase.OPTIONAL_SOME =>
          assertSince(LV.Features.optional, "Expr.OptionalSome")
          val some = lfExpr.getOptionalSome
          ESome(decodeType(some.getType), decodeExpr(some.getBody, definition))

        case PLF.Expr.SumCase.SUM_NOT_SET =>
          throw ParseError("Expr.SUM_NOT_SET")
      }

    private[this] def decodeCaseAlt(lfCaseAlt: PLF.CaseAlt, definition: String): CaseAlt = {
      val pat: CasePat = lfCaseAlt.getSumCase match {
        case PLF.CaseAlt.SumCase.DEFAULT =>
          CPDefault
        case PLF.CaseAlt.SumCase.VARIANT =>
          val variant = lfCaseAlt.getVariant
          CPVariant(
            decodeTypeConName(variant.getCon),
            name(variant.getVariant),
            name(variant.getBinder))
        case PLF.CaseAlt.SumCase.ENUM =>
          assertSince(LV.Features.enum, "CaseAlt.Enum")
          val enum = lfCaseAlt.getEnum
          CPEnum(decodeTypeConName(enum.getCon), name(enum.getConstructor))
        case PLF.CaseAlt.SumCase.PRIM_CON =>
          CPPrimCon(decodePrimCon(lfCaseAlt.getPrimCon))
        case PLF.CaseAlt.SumCase.NIL =>
          CPNil
        case PLF.CaseAlt.SumCase.CONS =>
          val cons = lfCaseAlt.getCons
          CPCons(name(cons.getVarHead), name(cons.getVarTail))

        case PLF.CaseAlt.SumCase.OPTIONAL_NONE =>
          assertSince(LV.Features.optional, "CaseAlt.OptionalNone")
          CPNone

        case PLF.CaseAlt.SumCase.OPTIONAL_SOME =>
          assertSince(LV.Features.optional, "CaseAlt.OptionalSome")
          CPSome(name(lfCaseAlt.getOptionalSome.getVarBody))

        case PLF.CaseAlt.SumCase.SUM_NOT_SET =>
          throw ParseError("CaseAlt.SUM_NOT_SET")
      }
      CaseAlt(pat, decodeExpr(lfCaseAlt.getBody, definition))
    }

    private[this] def decodeRetrieveByKey(
        value: PLF.Update.RetrieveByKey,
        definition: String): RetrieveByKey = {
      RetrieveByKey(
        decodeTypeConName(value.getTemplate),
        decodeExpr(value.getKey, definition),
      )
    }

    private[this] def decodeUpdate(lfUpdate: PLF.Update, definition: String): Update =
      lfUpdate.getSumCase match {

        case PLF.Update.SumCase.PURE =>
          val pure = lfUpdate.getPure
          UpdatePure(decodeType(pure.getType), decodeExpr(pure.getExpr, definition))

        case PLF.Update.SumCase.BLOCK =>
          val block = lfUpdate.getBlock
          UpdateBlock(
            bindings = ImmArray(block.getBindingsList.asScala.map(decodeBinding(_, definition))),
            body = decodeExpr(block.getBody, definition))

        case PLF.Update.SumCase.CREATE =>
          val create = lfUpdate.getCreate
          UpdateCreate(
            templateId = decodeTypeConName(create.getTemplate),
            arg = decodeExpr(create.getExpr, definition))

        case PLF.Update.SumCase.EXERCISE =>
          val exercise = lfUpdate.getExercise
          UpdateExercise(
            templateId = decodeTypeConName(exercise.getTemplate),
            choice = name(exercise.getChoice),
            cidE = decodeExpr(exercise.getCid, definition),
            actorsE =
              if (exercise.hasActor)
                Some(decodeExpr(exercise.getActor, definition))
              else {
                assertSince(LV.Features.optionalExerciseActor, "Update.Exercise.actors optional")
                None
              },
            argE = decodeExpr(exercise.getArg, definition)
          )

        case PLF.Update.SumCase.GET_TIME =>
          UpdateGetTime

        case PLF.Update.SumCase.FETCH =>
          val fetch = lfUpdate.getFetch
          UpdateFetch(
            templateId = decodeTypeConName(fetch.getTemplate),
            contractId = decodeExpr(fetch.getCid, definition))

        case PLF.Update.SumCase.FETCH_BY_KEY =>
          assertSince(LV.Features.contractKeys, "fetchByKey")
          UpdateFetchByKey(decodeRetrieveByKey(lfUpdate.getFetchByKey, definition))

        case PLF.Update.SumCase.LOOKUP_BY_KEY =>
          assertSince(LV.Features.contractKeys, "lookupByKey")
          UpdateLookupByKey(decodeRetrieveByKey(lfUpdate.getLookupByKey, definition))

        case PLF.Update.SumCase.EMBED_EXPR =>
          val embedExpr = lfUpdate.getEmbedExpr
          UpdateEmbedExpr(decodeType(embedExpr.getType), decodeExpr(embedExpr.getBody, definition))

        case PLF.Update.SumCase.SUM_NOT_SET =>
          throw ParseError("Update.SUM_NOT_SET")
      }

    private[this] def decodeScenario(lfScenario: PLF.Scenario, definition: String): Scenario =
      lfScenario.getSumCase match {
        case PLF.Scenario.SumCase.PURE =>
          val pure = lfScenario.getPure
          ScenarioPure(decodeType(pure.getType), decodeExpr(pure.getExpr, definition))

        case PLF.Scenario.SumCase.COMMIT =>
          val commit = lfScenario.getCommit
          ScenarioCommit(
            decodeExpr(commit.getParty, definition),
            decodeExpr(commit.getExpr, definition),
            decodeType(commit.getRetType))

        case PLF.Scenario.SumCase.MUSTFAILAT =>
          val commit = lfScenario.getMustFailAt
          ScenarioMustFailAt(
            decodeExpr(commit.getParty, definition),
            decodeExpr(commit.getExpr, definition),
            decodeType(commit.getRetType))

        case PLF.Scenario.SumCase.BLOCK =>
          val block = lfScenario.getBlock
          ScenarioBlock(
            bindings = ImmArray(block.getBindingsList.asScala).map(decodeBinding(_, definition)),
            body = decodeExpr(block.getBody, definition))

        case PLF.Scenario.SumCase.GET_TIME =>
          ScenarioGetTime

        case PLF.Scenario.SumCase.PASS =>
          ScenarioPass(decodeExpr(lfScenario.getPass, definition))

        case PLF.Scenario.SumCase.GET_PARTY =>
          ScenarioGetParty(decodeExpr(lfScenario.getGetParty, definition))

        case PLF.Scenario.SumCase.EMBED_EXPR =>
          val embedExpr = lfScenario.getEmbedExpr
          ScenarioEmbedExpr(
            decodeType(embedExpr.getType),
            decodeExpr(embedExpr.getBody, definition))

        case PLF.Scenario.SumCase.SUM_NOT_SET =>
          throw ParseError("Scenario.SUM_NOT_SET")
      }

    private[this] def decodeTypeVarWithKind(
        lfTypeVarWithKind: PLF.TypeVarWithKind): (TypeVarName, Kind) =
      name(lfTypeVarWithKind.getVar) -> decodeKind(lfTypeVarWithKind.getKind)

    private[this] def decodeBinding(lfBinding: PLF.Binding, definition: String): Binding = {
      val (binder, typ) = decodeBinder(lfBinding.getBinder)
      Binding(Some(binder), typ, decodeExpr(lfBinding.getBound, definition))
    }

    private[this] def decodeBinder(lfBinder: PLF.VarWithType): (ExprVarName, Type) =
      name(lfBinder.getVar) -> decodeType(lfBinder.getType)

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
          // FixMe: https://github.com/digital-asset/daml/issues/2289
          //   enable the check once the compiler produces proper DAML-LF 1.dev
          // assertUntil(LV.Features.numeric, "PrimLit.decimal")
          Decimal
            .fromString(lfPrimLit.getDecimal)
            .flatMap(Numeric.fromBigDecimal(Decimal.scale, _))
            .fold(e => throw ParseError("error parsing decimal: " + e), PLNumeric)
        case PLF.PrimLit.SumCase.NUMERIC =>
          assertSince(LV.Features.numeric, "PrimLit.numeric")
          Numeric
            .fromString(lfPrimLit.getNumeric)
            .fold(e => throw ParseError("error parsing numeric: " + e), PLNumeric)
        case PLF.PrimLit.SumCase.TEXT =>
          PLText(lfPrimLit.getText)
        case PLF.PrimLit.SumCase.PARTY =>
          val p = Party
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

  private def versionIsOlderThan(minVersion: LV): Boolean =
    LV.ordering.lt(languageVersion, minVersion)

  private def assertUntil(maxVersion: LV, description: String): Unit =
    if (!versionIsOlderThan(maxVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertSince(minVersion: LV, description: String): Unit =
    if (versionIsOlderThan(minVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertNonEmpty(s: Seq[_], description: String): Unit =
    if (s.isEmpty) throw ParseError(s"Unexpected empty $description")

  private def assertEmpty(s: Seq[_], description: String): Unit =
    if (s.nonEmpty) throw ParseError(s"Unexpected non-empty $description")

}

private[lf] object DecodeV1 {

  case class BuiltinTypeInfo(
      proto: PLF.PrimType,
      bTyp: BuiltinType,
      minVersion: LV = LV.Features.default
  )

  val builtinTypeInfos: List[BuiltinTypeInfo] = {
    import PLF.PrimType._, LV.Features._
    // DECIMAL is not there and should be handled in an ad-hoc way.
    List(
      BuiltinTypeInfo(UNIT, BTUnit),
      BuiltinTypeInfo(BOOL, BTBool),
      BuiltinTypeInfo(TEXT, BTText),
      BuiltinTypeInfo(INT64, BTInt64),
      BuiltinTypeInfo(TIMESTAMP, BTTimestamp),
      BuiltinTypeInfo(PARTY, BTParty),
      BuiltinTypeInfo(LIST, BTList),
      BuiltinTypeInfo(UPDATE, BTUpdate),
      BuiltinTypeInfo(SCENARIO, BTScenario),
      BuiltinTypeInfo(CONTRACT_ID, BTContractId),
      BuiltinTypeInfo(DATE, BTDate),
      BuiltinTypeInfo(OPTIONAL, BTOptional, minVersion = optional),
      BuiltinTypeInfo(MAP, BTMap, minVersion = optional),
      BuiltinTypeInfo(ARROW, BTArrow, minVersion = arrowType),
      BuiltinTypeInfo(NUMERIC, BTNumeric, minVersion = numeric)
    )
  }

  private val builtinTypeInfoMap =
    builtinTypeInfos
      .map(info => info.proto -> info)
      .toMap

  case class BuiltinFunctionInfo(
      proto: PLF.BuiltinFunction,
      builtin: BuiltinFunction,
      minVersion: LV = LV.Features.default, // first version that does support the builtin
      maxVersion: Option[LV] = None, // first version that does not support the builtin
      handleLegacyDecimal: Boolean = false
  )

  val builtinFunctionInfos: List[BuiltinFunctionInfo] = {
    import PLF.BuiltinFunction._, LV.Features._
    List(
      BuiltinFunctionInfo(
        ADD_DECIMAL,
        BAddNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(
        SUB_DECIMAL,
        BSubNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(
        MUL_DECIMAL,
        BMulNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(
        DIV_DECIMAL,
        BDivNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(
        ROUND_DECIMAL,
        BRoundNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(ADD_NUMERIC, BAddNumeric, minVersion = numeric),
      BuiltinFunctionInfo(SUB_NUMERIC, BSubNumeric, minVersion = numeric),
      BuiltinFunctionInfo(MUL_NUMERIC, BMulNumeric, minVersion = numeric),
      BuiltinFunctionInfo(DIV_NUMERIC, BDivNumeric, minVersion = numeric),
      BuiltinFunctionInfo(ROUND_NUMERIC, BRoundNumeric, minVersion = numeric),
      BuiltinFunctionInfo(ADD_INT64, BAddInt64),
      BuiltinFunctionInfo(SUB_INT64, BSubInt64),
      BuiltinFunctionInfo(MUL_INT64, BMulInt64),
      BuiltinFunctionInfo(DIV_INT64, BDivInt64),
      BuiltinFunctionInfo(MOD_INT64, BModInt64),
      BuiltinFunctionInfo(EXP_INT64, BExpInt64),
      BuiltinFunctionInfo(
        INT64_TO_DECIMAL,
        BInt64ToNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(
        DECIMAL_TO_INT64,
        BNumericToInt64,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(INT64_TO_NUMERIC, BInt64ToNumeric, minVersion = numeric),
      BuiltinFunctionInfo(NUMERIC_TO_INT64, BNumericToInt64, minVersion = numeric),
      BuiltinFunctionInfo(FOLDL, BFoldl),
      BuiltinFunctionInfo(FOLDR, BFoldr),
      BuiltinFunctionInfo(MAP_EMPTY, BMapEmpty, minVersion = map),
      BuiltinFunctionInfo(MAP_INSERT, BMapInsert, minVersion = map),
      BuiltinFunctionInfo(MAP_LOOKUP, BMapLookup, minVersion = map),
      BuiltinFunctionInfo(MAP_DELETE, BMapDelete, minVersion = map),
      BuiltinFunctionInfo(MAP_TO_LIST, BMapToList, minVersion = map),
      BuiltinFunctionInfo(MAP_SIZE, BMapSize, minVersion = map),
      BuiltinFunctionInfo(APPEND_TEXT, BAppendText),
      BuiltinFunctionInfo(ERROR, BError),
      BuiltinFunctionInfo(LEQ_INT64, BLessEqInt64),
      BuiltinFunctionInfo(
        LEQ_DECIMAL,
        BLessEqNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(LEQ_NUMERIC, BLessEqNumeric, minVersion = numeric),
      BuiltinFunctionInfo(LEQ_TEXT, BLessEqText),
      BuiltinFunctionInfo(LEQ_TIMESTAMP, BLessEqTimestamp),
      BuiltinFunctionInfo(LEQ_PARTY, BLessEqParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(GEQ_INT64, BGreaterEqInt64),
      BuiltinFunctionInfo(
        GEQ_DECIMAL,
        BGreaterEqNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(GEQ_NUMERIC, BGreaterEqNumeric, minVersion = numeric),
      BuiltinFunctionInfo(GEQ_TEXT, BGreaterEqText),
      BuiltinFunctionInfo(GEQ_TIMESTAMP, BGreaterEqTimestamp),
      BuiltinFunctionInfo(GEQ_PARTY, BGreaterEqParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(LESS_INT64, BLessInt64),
      BuiltinFunctionInfo(
        LESS_DECIMAL,
        BLessNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(LESS_NUMERIC, BLessNumeric, minVersion = numeric),
      BuiltinFunctionInfo(LESS_TEXT, BLessText),
      BuiltinFunctionInfo(LESS_TIMESTAMP, BLessTimestamp),
      BuiltinFunctionInfo(LESS_PARTY, BLessParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(GREATER_INT64, BGreaterInt64),
      BuiltinFunctionInfo(
        GREATER_DECIMAL,
        BGreaterNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(GREATER_NUMERIC, BGreaterNumeric, minVersion = numeric),
      BuiltinFunctionInfo(GREATER_TEXT, BGreaterText),
      BuiltinFunctionInfo(GREATER_TIMESTAMP, BGreaterTimestamp),
      BuiltinFunctionInfo(GREATER_PARTY, BGreaterParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(TO_TEXT_INT64, BToTextInt64),
      BuiltinFunctionInfo(
        TO_TEXT_DECIMAL,
        BToTextNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(TO_TEXT_NUMERIC, BToTextNumeric, minVersion = numeric),
      BuiltinFunctionInfo(TO_TEXT_TIMESTAMP, BToTextTimestamp),
      BuiltinFunctionInfo(TO_TEXT_PARTY, BToTextParty, minVersion = partyTextConversions),
      BuiltinFunctionInfo(TO_TEXT_TEXT, BToTextText),
      BuiltinFunctionInfo(TO_QUOTED_TEXT_PARTY, BToQuotedTextParty),
      BuiltinFunctionInfo(TEXT_FROM_CODE_POINTS, BToTextCodePoints, minVersion = textPacking),
      BuiltinFunctionInfo(FROM_TEXT_PARTY, BFromTextParty, minVersion = partyTextConversions),
      BuiltinFunctionInfo(FROM_TEXT_INT64, BFromTextInt64, minVersion = numberParsing),
      BuiltinFunctionInfo(
        FROM_TEXT_DECIMAL,
        BFromTextNumeric,
        handleLegacyDecimal = true,
        minVersion = numberParsing,
        maxVersion = Some(numeric)),
      BuiltinFunctionInfo(FROM_TEXT_NUMERIC, BFromTextNumeric, minVersion = numeric),
      BuiltinFunctionInfo(TEXT_TO_CODE_POINTS, BFromTextCodePoints, minVersion = textPacking),
      BuiltinFunctionInfo(SHA256_TEXT, BSHA256Text, minVersion = shaText),
      BuiltinFunctionInfo(DATE_TO_UNIX_DAYS, BDateToUnixDays),
      BuiltinFunctionInfo(EXPLODE_TEXT, BExplodeText),
      BuiltinFunctionInfo(IMPLODE_TEXT, BImplodeText),
      BuiltinFunctionInfo(GEQ_DATE, BGreaterEqDate),
      BuiltinFunctionInfo(LEQ_DATE, BLessEqDate),
      BuiltinFunctionInfo(LESS_DATE, BLessDate),
      BuiltinFunctionInfo(TIMESTAMP_TO_UNIX_MICROSECONDS, BTimestampToUnixMicroseconds),
      BuiltinFunctionInfo(TO_TEXT_DATE, BToTextDate),
      BuiltinFunctionInfo(UNIX_DAYS_TO_DATE, BUnixDaysToDate),
      BuiltinFunctionInfo(UNIX_MICROSECONDS_TO_TIMESTAMP, BUnixMicrosecondsToTimestamp),
      BuiltinFunctionInfo(GREATER_DATE, BGreaterDate),
      BuiltinFunctionInfo(EQUAL_INT64, BEqualInt64),
      BuiltinFunctionInfo(
        EQUAL_DECIMAL,
        BEqualNumeric,
        maxVersion = Some(numeric),
        handleLegacyDecimal = true),
      BuiltinFunctionInfo(EQUAL_NUMERIC, BEqualNumeric, minVersion = numeric),
      BuiltinFunctionInfo(EQUAL_TEXT, BEqualText),
      BuiltinFunctionInfo(EQUAL_TIMESTAMP, BEqualTimestamp),
      BuiltinFunctionInfo(EQUAL_DATE, BEqualDate),
      BuiltinFunctionInfo(EQUAL_PARTY, BEqualParty),
      BuiltinFunctionInfo(EQUAL_BOOL, BEqualBool),
      BuiltinFunctionInfo(EQUAL_LIST, BEqualList),
      BuiltinFunctionInfo(EQUAL_CONTRACT_ID, BEqualContractId),
      BuiltinFunctionInfo(TRACE, BTrace),
      BuiltinFunctionInfo(COERCE_CONTRACT_ID, BCoerceContractId, minVersion = coerceContractId)
    )
  }

  private val builtinInfoMap =
    builtinFunctionInfos
      .map(info => info.proto -> info)
      .toMap
      .withDefault(_ => throw ParseError("BuiltinFunction.UNRECOGNIZED"))

}
