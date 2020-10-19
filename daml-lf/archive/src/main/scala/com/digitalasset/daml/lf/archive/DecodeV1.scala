// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package archive

import java.util

import com.daml.lf.archive.Decode.ParseError
import com.daml.lf.data.Ref._
import com.daml.lf.data.{Decimal, ImmArray, Numeric, Struct, Time}
import ImmArray.ImmArraySeq
import com.daml.lf.language.Ast._
import com.daml.lf.language.Util._
import com.daml.lf.language.{LanguageVersion => LV}
import com.daml.daml_lf_dev.{DamlLf1 => PLF}
import com.google.protobuf.CodedInputStream

import scala.collection.JavaConverters._
import scala.collection.{breakOut, mutable}

private[archive] class DecodeV1(minor: LV.Minor) extends Decode.OfPackage[PLF.Package] {

  import Decode._, DecodeV1._

  private val languageVersion = LV(LV.Major.V1, minor)

  override def decodePackage(
      packageId: PackageId,
      lfPackage: PLF.Package,
      onlySerializableDataDefs: Boolean
  ): Package = {
    val internedStrings: ImmArraySeq[String] = ImmArraySeq(
      lfPackage.getInternedStringsList.asScala: _*)
    if (internedStrings.nonEmpty)
      assertSince(LV.Features.internedPackageId, "interned strings table")

    val internedDottedNames =
      decodeInternedDottedNames(lfPackage.getInternedDottedNamesList.asScala, internedStrings)

    val dependencyTracker = new PackageDependencyTracker(packageId)

    val metadata: Option[PackageMetadata] =
      if (lfPackage.hasMetadata) {
        assertSince(LV.Features.packageMetadata, "Package.metadata")
        Some(decodePackageMetadata(lfPackage.getMetadata, internedStrings))
      } else {
        if (!versionIsOlderThan(LV.Features.packageMetadata)) {
          throw ParseError(s"Package.metadata is required in DAML-LF 1.$minor")
        }
        None
      }

    Package(
      modules = lfPackage.getModulesList.asScala
        .map(
          ModuleDecoder(
            packageId,
            internedStrings,
            internedDottedNames,
            Some(dependencyTracker),
            _,
            onlySerializableDataDefs).decode),
      directDeps = dependencyTracker.getDependencies,
      languageVersion = languageVersion,
      metadata = metadata,
    )

  }

  private[archive] def decodePackageMetadata(
      metadata: PLF.PackageMetadata,
      internedStrings: ImmArraySeq[String]): PackageMetadata = {
    def getInternedStr(id: Int) =
      internedStrings.lift(id).getOrElse {
        throw ParseError(s"invalid internedString table index $id")
      }
    PackageMetadata(
      toPackageName(getInternedStr(metadata.getNameInternedStr), "PackageMetadata.name"),
      toPackageVersion(getInternedStr(metadata.getVersionInternedStr), "PackageMetadata.version22")
    )
  }

  // each LF scenario module is wrapped in a distinct proto package
  type ProtoScenarioModule = PLF.Package

  override def protoScenarioModule(cis: CodedInputStream): ProtoScenarioModule =
    PLF.Package.parser().parseFrom(cis)

  override def decodeScenarioModule(
      packageId: PackageId,
      lfScenarioModule: ProtoScenarioModule): Module = {

    val internedStrings =
      ImmArray(lfScenarioModule.getInternedStringsList.asScala).toSeq
    val internedDottedNames =
      decodeInternedDottedNames(
        lfScenarioModule.getInternedDottedNamesList.asScala,
        internedStrings)

    if (lfScenarioModule.getModulesCount != 1)
      throw ParseError(
        s"expected exactly one module in proto package, found ${lfScenarioModule.getModulesCount} modules")

    ModuleDecoder(
      packageId,
      internedStrings,
      internedDottedNames,
      None,
      lfScenarioModule.getModules(0),
      onlySerializableDataDefs = false
    ).decode()

  }

  private[this] def decodeInternedDottedNames(
      internedList: Seq[PLF.InternedDottedName],
      internedStrings: ImmArraySeq[String]): ImmArraySeq[DottedName] = {

    if (internedList.nonEmpty)
      assertSince(LV.Features.internedDottedNames, "interned dotted names table")

    def outOfRange(id: Int) =
      ParseError(s"invalid string table index $id")

    internedList
      .map(
        idn =>
          decodeSegments(
            idn.getSegmentsInternedStrList.asScala
              .map(id => internedStrings.lift(id).getOrElse(throw outOfRange(id)))(breakOut))
      )(breakOut)
  }

  private[this] def decodeSegments(segments: Seq[String]): DottedName =
    DottedName.fromSegments(segments) match {
      case Left(err) => throw new ParseError(err)
      case Right(x) => x
    }

  case class PackageDependencyTracker(self: PackageId) {
    private val deps = mutable.Set.empty[PackageId]
    def markDependency(pkgId: PackageId): Unit =
      if (pkgId != self)
        deps += pkgId
    def getDependencies: Set[PackageId] = deps.toSet
  }

  case class ModuleDecoder(
      packageId: PackageId,
      internedStrings: ImmArraySeq[String],
      internedDottedNames: ImmArraySeq[DottedName],
      optDependencyTracker: Option[PackageDependencyTracker],
      lfModule: PLF.Module,
      onlySerializableDataDefs: Boolean
  ) {

    private val moduleName: ModuleName =
      handleDottedName(
        lfModule.getNameCase,
        PLF.Module.NameCase.NAME_DNAME,
        lfModule.getNameDname,
        PLF.Module.NameCase.NAME_INTERNED_DNAME,
        lfModule.getNameInternedDname,
        "Module.name.name"
      )

    private var currentDefinitionRef: Option[DefinitionRef] = None

    def decode(): Module = {
      val defs = mutable.ArrayBuffer[(DottedName, Definition)]()
      val templates = mutable.ArrayBuffer[(DottedName, Template)]()

      if (versionIsOlderThan(LV.Features.typeSynonyms)) {
        assertEmpty(lfModule.getSynonymsList, "Module.synonyms")
      } else if (!onlySerializableDataDefs) {
        // collect type synonyms
        lfModule.getSynonymsList.asScala
          .foreach { defn =>
            val defName = handleDottedName(
              defn.getNameCase,
              PLF.DefTypeSyn.NameCase.NAME_DNAME,
              defn.getNameDname,
              PLF.DefTypeSyn.NameCase.NAME_INTERNED_DNAME,
              defn.getNameInternedDname,
              "DefTypeSyn.name.name"
            )
            currentDefinitionRef =
              Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
            val d = decodeDefTypeSyn(defn)
            defs += (defName -> d)
          }
      }

      // collect data types
      lfModule.getDataTypesList.asScala
        .filter(!onlySerializableDataDefs || _.getSerializable)
        .foreach { defn =>
          val defName = handleDottedName(
            defn.getNameCase,
            PLF.DefDataType.NameCase.NAME_DNAME,
            defn.getNameDname,
            PLF.DefDataType.NameCase.NAME_INTERNED_DNAME,
            defn.getNameInternedDname,
            "DefDataType.name.name"
          )
          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = decodeDefDataType(defn)
          defs += (defName -> d)
        }

      if (!onlySerializableDataDefs) {
        // collect values
        lfModule.getValuesList.asScala.foreach { defn =>
          val nameWithType = defn.getNameWithType
          val defName = handleDottedName(
            nameWithType.getNameDnameList.asScala,
            nameWithType.getNameInternedDname,
            "NameWithType.name"
          )

          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = decodeDefValue(defn)
          defs += (defName -> d)
        }
      }

      // collect templates
      lfModule.getTemplatesList.asScala.foreach { defn =>
        val defName = handleDottedName(
          defn.getTyconCase,
          PLF.DefTemplate.TyconCase.TYCON_DNAME,
          defn.getTyconDname,
          PLF.DefTemplate.TyconCase.TYCON_INTERNED_DNAME,
          defn.getTyconInternedDname,
          "DefTemplate.tycon.tycon"
        )
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        templates += ((defName, decodeTemplate(defn)))
      }

      Module(moduleName, defs, templates, decodeFeatureFlags(lfModule.getFlags))
    }

    // -----------------------------------------------------------------------
    private[this] def getInternedStr(id: Int) =
      internedStrings.lift(id).getOrElse {
        throw ParseError(s"invalid internedString table index $id")
      }

    private[this] def getInternedPackageId(id: Int): PackageId = {
      assertSince(LV.Features.internedPackageId, "interned PackageId")
      eitherToParseError(PackageId.fromString(getInternedStr(id)))
    }

    private[this] def getInternedName(id: Int, description: => String): Name = {
      assertSince(LV.Features.internedStrings, description)
      eitherToParseError(Name.fromString(getInternedStr(id)))
    }

    private[this] def getInternedDottedName(id: Int) =
      internedDottedNames.lift(id).getOrElse {
        throw ParseError(s"invalid dotted name table index $id")
      }

    private[this] def handleDottedName(
        segments: Seq[String],
        interned_id: Int,
        description: => String,
    ): DottedName =
      if (versionIsOlderThan(LV.Features.internedDottedNames)) {
        assertUndefined(interned_id, s"${description}_interned_id")
        decodeSegments(segments)
      } else {
        assertUndefined(segments, description)
        getInternedDottedName(interned_id)
      }

    private[this] def handleDottedName[Case](
        actualCase: Case,
        dNameCase: Case,
        dName: => PLF.DottedName,
        internedDNameCase: Case,
        internedDName: => Int,
        description: => String): DottedName =
      if (versionIsOlderThan(LV.Features.internedDottedNames)) {
        if (actualCase != dNameCase)
          throw ParseError(s"${description}_dname is required by DAML-LF 1.$minor")
        decodeSegments(dName.getSegmentsList.asScala)
      } else {
        if (actualCase != internedDNameCase)
          throw ParseError(s"${description}_interned_dname is required by DAML-LF 1.$minor")
        getInternedDottedName(internedDName)
      }

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
      val params = lfDataType.getParamsList.asScala
      DDataType(
        lfDataType.getSerializable,
        ImmArray(params).map(decodeTypeVarWithKind),
        lfDataType.getDataConsCase match {
          case PLF.DefDataType.DataConsCase.RECORD =>
            DataRecord(decodeFields(ImmArray(lfDataType.getRecord.getFieldsList.asScala)))
          case PLF.DefDataType.DataConsCase.VARIANT =>
            DataVariant(decodeFields(ImmArray(lfDataType.getVariant.getFieldsList.asScala)))
          case PLF.DefDataType.DataConsCase.ENUM =>
            assertSince(LV.Features.enum, "DefDataType.DataCons.Enum")
            assertEmpty(params, "params")
            DataEnum(decodeEnumCon(lfDataType.getEnum))
          case PLF.DefDataType.DataConsCase.DATACONS_NOT_SET =>
            throw ParseError("DefDataType.DATACONS_NOT_SET")

        }
      )
    }

    private[this] def decodeDefTypeSyn(lfTypeSyn: PLF.DefTypeSyn): DTypeSyn = {
      val params = lfTypeSyn.getParamsList.asScala
      DTypeSyn(
        ImmArray(params).map(decodeTypeVarWithKind),
        decodeType(lfTypeSyn.getType)
      )
    }

    def handleInternedName[Case](
        actualCase: Case,
        stringCase: Case,
        string: => String,
        internedStringCase: Case,
        internedString: => Int,
        description: => String) = {
      val str = if (versionIsOlderThan(LV.Features.internedStrings)) {
        if (actualCase != stringCase)
          throw ParseError(s"${description}_str is required by DAML-LF 1.$minor")
        string
      } else {
        if (actualCase != internedStringCase)
          throw ParseError(s"${description}_interned_str is required by DAML-LF 1.$minor")
        internedStrings(internedString)
      }
      toName(str)
    }

    def handleInternedNames(
        strings: util.List[String],
        stringIds: util.List[Integer],
        description: => String): Seq[Name] =
      if (versionIsOlderThan(LV.Features.internedStrings)) {
        assertEmpty(stringIds, description + "_interned_string")
        strings.asScala.map(toName)
      } else {
        assertEmpty(strings, description)
        stringIds.asScala.map(id => toName(internedStrings(id)))
      }

    private[this] def decodeFieldWithType(lfFieldWithType: PLF.FieldWithType): (Name, Type) =
      handleInternedName(
        lfFieldWithType.getFieldCase,
        PLF.FieldWithType.FieldCase.FIELD_STR,
        lfFieldWithType.getFieldStr,
        PLF.FieldWithType.FieldCase.FIELD_INTERNED_STR,
        lfFieldWithType.getFieldInternedStr,
        "FieldWithType.field.field",
      ) -> decodeType(lfFieldWithType.getType)

    private[this] def decodeFields(lfFields: ImmArray[PLF.FieldWithType]): ImmArray[(Name, Type)] =
      lfFields.map(decodeFieldWithType)

    private[this] def decodeFieldWithExpr(
        lfFieldWithExpr: PLF.FieldWithExpr,
        definition: String
    ): (Name, Expr) =
      handleInternedName(
        lfFieldWithExpr.getFieldCase,
        PLF.FieldWithExpr.FieldCase.FIELD_STR,
        lfFieldWithExpr.getFieldStr,
        PLF.FieldWithExpr.FieldCase.FIELD_INTERNED_STR,
        lfFieldWithExpr.getFieldInternedStr,
        "FieldWithType.name",
      ) -> decodeExpr(lfFieldWithExpr.getExpr, definition)

    private[this] def decodeEnumCon(
        enumCon: PLF.DefDataType.EnumConstructors): ImmArray[EnumConName] =
      ImmArray(
        handleInternedNames(
          enumCon.getConstructorsStrList,
          enumCon.getConstructorsInternedStrList,
          "EnumConstructors.constructors"
        ))

    private[this] def decodeDefValue(lfValue: PLF.DefValue): DValue = {
      val name = handleDottedName(
        lfValue.getNameWithType.getNameDnameList.asScala,
        lfValue.getNameWithType.getNameInternedDname,
        "DefValue.NameWithType.name"
      )
      DValue(
        typ = decodeType(lfValue.getNameWithType.getType),
        noPartyLiterals = lfValue.getNoPartyLiterals,
        body = decodeExpr(lfValue.getExpr, name.toString),
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
        tpl: DottedName,
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
            fields = ImmArray(recCon.getFieldsList.asScala).map(
              field =>
                handleInternedName(
                  field.getFieldCase,
                  PLF.KeyExpr.RecordField.FieldCase.FIELD_STR,
                  field.getFieldStr,
                  PLF.KeyExpr.RecordField.FieldCase.FIELD_INTERNED_STR,
                  field.getFieldInternedStr,
                  "KeyExpr.field"
                ) -> decodeKeyExpr(field.getExpr, tplVar))
          )

        case PLF.KeyExpr.SumCase.PROJECTIONS =>
          val lfProjs = expr.getProjections.getProjectionsList.asScala
          lfProjs.foldLeft(EVar(tplVar): Expr)(
            (acc, lfProj) =>
              ERecProj(
                decodeTypeConApp(lfProj.getTycon),
                handleInternedName(
                  lfProj.getFieldCase,
                  PLF.KeyExpr.Projection.FieldCase.FIELD_STR,
                  lfProj.getFieldStr,
                  PLF.KeyExpr.Projection.FieldCase.FIELD_INTERNED_STR,
                  lfProj.getFieldInternedStr,
                  "KeyExpr.Projection.field"
                ),
                acc
            ))

        case PLF.KeyExpr.SumCase.SUM_NOT_SET =>
          throw ParseError("KeyExpr.SUM_NOT_SET")
      }
    }

    private[this] def decodeTemplate(lfTempl: PLF.DefTemplate): Template = {
      val tpl = handleDottedName(
        lfTempl.getTyconCase,
        PLF.DefTemplate.TyconCase.TYCON_DNAME,
        lfTempl.getTyconDname,
        PLF.DefTemplate.TyconCase.TYCON_INTERNED_DNAME,
        lfTempl.getTyconInternedDname,
        "DefTemplate.tycon.tycon"
      )
      val paramName = handleInternedName(
        lfTempl.getParamCase,
        PLF.DefTemplate.ParamCase.PARAM_STR,
        lfTempl.getParamStr,
        PLF.DefTemplate.ParamCase.PARAM_INTERNED_STR,
        lfTempl.getParamInternedStr,
        "DefTemplate.param.param"
      )
      Template(
        param = paramName,
        precond = if (lfTempl.hasPrecond) decodeExpr(lfTempl.getPrecond, s"$tpl:ensure") else ETrue,
        signatories = decodeExpr(lfTempl.getSignatories, s"$tpl.signatory"),
        agreementText = decodeExpr(lfTempl.getAgreement, s"$tpl:agreement"),
        choices = lfTempl.getChoicesList.asScala
          .map(decodeChoice(tpl, _))
          .map(ch => (ch.name, ch)),
        observers = decodeExpr(lfTempl.getObservers, s"$tpl:observer"),
        key =
          if (lfTempl.hasKey) Some(decodeTemplateKey(tpl, lfTempl.getKey, paramName))
          else None
      )
    }

    private[this] def decodeChoice(
        tpl: DottedName,
        lfChoice: PLF.TemplateChoice): TemplateChoice = {
      val (v, t) = decodeBinder(lfChoice.getArgBinder)
      val chName = handleInternedName(
        lfChoice.getNameCase,
        PLF.TemplateChoice.NameCase.NAME_STR,
        lfChoice.getNameStr,
        PLF.TemplateChoice.NameCase.NAME_INTERNED_STR,
        lfChoice.getNameInternedStr,
        "TemplateChoice.name.name"
      )
      val selfBinder = handleInternedName(
        lfChoice.getSelfBinderCase,
        PLF.TemplateChoice.SelfBinderCase.SELF_BINDER_STR,
        lfChoice.getSelfBinderStr,
        PLF.TemplateChoice.SelfBinderCase.SELF_BINDER_INTERNED_STR,
        lfChoice.getSelfBinderInternedStr,
        "TemplateChoice.self_binder.self_binder"
      )
      TemplateChoice(
        name = chName,
        consuming = lfChoice.getConsuming,
        controllers = decodeExpr(lfChoice.getControllers, s"$tpl:$chName:controller"),
        selfBinder = selfBinder,
        argBinder = v -> t,
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
          (params foldRight decodeKind(kArrow.getResult))((param, kind) =>
            KArrow(decodeKind(param), kind))
        case PLF.Kind.SumCase.SUM_NOT_SET =>
          throw ParseError("Kind.SUM_NOT_SET")
      }

    private[lf] def decodeType(lfType: PLF.Type): Type =
      lfType.getSumCase match {
        case PLF.Type.SumCase.VAR =>
          val tvar = lfType.getVar
          val varName = handleInternedName(
            tvar.getVarCase,
            PLF.Type.Var.VarCase.VAR_STR,
            tvar.getVarStr,
            PLF.Type.Var.VarCase.VAR_INTERNED_STR,
            tvar.getVarInternedStr,
            "Type.var.var"
          )
          tvar.getArgsList.asScala
            .foldLeft[Type](TVar(varName))((typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.NAT =>
          assertSince(LV.Features.numeric, "Type.NAT")
          Numeric.Scale
            .fromLong(lfType.getNat)
            .fold[TNat](
              _ =>
                throw ParseError(
                  s"TNat must be between ${Numeric.Scale.MinValue} and ${Numeric.Scale.MaxValue}, found ${lfType.getNat}"),
              TNat(_)
            )
        case PLF.Type.SumCase.CON =>
          val tcon = lfType.getCon
          (tcon.getArgsList.asScala foldLeft [Type] TTyCon(decodeTypeConName(tcon.getTycon)))(
            (typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.SYN =>
          val tsyn = lfType.getSyn
          TSynApp(
            decodeTypeSynName(tsyn.getTysyn),
            ImmArray(tsyn.getArgsList.asScala.map(decodeType))
          )
        case PLF.Type.SumCase.PRIM =>
          val prim = lfType.getPrim
          val baseType =
            if (prim.getPrim == PLF.PrimType.DECIMAL) {
              assertUntil(LV.Features.numeric, "PrimType.DECIMAL")
              TDecimal
            } else {
              val info = builtinTypeInfoMap(prim.getPrim)
              assertSince(info.minVersion, prim.getPrim.getValueDescriptor.getFullName)
              info.typ
            }
          (prim.getArgsList.asScala foldLeft [Type] baseType)((typ, arg) =>
            TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.FUN =>
          assertUntil(LV.Features.arrowType, "Type.Fun")
          val tFun = lfType.getFun
          val params = tFun.getParamsList.asScala
          assertNonEmpty(params, "params")
          (params foldRight decodeType(tFun.getResult))(
            (param, res) => TFun(decodeType(param), res))
        case PLF.Type.SumCase.FORALL =>
          val tForall = lfType.getForall
          val vars = tForall.getVarsList.asScala
          assertNonEmpty(vars, "vars")
          (vars foldRight decodeType(tForall.getBody))((binder, acc) =>
            TForall(decodeTypeVarWithKind(binder), acc))
        case PLF.Type.SumCase.STRUCT =>
          val struct = lfType.getStruct
          val fields = struct.getFieldsList.asScala
          assertNonEmpty(fields, "fields")
          TStruct(
            Struct
              .fromSeq(fields.map(decodeFieldWithType))
              .fold(
                name => throw ParseError(s"TStruct: duplicate field $name"),
                identity
              ))
        case PLF.Type.SumCase.SUM_NOT_SET =>
          throw ParseError("Type.SUM_NOT_SET")
      }

    private[this] def decodeModuleRef(lfRef: PLF.ModuleRef): (PackageId, ModuleName) = {
      val modName = handleDottedName(
        lfRef.getModuleNameCase,
        PLF.ModuleRef.ModuleNameCase.MODULE_NAME_DNAME,
        lfRef.getModuleNameDname,
        PLF.ModuleRef.ModuleNameCase.MODULE_NAME_INTERNED_DNAME,
        lfRef.getModuleNameInternedDname,
        "ModuleRef.module_name.module_name"
      )
      import PLF.PackageRef.{SumCase => SC}

      val pkgId = lfRef.getPackageRef.getSumCase match {
        case SC.SELF =>
          this.packageId
        case SC.PACKAGE_ID_STR =>
          toPackageId(lfRef.getPackageRef.getPackageIdStr, "PackageRef.packageId")
        case SC.PACKAGE_ID_INTERNED_STR =>
          getInternedPackageId(lfRef.getPackageRef.getPackageIdInternedStr)
        case SC.SUM_NOT_SET =>
          throw ParseError("PackageRef.SUM_NOT_SET")
      }
      optDependencyTracker.foreach(_.markDependency(pkgId))
      (pkgId, modName)
    }

    private[this] def decodeValName(lfVal: PLF.ValName): ValueRef = {
      val (packageId, module) = decodeModuleRef(lfVal.getModule)
      val name =
        handleDottedName(lfVal.getNameDnameList.asScala, lfVal.getNameInternedDname, "ValName.name")
      ValueRef(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConName(lfTyConName: PLF.TypeConName): TypeConName = {
      val (packageId, module) = decodeModuleRef(lfTyConName.getModule)
      val name = handleDottedName(
        lfTyConName.getNameCase,
        PLF.TypeConName.NameCase.NAME_DNAME,
        lfTyConName.getNameDname,
        PLF.TypeConName.NameCase.NAME_INTERNED_DNAME,
        lfTyConName.getNameInternedDname,
        "TypeConName.name.name"
      )
      Identifier(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeSynName(lfTySynName: PLF.TypeSynName): TypeSynName = {
      val (packageId, module) = decodeModuleRef(lfTySynName.getModule)
      val name = handleDottedName(
        lfTySynName.getNameCase,
        PLF.TypeSynName.NameCase.NAME_DNAME,
        lfTySynName.getNameDname,
        PLF.TypeSynName.NameCase.NAME_INTERNED_DNAME,
        lfTySynName.getNameInternedDname,
        "TypeSynName.name.name"
      )
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
        case PLF.Expr.SumCase.VAR_STR =>
          assertUntil(LV.Features.internedStrings, "Expr.var_str")
          EVar(toName(lfExpr.getVarStr))

        case PLF.Expr.SumCase.VAR_INTERNED_STR =>
          EVar(getInternedName(lfExpr.getVarInternedStr, "Expr.var_interned_id"))

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
          info.maxVersion.foreach(assertUntil(_, lfExpr.getBuiltin.getValueDescriptor.getFullName))
          info.expr

        case PLF.Expr.SumCase.REC_CON =>
          val recCon = lfExpr.getRecCon
          ERecCon(
            tycon = decodeTypeConApp(recCon.getTycon),
            fields = ImmArray(recCon.getFieldsList.asScala).map(decodeFieldWithExpr(_, definition))
          )

        case PLF.Expr.SumCase.REC_PROJ =>
          val recProj = lfExpr.getRecProj
          ERecProj(
            tycon = decodeTypeConApp(recProj.getTycon),
            field = handleInternedName(
              recProj.getFieldCase,
              PLF.Expr.RecProj.FieldCase.FIELD_STR,
              recProj.getFieldStr,
              PLF.Expr.RecProj.FieldCase.FIELD_INTERNED_STR,
              recProj.getFieldInternedStr,
              "Expr.RecProj.field.field"
            ),
            record = decodeExpr(recProj.getRecord, definition)
          )

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          ERecUpd(
            tycon = decodeTypeConApp(recUpd.getTycon),
            field = handleInternedName(
              recUpd.getFieldCase,
              PLF.Expr.RecUpd.FieldCase.FIELD_STR,
              recUpd.getFieldStr,
              PLF.Expr.RecUpd.FieldCase.FIELD_INTERNED_STR,
              recUpd.getFieldInternedStr,
              "Expr.RecUpd.field.field"
            ),
            record = decodeExpr(recUpd.getRecord, definition),
            update = decodeExpr(recUpd.getUpdate, definition)
          )

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          EVariantCon(
            decodeTypeConApp(varCon.getTycon),
            handleInternedName(
              varCon.getVariantConCase,
              PLF.Expr.VariantCon.VariantConCase.VARIANT_CON_STR,
              varCon.getVariantConStr,
              PLF.Expr.VariantCon.VariantConCase.VARIANT_CON_INTERNED_STR,
              varCon.getVariantConInternedStr,
              "Expr.VariantCon.variant_con.variant_con"
            ),
            decodeExpr(varCon.getVariantArg, definition)
          )

        case PLF.Expr.SumCase.ENUM_CON =>
          assertSince(LV.Features.enum, "Expr.Enum")
          val enumCon = lfExpr.getEnumCon
          EEnumCon(
            decodeTypeConName(enumCon.getTycon),
            handleInternedName(
              enumCon.getEnumConCase,
              PLF.Expr.EnumCon.EnumConCase.ENUM_CON_STR,
              enumCon.getEnumConStr,
              PLF.Expr.EnumCon.EnumConCase.ENUM_CON_INTERNED_STR,
              enumCon.getEnumConInternedStr,
              "Expr.EnumCon.enum_con.enum_con"
            )
          )

        case PLF.Expr.SumCase.STRUCT_CON =>
          val structCon = lfExpr.getStructCon
          EStructCon(
            ImmArray(structCon.getFieldsList.asScala).map(decodeFieldWithExpr(_, definition))
          )

        case PLF.Expr.SumCase.STRUCT_PROJ =>
          val structProj = lfExpr.getStructProj
          EStructProj(
            field = handleInternedName(
              structProj.getFieldCase,
              PLF.Expr.StructProj.FieldCase.FIELD_STR,
              structProj.getFieldStr,
              PLF.Expr.StructProj.FieldCase.FIELD_INTERNED_STR,
              structProj.getFieldInternedStr,
              "Expr.StructProj.field.field"
            ),
            struct = decodeExpr(structProj.getStruct, definition)
          )

        case PLF.Expr.SumCase.STRUCT_UPD =>
          val structUpd = lfExpr.getStructUpd
          EStructUpd(
            field = handleInternedName(
              structUpd.getFieldCase,
              PLF.Expr.StructUpd.FieldCase.FIELD_STR,
              structUpd.getFieldStr,
              PLF.Expr.StructUpd.FieldCase.FIELD_INTERNED_STR,
              structUpd.getFieldInternedStr,
              "Expr.StructUpd.field.field"
            ),
            struct = decodeExpr(structUpd.getStruct, definition),
            update = decodeExpr(structUpd.getUpdate, definition)
          )

        case PLF.Expr.SumCase.APP =>
          val app = lfExpr.getApp
          val args = app.getArgsList.asScala
          assertNonEmpty(args, "args")
          (args foldLeft decodeExpr(app.getFun, definition))((e, arg) =>
            EApp(e, decodeExpr(arg, definition)))

        case PLF.Expr.SumCase.ABS =>
          val lfAbs = lfExpr.getAbs
          val params = lfAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          // val params = lfAbs.getParamList.asScala.map(decodeBinder)
          (params foldRight decodeExpr(lfAbs.getBody, definition))((param, e) =>
            EAbs(decodeBinder(param), e, currentDefinitionRef))

        case PLF.Expr.SumCase.TY_APP =>
          val tyapp = lfExpr.getTyApp
          val args = tyapp.getTypesList.asScala
          assertNonEmpty(args, "args")
          (args foldLeft decodeExpr(tyapp.getExpr, definition))((e, arg) =>
            ETyApp(e, decodeType(arg)))

        case PLF.Expr.SumCase.TY_ABS =>
          val lfTyAbs = lfExpr.getTyAbs
          val params = lfTyAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          (params foldRight decodeExpr(lfTyAbs.getBody, definition))((param, e) =>
            ETyAbs(decodeTypeVarWithKind(param), e))

        case PLF.Expr.SumCase.LET =>
          val lfLet = lfExpr.getLet
          val bindings = lfLet.getBindingsList.asScala
          assertNonEmpty(bindings, "bindings")
          (bindings foldRight decodeExpr(lfLet.getBody, definition))((binding, e) => {
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

        case PLF.Expr.SumCase.TO_ANY =>
          assertSince(LV.Features.anyType, "Expr.ToAny")
          EToAny(
            decodeType(lfExpr.getToAny.getType),
            decodeExpr(lfExpr.getToAny.getExpr, definition))

        case PLF.Expr.SumCase.FROM_ANY =>
          assertSince(LV.Features.anyType, "Expr.FromAny")
          EFromAny(
            decodeType(lfExpr.getFromAny.getType),
            decodeExpr(lfExpr.getFromAny.getExpr, definition))

        case PLF.Expr.SumCase.TYPE_REP =>
          assertSince(LV.Features.typeRep, "Expr.type_rep")
          ETypeRep(decodeType(lfExpr.getTypeRep))

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
            tycon = decodeTypeConName(variant.getCon),
            variant = handleInternedName(
              variant.getVariantCase,
              PLF.CaseAlt.Variant.VariantCase.VARIANT_STR,
              variant.getVariantStr,
              PLF.CaseAlt.Variant.VariantCase.VARIANT_INTERNED_STR,
              variant.getVariantInternedStr,
              "CaseAlt.Variant.variant.variant"
            ),
            binder = handleInternedName(
              variant.getBinderCase,
              PLF.CaseAlt.Variant.BinderCase.BINDER_STR,
              variant.getBinderStr,
              PLF.CaseAlt.Variant.BinderCase.BINDER_INTERNED_STR,
              variant.getBinderInternedStr,
              "CaseAlt.Variant.binder.binder"
            )
          )
        case PLF.CaseAlt.SumCase.ENUM =>
          assertSince(LV.Features.enum, "CaseAlt.Enum")
          val enum = lfCaseAlt.getEnum
          CPEnum(
            tycon = decodeTypeConName(enum.getCon),
            constructor = handleInternedName(
              enum.getConstructorCase,
              PLF.CaseAlt.Enum.ConstructorCase.CONSTRUCTOR_STR,
              enum.getConstructorStr,
              PLF.CaseAlt.Enum.ConstructorCase.CONSTRUCTOR_INTERNED_STR,
              enum.getConstructorInternedStr,
              "CaseAlt.Enum.constructor.constructor"
            )
          )
        case PLF.CaseAlt.SumCase.PRIM_CON =>
          decodePrimCon(lfCaseAlt.getPrimCon)
        case PLF.CaseAlt.SumCase.NIL =>
          CPNil
        case PLF.CaseAlt.SumCase.CONS =>
          val cons = lfCaseAlt.getCons
          CPCons(
            head = handleInternedName(
              cons.getVarHeadCase,
              PLF.CaseAlt.Cons.VarHeadCase.VAR_HEAD_STR,
              cons.getVarHeadStr,
              PLF.CaseAlt.Cons.VarHeadCase.VAR_HEAD_INTERNED_STR,
              cons.getVarHeadInternedStr,
              "CaseAlt.Cons.var_head.var_head"
            ),
            tail = handleInternedName(
              cons.getVarTailCase,
              PLF.CaseAlt.Cons.VarTailCase.VAR_TAIL_STR,
              cons.getVarTailStr,
              PLF.CaseAlt.Cons.VarTailCase.VAR_TAIL_INTERNED_STR,
              cons.getVarTailInternedStr,
              "CaseAlt.Cons.var_tail.var_tail"
            )
          )

        case PLF.CaseAlt.SumCase.OPTIONAL_NONE =>
          assertSince(LV.Features.optional, "CaseAlt.OptionalNone")
          CPNone

        case PLF.CaseAlt.SumCase.OPTIONAL_SOME =>
          assertSince(LV.Features.optional, "CaseAlt.OptionalSome")
          val some = lfCaseAlt.getOptionalSome
          CPSome(
            handleInternedName(
              some.getVarBodyCase,
              PLF.CaseAlt.OptionalSome.VarBodyCase.VAR_BODY_STR,
              some.getVarBodyStr,
              PLF.CaseAlt.OptionalSome.VarBodyCase.VAR_BODY_INTERNED_STR,
              some.getVarBodyInternedStr,
              "CaseAlt.OptionalSom.var_body.var_body"
            ))

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
            choice = handleInternedName(
              exercise.getChoiceCase,
              PLF.Update.Exercise.ChoiceCase.CHOICE_STR,
              exercise.getChoiceStr,
              PLF.Update.Exercise.ChoiceCase.CHOICE_INTERNED_STR,
              exercise.getChoiceInternedStr,
              "Update.Exercise.choice.choice"
            ),
            cidE = decodeExpr(exercise.getCid, definition),
            actorsE = if (versionIsOlderThan(LV.Features.noExerciseActor)) {
              if (!exercise.hasActor)
                throw ParseError(s"Update.Exercise.actors is required by DAML-LF {1.$minor}")
              Some(decodeExpr(exercise.getActor, definition))
            } else {
              if (exercise.hasActor)
                throw ParseError(s"Update.Exercise.actors is not supported by DAML-LF {1.$minor}")
              None
            },
            argE = decodeExpr(exercise.getArg, definition)
          )

        case PLF.Update.SumCase.EXERCISE_BY_KEY =>
          assertSince(LV.Features.exerciseByKey, "exerciseByKey")
          val exerciseByKey = lfUpdate.getExerciseByKey
          UpdateExerciseByKey(
            templateId = decodeTypeConName(exerciseByKey.getTemplate),
            choice = getInternedName(
              exerciseByKey.getChoiceInternedStr,
              "Update.ExerciseByKey.choice.choice"),
            keyE = decodeExpr(exerciseByKey.getKey, definition),
            argE = decodeExpr(exerciseByKey.getArg, definition)
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
      handleInternedName(
        lfTypeVarWithKind.getVarCase,
        PLF.TypeVarWithKind.VarCase.VAR_STR,
        lfTypeVarWithKind.getVarStr,
        PLF.TypeVarWithKind.VarCase.VAR_INTERNED_STR,
        lfTypeVarWithKind.getVarInternedStr,
        "TypeVarWithKind.var.var"
      ) -> decodeKind(lfTypeVarWithKind.getKind)

    private[this] def decodeBinding(lfBinding: PLF.Binding, definition: String): Binding = {
      val (binder, typ) = decodeBinder(lfBinding.getBinder)
      Binding(Some(binder), typ, decodeExpr(lfBinding.getBound, definition))
    }

    private[this] def decodeBinder(lfBinder: PLF.VarWithType): (ExprVarName, Type) =
      handleInternedName(
        lfBinder.getVarCase,
        PLF.VarWithType.VarCase.VAR_STR,
        lfBinder.getVarStr,
        PLF.VarWithType.VarCase.VAR_INTERNED_STR,
        lfBinder.getVarInternedStr,
        "VarWithType.var.var"
      ) -> decodeType(lfBinder.getType)

    private[this] def decodePrimCon(lfPrimCon: PLF.PrimCon): CPPrimCon =
      lfPrimCon match {
        case PLF.PrimCon.CON_UNIT =>
          CPUnit
        case PLF.PrimCon.CON_FALSE =>
          CPFalse
        case PLF.PrimCon.CON_TRUE =>
          CPTrue
        case _ => throw ParseError("Unknown PrimCon: " + lfPrimCon.toString)
      }

    private[this] def decodePrimLit(lfPrimLit: PLF.PrimLit): PrimLit =
      lfPrimLit.getSumCase match {
        case PLF.PrimLit.SumCase.INT64 =>
          PLInt64(lfPrimLit.getInt64)
        case PLF.PrimLit.SumCase.DECIMAL_STR =>
          assertUntil(LV.Features.numeric, "PrimLit.decimal")
          assertUntil(LV.Features.internedStrings, "PrimLit.decimal_str")
          toPLDecimal(lfPrimLit.getDecimalStr)
        case PLF.PrimLit.SumCase.TEXT_STR =>
          assertUntil(LV.Features.internedStrings, "PrimLit.text_str")
          PLText(lfPrimLit.getTextStr)
        case PLF.PrimLit.SumCase.PARTY_STR =>
          assertUntil(LV.Features.internedStrings, "PrimLit.party_str")
          toPLParty(lfPrimLit.getPartyStr)
        case PLF.PrimLit.SumCase.TIMESTAMP =>
          val t = Time.Timestamp.fromLong(lfPrimLit.getTimestamp)
          t.fold(e => throw ParseError("error decoding timestamp: " + e), PLTimestamp)
        case PLF.PrimLit.SumCase.DATE =>
          val d = Time.Date.fromDaysSinceEpoch(lfPrimLit.getDate)
          d.fold(e => throw ParseError("error decoding date: " + e), PLDate)
        case PLF.PrimLit.SumCase.TEXT_INTERNED_STR =>
          assertSince(LV.Features.internedStrings, "PrimLit.text_interned_str")
          PLText(getInternedStr(lfPrimLit.getTextInternedStr))
        case PLF.PrimLit.SumCase.NUMERIC_INTERNED_STR =>
          assertSince(LV.Features.numeric, "PrimLit.numeric")
          toPLNumeric(getInternedStr(lfPrimLit.getNumericInternedStr))
        case PLF.PrimLit.SumCase.PARTY_INTERNED_STR =>
          assertSince(LV.Features.internedStrings, "PrimLit.party_interned_str")
          toPLParty(getInternedStr(lfPrimLit.getPartyInternedStr))
        case PLF.PrimLit.SumCase.SUM_NOT_SET =>
          throw ParseError("PrimLit.SUM_NOT_SET")
      }
  }

  private def versionIsOlderThan(minVersion: LV): Boolean =
    LV.ordering.lt(languageVersion, minVersion)

  private def toPackageId(s: String, description: => String): PackageId = {
    assertUntil(LV.Features.internedStrings, description)
    eitherToParseError(PackageId.fromString(s))
  }

  private[this] def toName(s: String): Name =
    eitherToParseError(Name.fromString(s))

  private[this] def toPackageName(s: String, description: => String): PackageName = {
    assertSince(LV.Features.packageMetadata, description)
    eitherToParseError(PackageName.fromString(s))
  }

  private[this] def toPackageVersion(s: String, description: => String): PackageVersion = {
    assertSince(LV.Features.packageMetadata, description)
    eitherToParseError(PackageVersion.fromString(s))
  }

  private[this] def toPLNumeric(s: String) =
    PLNumeric(eitherToParseError(Numeric.fromString(s)))

  private[this] def toPLDecimal(s: String) =
    PLNumeric(eitherToParseError(Decimal.fromString(s)))

  private[this] def toPLParty(s: String) =
    PLParty(eitherToParseError(Party.fromString(s)))

  // maxVersion excluded
  private[this] def assertUntil(maxVersion: LV, description: => String): Unit =
    if (!versionIsOlderThan(maxVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  // minVersion included
  private[this] def assertSince(minVersion: LV, description: => String): Unit =
    if (versionIsOlderThan(minVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertUndefined(i: Int, description: => String): Unit =
    if (i != 0)
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertUndefined(s: Seq[_], description: => String): Unit =
    if (s.nonEmpty)
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertNonEmpty(s: Seq[_], description: => String): Unit =
    if (s.isEmpty) throw ParseError(s"Unexpected empty $description")

  private[this] def assertEmpty(s: Seq[_], description: => String): Unit =
    if (s.nonEmpty) throw ParseError(s"Unexpected non-empty $description")

  private[this] def assertEmpty(s: util.List[_], description: => String): Unit =
    if (!s.isEmpty) throw ParseError(s"Unexpected non-empty $description")

}

private[lf] object DecodeV1 {

  private def eitherToParseError[A](x: Either[String, A]): A =
    x.fold(err => throw new ParseError(err), identity)

  case class BuiltinTypeInfo(
      proto: PLF.PrimType,
      bTyp: BuiltinType,
      minVersion: LV = LV.Features.default
  ) {
    val typ = TBuiltin(bTyp)
  }

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
      BuiltinTypeInfo(TEXTMAP, BTTextMap, minVersion = optional),
      BuiltinTypeInfo(GENMAP, BTGenMap, minVersion = genMap),
      BuiltinTypeInfo(ARROW, BTArrow, minVersion = arrowType),
      BuiltinTypeInfo(NUMERIC, BTNumeric, minVersion = numeric),
      BuiltinTypeInfo(ANY, BTAny, minVersion = anyType),
      BuiltinTypeInfo(TYPE_REP, BTTypeRep, minVersion = typeRep)
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
      implicitParameters: List[Type] = List.empty
  ) {
    val expr: Expr = implicitParameters.foldLeft[Expr](EBuiltin(builtin))(ETyApp)
  }

  val builtinFunctionInfos: List[BuiltinFunctionInfo] = {
    import PLF.BuiltinFunction._, LV.Features._
    List(
      BuiltinFunctionInfo(
        ADD_DECIMAL,
        BAddNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal)
      ),
      BuiltinFunctionInfo(
        SUB_DECIMAL,
        BSubNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal)
      ),
      BuiltinFunctionInfo(
        MUL_DECIMAL,
        BMulNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal, TNat.Decimal, TNat.Decimal)
      ),
      BuiltinFunctionInfo(
        DIV_DECIMAL,
        BDivNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal, TNat.Decimal, TNat.Decimal)
      ),
      BuiltinFunctionInfo(
        ROUND_DECIMAL,
        BRoundNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal)
      ),
      BuiltinFunctionInfo(ADD_NUMERIC, BAddNumeric, minVersion = numeric),
      BuiltinFunctionInfo(SUB_NUMERIC, BSubNumeric, minVersion = numeric),
      BuiltinFunctionInfo(MUL_NUMERIC, BMulNumeric, minVersion = numeric),
      BuiltinFunctionInfo(DIV_NUMERIC, BDivNumeric, minVersion = numeric),
      BuiltinFunctionInfo(ROUND_NUMERIC, BRoundNumeric, minVersion = numeric),
      BuiltinFunctionInfo(CAST_NUMERIC, BCastNumeric, minVersion = numeric),
      BuiltinFunctionInfo(SHIFT_NUMERIC, BShiftNumeric, minVersion = numeric),
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
        implicitParameters = List(TNat.Decimal)
      ),
      BuiltinFunctionInfo(
        DECIMAL_TO_INT64,
        BNumericToInt64,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal)
      ),
      BuiltinFunctionInfo(INT64_TO_NUMERIC, BInt64ToNumeric, minVersion = numeric),
      BuiltinFunctionInfo(NUMERIC_TO_INT64, BNumericToInt64, minVersion = numeric),
      BuiltinFunctionInfo(FOLDL, BFoldl),
      BuiltinFunctionInfo(FOLDR, BFoldr),
      BuiltinFunctionInfo(TEXTMAP_EMPTY, BTextMapEmpty, minVersion = textMap),
      BuiltinFunctionInfo(TEXTMAP_INSERT, BTextMapInsert, minVersion = textMap),
      BuiltinFunctionInfo(TEXTMAP_LOOKUP, BTextMapLookup, minVersion = textMap),
      BuiltinFunctionInfo(TEXTMAP_DELETE, BTextMapDelete, minVersion = textMap),
      BuiltinFunctionInfo(TEXTMAP_TO_LIST, BTextMapToList, minVersion = textMap),
      BuiltinFunctionInfo(TEXTMAP_SIZE, BTextMapSize, minVersion = textMap),
      BuiltinFunctionInfo(GENMAP_EMPTY, BGenMapEmpty, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_INSERT, BGenMapInsert, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_LOOKUP, BGenMapLookup, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_DELETE, BGenMapDelete, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_KEYS, BGenMapKeys, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_VALUES, BGenMapValues, minVersion = genMap),
      BuiltinFunctionInfo(GENMAP_SIZE, BGenMapSize, minVersion = genMap),
      BuiltinFunctionInfo(APPEND_TEXT, BAppendText),
      BuiltinFunctionInfo(ERROR, BError),
      BuiltinFunctionInfo(
        LEQ_INT64,
        BLessEq,
        implicitParameters = List(TInt64),
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        LEQ_DECIMAL,
        BLessEq,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal)
      ),
      BuiltinFunctionInfo(
        LEQ_NUMERIC,
        BLessEqNumeric,
        maxVersion = Some(genComparison),
        minVersion = numeric,
      ),
      BuiltinFunctionInfo(
        LEQ_TEXT,
        BLessEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText)),
      BuiltinFunctionInfo(
        LEQ_TIMESTAMP,
        BLessEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp)),
      BuiltinFunctionInfo(
        LEQ_PARTY,
        BLessEq,
        minVersion = partyOrdering,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty)),
      BuiltinFunctionInfo(
        GEQ_INT64,
        BGreaterEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TInt64)),
      BuiltinFunctionInfo(
        GEQ_DECIMAL,
        BGreaterEq,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal)
      ),
      BuiltinFunctionInfo(
        GEQ_NUMERIC,
        BGreaterEqNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        GEQ_TEXT,
        BGreaterEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText)),
      BuiltinFunctionInfo(
        GEQ_TIMESTAMP,
        BGreaterEq,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp)),
      BuiltinFunctionInfo(
        GEQ_PARTY,
        BGreaterEq,
        minVersion = partyOrdering,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty),
      ),
      BuiltinFunctionInfo(
        LESS_INT64,
        BLess,
        maxVersion = Some(genComparison),
        implicitParameters = List(TInt64)),
      BuiltinFunctionInfo(
        LESS_DECIMAL,
        BLess,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal)
      ),
      BuiltinFunctionInfo(
        LESS_NUMERIC,
        BLessNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        LESS_TEXT,
        BLess,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText)),
      BuiltinFunctionInfo(
        LESS_TIMESTAMP,
        BLess,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp)),
      BuiltinFunctionInfo(
        LESS_PARTY,
        BLess,
        minVersion = partyOrdering,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty)
      ),
      BuiltinFunctionInfo(
        GREATER_INT64,
        BGreater,
        maxVersion = Some(genComparison),
        implicitParameters = List(TInt64)),
      BuiltinFunctionInfo(
        GREATER_DECIMAL,
        BGreater,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal)
      ),
      BuiltinFunctionInfo(
        GREATER_NUMERIC,
        BGreaterNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison),
      ),
      BuiltinFunctionInfo(
        GREATER_TEXT,
        BGreater,
        maxVersion = Some(genComparison),
        implicitParameters = List(TText)),
      BuiltinFunctionInfo(
        GREATER_TIMESTAMP,
        BGreater,
        maxVersion = Some(genComparison),
        implicitParameters = List(TTimestamp)),
      BuiltinFunctionInfo(
        GREATER_PARTY,
        BGreater,
        minVersion = partyOrdering,
        maxVersion = Some(genComparison),
        implicitParameters = List(TParty),
      ),
      BuiltinFunctionInfo(TO_TEXT_INT64, BToTextInt64),
      BuiltinFunctionInfo(
        TO_TEXT_DECIMAL,
        BToTextNumeric,
        maxVersion = Some(numeric),
        implicitParameters = List(TNat.Decimal)
      ),
      BuiltinFunctionInfo(TO_TEXT_NUMERIC, BToTextNumeric, minVersion = numeric),
      BuiltinFunctionInfo(TO_TEXT_TIMESTAMP, BToTextTimestamp),
      BuiltinFunctionInfo(TO_TEXT_PARTY, BToTextParty, minVersion = partyTextConversions),
      BuiltinFunctionInfo(TO_TEXT_TEXT, BToTextText),
      BuiltinFunctionInfo(
        TO_TEXT_CONTRACT_ID,
        BToTextContractId,
        minVersion = contractIdTextConversions,
      ),
      BuiltinFunctionInfo(TO_QUOTED_TEXT_PARTY, BToQuotedTextParty),
      BuiltinFunctionInfo(TEXT_FROM_CODE_POINTS, BToTextCodePoints, minVersion = textPacking),
      BuiltinFunctionInfo(FROM_TEXT_PARTY, BFromTextParty, minVersion = partyTextConversions),
      BuiltinFunctionInfo(FROM_TEXT_INT64, BFromTextInt64, minVersion = numberParsing),
      BuiltinFunctionInfo(
        FROM_TEXT_DECIMAL,
        BFromTextNumeric,
        implicitParameters = List(TNat.Decimal),
        minVersion = numberParsing,
        maxVersion = Some(numeric)),
      BuiltinFunctionInfo(FROM_TEXT_NUMERIC, BFromTextNumeric, minVersion = numeric),
      BuiltinFunctionInfo(TEXT_TO_CODE_POINTS, BFromTextCodePoints, minVersion = textPacking),
      BuiltinFunctionInfo(SHA256_TEXT, BSHA256Text, minVersion = shaText),
      BuiltinFunctionInfo(DATE_TO_UNIX_DAYS, BDateToUnixDays),
      BuiltinFunctionInfo(EXPLODE_TEXT, BExplodeText),
      BuiltinFunctionInfo(IMPLODE_TEXT, BImplodeText),
      BuiltinFunctionInfo(GEQ_DATE, BGreaterEq, implicitParameters = List(TDate)),
      BuiltinFunctionInfo(LEQ_DATE, BLessEq, implicitParameters = List(TDate)),
      BuiltinFunctionInfo(LESS_DATE, BLess, implicitParameters = List(TDate)),
      BuiltinFunctionInfo(TIMESTAMP_TO_UNIX_MICROSECONDS, BTimestampToUnixMicroseconds),
      BuiltinFunctionInfo(TO_TEXT_DATE, BToTextDate),
      BuiltinFunctionInfo(UNIX_DAYS_TO_DATE, BUnixDaysToDate),
      BuiltinFunctionInfo(UNIX_MICROSECONDS_TO_TIMESTAMP, BUnixMicrosecondsToTimestamp),
      BuiltinFunctionInfo(GREATER_DATE, BGreater, implicitParameters = List(TDate)),
      BuiltinFunctionInfo(EQUAL, BEqual, minVersion = genComparison),
      BuiltinFunctionInfo(LESS, BLess, minVersion = genComparison),
      BuiltinFunctionInfo(LESS_EQ, BLessEq, minVersion = genComparison),
      BuiltinFunctionInfo(GREATER, BGreater, minVersion = genComparison),
      BuiltinFunctionInfo(GREATER_EQ, BGreaterEq, minVersion = genComparison),
      BuiltinFunctionInfo(EQUAL_LIST, BEqualList),
      BuiltinFunctionInfo(EQUAL_INT64, BEqual, implicitParameters = List(TInt64)),
      BuiltinFunctionInfo(
        EQUAL_DECIMAL,
        BEqual,
        maxVersion = Some(numeric),
        implicitParameters = List(TDecimal)
      ),
      BuiltinFunctionInfo(
        EQUAL_NUMERIC,
        BEqualNumeric,
        minVersion = numeric,
        maxVersion = Some(genComparison)
      ),
      BuiltinFunctionInfo(
        EQUAL_TEXT,
        BEqual,
        maxVersion = Some(genMap),
        implicitParameters = List(TText)),
      BuiltinFunctionInfo(
        EQUAL_TIMESTAMP,
        BEqual,
        maxVersion = Some(genMap),
        implicitParameters = List(TTimestamp)),
      BuiltinFunctionInfo(
        EQUAL_DATE,
        BEqual,
        maxVersion = Some(genMap),
        implicitParameters = List(TDate)),
      BuiltinFunctionInfo(
        EQUAL_PARTY,
        BEqual,
        maxVersion = Some(genMap),
        implicitParameters = List(TParty)),
      BuiltinFunctionInfo(
        EQUAL_BOOL,
        BEqual,
        maxVersion = Some(genMap),
        implicitParameters = List(TBool)),
      BuiltinFunctionInfo(
        EQUAL_TYPE_REP,
        BEqual,
        minVersion = typeRep,
        maxVersion = Some(genMap),
        implicitParameters = List(TTypeRep)),
      BuiltinFunctionInfo(EQUAL_CONTRACT_ID, BEqualContractId, maxVersion = Some(genMap)),
      BuiltinFunctionInfo(TRACE, BTrace),
      BuiltinFunctionInfo(COERCE_CONTRACT_ID, BCoerceContractId, minVersion = coerceContractId),
      BuiltinFunctionInfo(TEXT_TO_UPPER, BTextToUpper, minVersion = unstable),
      BuiltinFunctionInfo(TEXT_TO_LOWER, BTextToLower, minVersion = unstable),
      BuiltinFunctionInfo(TEXT_SLICE, BTextSlice, minVersion = unstable),
      BuiltinFunctionInfo(TEXT_SLICE_INDEX, BTextSliceIndex, minVersion = unstable),
      BuiltinFunctionInfo(TEXT_CONTAINS_ONLY, BTextContainsOnly, minVersion = unstable),
      BuiltinFunctionInfo(TEXT_REPLICATE, BTextReplicate, minVersion = unstable),
      BuiltinFunctionInfo(TEXT_SPLIT_ON, BTextSplitOn, minVersion = unstable),
      BuiltinFunctionInfo(TEXT_INTERCALATE, BTextIntercalate, minVersion = unstable)
    )
  }

  private val builtinInfoMap =
    builtinFunctionInfos
      .map(info => info.proto -> info)
      .toMap
      .withDefault(_ => throw ParseError("BuiltinFunction.UNRECOGNIZED"))

}
