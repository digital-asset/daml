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

import scala.annotation.tailrec
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
    val internedStrings: ImmArraySeq[String] = ImmArraySeq(
      lfPackage.getInternedStringsList.asScala: _*)
    if (internedStrings.nonEmpty)
      assertSince(LV.Features.internedStrings, "interned strings table")

    val internedDottedNames: ImmArraySeq[DottedName] =
      decodeInternedDottedNames(lfPackage.getInternedDottedNamesList.asScala, internedStrings)

    Package(
      lfPackage.getModulesList.asScala
        .map(
          ModuleDecoder(
            packageId,
            internedStrings,
            internedDottedNames,
            _,
            onlySerializableDataDefs).decode))
  }

  type ProtoModule = PLF.Module

  override def protoModule(cis: CodedInputStream): ProtoModule =
    PLF.Module.parser().parseFrom(cis)

  override def decodeScenarioModule(packageId: PackageId, lfModule: ProtoModule): Module =
    ModuleDecoder(
      packageId,
      ImmArraySeq.empty,
      ImmArraySeq.empty,
      lfModule,
      onlySerializableDataDefs = false).decode()

  private[this] def eitherToParseError[A](x: Either[String, A]): A =
    x.fold(err => throw new ParseError(err), identity)

  private[this] def decodeInternedDottedNames(
      internedList: Seq[PLF.InternedDottedName],
      internedStrings: ImmArraySeq[String]): ImmArraySeq[DottedName] = {
    if (internedList.nonEmpty)
      assertSince(LV.Features.internedDottedNames, "interned dotted names table")

    def outOfRange(id: Long) =
      ParseError(s"invalid string table index $id")

    internedList
      .map(
        idn =>
          decodeSegments(
            idn.getSegmentIdsList.asScala
              .map(id => internedStrings.lift(id.toInt).getOrElse(throw outOfRange(id)))(breakOut))
      )(breakOut)
  }

  private[this] def decodeSegments(segments: ImmArray[String]): DottedName =
    DottedName.fromSegments(segments.toSeq) match {
      case Left(err) => throw new ParseError(err)
      case Right(x) => x
    }

  case class ModuleDecoder(
      packageId: PackageId,
      internedStrings: ImmArraySeq[String],
      internedDottedNames: ImmArraySeq[DottedName],
      lfModule: PLF.Module,
      onlySerializableDataDefs: Boolean
  ) {

    val moduleName: ModuleName =
      decodeDottedName(lfModule.getName)

    private var currentDefinitionRef: Option[DefinitionRef] = None

    def decode(): Module = {
      val defs = mutable.ArrayBuffer[(DottedName, Definition)]()
      val templates = mutable.ArrayBuffer[(DottedName, Template)]()

      // collect data types
      lfModule.getDataTypesList.asScala
        .filter(!onlySerializableDataDefs || _.getSerializable)
        .foreach { defn =>
          val defName = decodeDottedName(defn.getName)
          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = decodeDefDataType(defn)
          defs += (defName -> d)
        }

      if (!onlySerializableDataDefs) {
        // collect values
        lfModule.getValuesList.asScala.foreach { defn =>
          val nameWithType = defn.getNameWithType
          val defName =
            if (nameWithType.getNameCount == 0) {
              assertSince(LV.Features.internedDottedNames, "interned dotted names table")
              getInternedDottedName(nameWithType.getNameInternedId)
            } else {
              decodeSegments(ImmArray(nameWithType.getNameList.asScala))
            }
          currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
          val d = decodeDefValue(defn)
          defs += (defName -> d)
        }
      }

      // collect templates
      lfModule.getTemplatesList.asScala.foreach { defn =>
        val defName = decodeDottedName(defn.getTycon)
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        templates += ((defName, decodeTemplate(defn)))
      }

      Module(moduleName, defs, templates, languageVersion, decodeFeatureFlags(lfModule.getFlags))
    }

    // -----------------------------------------------------------------------

    private[this] def getInternedString(id: Long): String = {
      assertSince(LV.Features.internedStrings, "interned strings table")
      def outOfRange = ParseError(s"invalid string table index $id")
      val iid = id.toInt
      if (iid != iid.toLong) throw outOfRange
      internedStrings.lift(iid).getOrElse(throw outOfRange)
    }

    private[this] def getInternedDottedName(id: Long): DottedName = {
      assertSince(LV.Features.internedDottedNames, "interned dotted name")

      def outOfRange = ParseError(s"invalid dotted name table index $id")
      val iid = id.toInt
      if (iid != iid.toLong) throw outOfRange
      internedDottedNames.lift(iid).getOrElse(throw outOfRange)
    }

    private[this] def decodeDottedName(name: PLF.DottedName): DottedName =
      if (name.getSegmentsCount == 0) {
        getInternedDottedName(name.getSegmentsInternedId)
      } else {
        decodeSegments(ImmArray(name.getSegmentsList.asScala))
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

    private[this] def decodeFieldWithType(lfFieldWithType: PLF.FieldWithType): (Name, Type) =
      name(lfFieldWithType.getFieldCase match {
        case PLF.FieldWithType.FieldCase.FIELD_NAME => lfFieldWithType.getFieldName
        case PLF.FieldWithType.FieldCase.FIELD_INTERNED_ID =>
          getInternedString(lfFieldWithType.getFieldInternedId)
        case PLF.FieldWithType.FieldCase.FIELD_NOT_SET =>
          throw ParseError("FieldWithType.FIELD_NOT_SET")

      }) -> decodeType(lfFieldWithType.getType)

    private[this] def decodeFields(lfFields: ImmArray[PLF.FieldWithType]): ImmArray[(Name, Type)] =
      lfFields.map(decodeFieldWithType)

    private[this] def decodeFieldWithExpr(
        lfFieldWithExpr: PLF.FieldWithExpr,
        definition: String): (Name, Expr) =
      name(lfFieldWithExpr.getFieldCase match {
        case PLF.FieldWithExpr.FieldCase.FIELD_NAME => lfFieldWithExpr.getFieldName
        case PLF.FieldWithExpr.FieldCase.FIELD_INTERNED_ID =>
          getInternedString(lfFieldWithExpr.getFieldInternedId)
        case PLF.FieldWithExpr.FieldCase.FIELD_NOT_SET =>
          throw ParseError("FieldWithExpr.FIELD_NOT_SET")

      }) -> decodeExpr(lfFieldWithExpr.getExpr, definition)

    private[this] def decodeEnumCons(cons: ImmArray[String]): ImmArray[EnumConName] =
      cons.map(name)

    private[this] def decodeDefValue(lfValue: PLF.DefValue): DValue = {
      val nameWithType = lfValue.getNameWithType
      val definition =
        if (nameWithType.getNameCount == 0)
          getInternedDottedName(nameWithType.getNameInternedId).segments.toSeq.mkString(".")
        else
          nameWithType.getNameList.asScala.mkString(".")
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
              name(field.getFieldCase match {
                case PLF.KeyExpr.RecordField.FieldCase.FIELD_NAME =>
                  field.getFieldName
                case PLF.KeyExpr.RecordField.FieldCase.FIELD_INTERNED_ID =>
                  getInternedString(field.getFieldInternedId)
                case PLF.KeyExpr.RecordField.FieldCase.FIELD_NOT_SET =>
                  throw ParseError("KeyExpr.Record.Field.FIELD_NOT_SET")

              }) -> decodeKeyExpr(field.getExpr, tplVar))
          )

        case PLF.KeyExpr.SumCase.PROJECTIONS =>
          val lfProjs = expr.getProjections.getProjectionsList.asScala
          lfProjs.foldLeft(EVar(tplVar): Expr)(
            (acc, lfProj) =>
              ERecProj(
                decodeTypeConApp(lfProj.getTycon),
                name(lfProj.getFieldCase match {
                  case PLF.KeyExpr.Projection.FieldCase.FIELD_NAME => lfProj.getFieldName
                  case PLF.KeyExpr.Projection.FieldCase.FIELD_INTERNED_ID =>
                    getInternedString(lfProj.getFieldInternedId)
                  case PLF.KeyExpr.Projection.FieldCase.FIELD_NOT_SET =>
                    throw ParseError("KeyExpr.Projection.Field.FIELD_NOT_SET")
                }),
                acc
            ))

        case PLF.KeyExpr.SumCase.SUM_NOT_SET =>
          throw ParseError("KeyExpr.SUM_NOT_SET")
      }
    }

    private[this] def decodeTemplate(lfTempl: PLF.DefTemplate): Template = {
      val tpl = lfTempl.getTycon.getSegmentsList.asScala.mkString(".")
      val paramName = name(lfTempl.getParamCase match {
        case PLF.DefTemplate.ParamCase.PARAM_NAME => lfTempl.getParamName
        case PLF.DefTemplate.ParamCase.PARAM_INTERNED_ID =>
          getInternedString(lfTempl.getParamInternedId)
        case PLF.DefTemplate.ParamCase.PARAM_NOT_SET =>
          throw ParseError("DefTemplate.PARAM_NOT_SET")
      })
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

    private[this] def decodeChoice(tpl: String, lfChoice: PLF.TemplateChoice): TemplateChoice = {
      val (v, t) = decodeBinder(lfChoice.getArgBinder)
      val chName: String = lfChoice.getNameCase match {
        case PLF.TemplateChoice.NameCase.CHOICE_NAME =>
          lfChoice.getChoiceName
        case PLF.TemplateChoice.NameCase.CHOICE_INTERNED_ID =>
          getInternedString(lfChoice.getChoiceInternedId)
        case PLF.TemplateChoice.NameCase.NAME_NOT_SET =>
          throw ParseError("TemplateChoice.NAME_NOT_SET")

      }
      val selfBinder: String = lfChoice.getSelfBinderCase match {
        case PLF.TemplateChoice.SelfBinderCase.SELF_BINDER_NAME =>
          lfChoice.getSelfBinderName
        case PLF.TemplateChoice.SelfBinderCase.SELF_BINDER_INTERNED_ID =>
          getInternedString(lfChoice.getSelfBinderInternedId)
        case PLF.TemplateChoice.SelfBinderCase.SELFBINDER_NOT_SET =>
          throw ParseError("TemplateChoice.SELFBINDER_NOT_SET")
      }

      TemplateChoice(
        name = name(chName),
        consuming = lfChoice.getConsuming,
        controllers = decodeExpr(lfChoice.getControllers, s"$tpl:$chName:controller"),
        selfBinder = name(selfBinder),
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
          val varName = name(tvar.getVarCase match {
            case PLF.Type.Var.VarCase.VAR_NAME => tvar.getVarName
            case PLF.Type.Var.VarCase.VAR_INTERNED_ID =>
              getInternedString(tvar.getVarInternedId)
            case PLF.Type.Var.VarCase.VAR_NOT_SET =>
              throw ParseError("Type.VAR_NOT_SET")

          })
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
              TNat
            )
        case PLF.Type.SumCase.CON =>
          val tcon = lfType.getCon
          (TTyCon(decodeTypeConName(tcon.getTycon)) /: [Type] tcon.getArgsList.asScala)(
            (typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.PRIM =>
          val prim = lfType.getPrim
          val baseType =
            if (prim.getPrim == PLF.PrimType.DECIMAL) {
              assertUntil(LV.Features.numeric, "PrimType.DECIMAL")
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
          TTuple(fields.map(decodeFieldWithType)(breakOut))

        case PLF.Type.SumCase.SUM_NOT_SET =>
          throw ParseError("Type.SUM_NOT_SET")
      }

    private[this] def decodeModuleRef(lfRef: PLF.ModuleRef): (PackageId, ModuleName) = {
      val modName = decodeDottedName(lfRef.getModuleName)
      import PLF.PackageRef.{SumCase => SC}

      def toPackageId(rawPid: String): PackageId =
        PackageId
          .fromString(rawPid)
          .getOrElse(throw ParseError(s"invalid packageId '$rawPid'"))

      val pkgId = lfRef.getPackageRef.getSumCase match {
        case SC.SELF =>
          this.packageId
        case SC.PACKAGE_ID =>
          toPackageId(lfRef.getPackageRef.getPackageId)
        case SC.INTERNED_ID =>
          toPackageId(getInternedString(lfRef.getPackageRef.getInternedId))
        case SC.SUM_NOT_SET =>
          throw ParseError("PackageRef.SUM_NOT_SET")
      }
      (pkgId, modName)
    }

    private[this] def decodeValName(lfVal: PLF.ValName): ValueRef = {
      val (packageId, module) = decodeModuleRef(lfVal.getModule)
      val name: DottedName =
        if (lfVal.getNameCount == 0) {
          getInternedDottedName(lfVal.getNameInternedId)
        } else {
          decodeSegments(ImmArray(lfVal.getNameList.asScala))
        }
      ValueRef(packageId, QualifiedName(module, name))
    }

    private[this] def decodeTypeConName(lfTyConName: PLF.TypeConName): TypeConName = {
      val (packageId, module) = decodeModuleRef(lfTyConName.getModule)
      val name = decodeDottedName(lfTyConName.getName)
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

        case PLF.Expr.SumCase.VAR_INTERNED_ID =>
          EVar(name(getInternedString(lfExpr.getVarInternedId)))

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
          ntimes[Expr](
            info.implicitDecimalScaleParameters,
            ETyApp(_, TDecimalScale),
            EBuiltin(info.builtin))

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
            field = name(recProj.getFieldCase match {
              case PLF.Expr.RecProj.FieldCase.NAME => recProj.getName
              case PLF.Expr.RecProj.FieldCase.INTERNED_ID =>
                getInternedString(recProj.getInternedId)
              case PLF.Expr.RecProj.FieldCase.FIELD_NOT_SET =>
                throw ParseError("Expr.RecProj.FIELD_NOT_SET")
            }),
            record = decodeExpr(recProj.getRecord, definition)
          )

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          ERecUpd(
            tycon = decodeTypeConApp(recUpd.getTycon),
            field = name(recUpd.getFieldCase match {
              case PLF.Expr.RecUpd.FieldCase.NAME => recUpd.getName
              case PLF.Expr.RecUpd.FieldCase.INTERNED_ID =>
                getInternedString(recUpd.getInternedId)
              case PLF.Expr.RecUpd.FieldCase.FIELD_NOT_SET =>
                throw ParseError("Expr.RecUpd.FIELD_NOT_SET")
            }),
            record = decodeExpr(recUpd.getRecord, definition),
            update = decodeExpr(recUpd.getUpdate, definition)
          )

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          EVariantCon(
            decodeTypeConApp(varCon.getTycon),
            name(varCon.getVariantConCase match {
              case PLF.Expr.VariantCon.VariantConCase.NAME => varCon.getName
              case PLF.Expr.VariantCon.VariantConCase.INTERNED_ID =>
                getInternedString(varCon.getInternedId)
              case PLF.Expr.VariantCon.VariantConCase.VARIANTCON_NOT_SET =>
                throw ParseError("Expr.VariantCon.VARIANTCON_NOT_SET")
            }),
            decodeExpr(varCon.getVariantArg, definition)
          )

        case PLF.Expr.SumCase.ENUM_CON =>
          assertSince(LV.Features.enum, "Expr.Enum")
          val enumCon = lfExpr.getEnumCon
          EEnumCon(
            decodeTypeConName(enumCon.getTycon),
            name(enumCon.getEnumConCase match {
              case PLF.Expr.EnumCon.EnumConCase.NAME => enumCon.getName
              case PLF.Expr.EnumCon.EnumConCase.INTERNED_ID =>
                getInternedString(enumCon.getInternedId)
              case PLF.Expr.EnumCon.EnumConCase.ENUMCON_NOT_SET =>
                throw ParseError("Expr.EnumCon.ENUMCON_NOT_SET")
            })
          )

        case PLF.Expr.SumCase.TUPLE_CON =>
          val tupleCon = lfExpr.getTupleCon
          ETupleCon(
            ImmArray(tupleCon.getFieldsList.asScala).map(decodeFieldWithExpr(_, definition))
          )

        case PLF.Expr.SumCase.TUPLE_PROJ =>
          val tupleProj = lfExpr.getTupleProj
          ETupleProj(
            name(tupleProj.getFieldCase match {
              case PLF.Expr.TupleProj.FieldCase.NAME => tupleProj.getName
              case PLF.Expr.TupleProj.FieldCase.INTERNED_ID =>
                getInternedString(tupleProj.getInternedId)
              case PLF.Expr.TupleProj.FieldCase.FIELD_NOT_SET =>
                throw ParseError("Expr.TupleProj.FIELD_NOT_SET")
            }),
            decodeExpr(tupleProj.getTuple, definition)
          )

        case PLF.Expr.SumCase.TUPLE_UPD =>
          val tupleUpd = lfExpr.getTupleUpd
          ETupleUpd(
            field = name(tupleUpd.getFieldCase match {
              case PLF.Expr.TupleUpd.FieldCase.NAME => tupleUpd.getName
              case PLF.Expr.TupleUpd.FieldCase.INTERNED_ID =>
                getInternedString(tupleUpd.getInternedId)
              case PLF.Expr.TupleUpd.FieldCase.FIELD_NOT_SET =>
                throw ParseError("Expr.TupleUpd.FIELD_NOT_SET")
            }),
            tuple = decodeExpr(tupleUpd.getTuple, definition),
            update = decodeExpr(tupleUpd.getUpdate, definition)
          )

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

        case PLF.Expr.SumCase.TO_ANY =>
          assertSince(LV.Features.anyTemplate, "Expr.ToAnyTemplate")
          decodeType(lfExpr.getToAny.getType) match {
            case TTyCon(tmplId) =>
              EToAnyTemplate(
                tmplId = tmplId,
                body = decodeExpr(lfExpr.getToAny.getExpr, definition))
            case ty => throw ParseError(s"TO_ANY must be applied to a template type but got $ty")
          }

        case PLF.Expr.SumCase.FROM_ANY =>
          assertSince(LV.Features.anyTemplate, "Expr.FromAnyTemplate")
          val fromAny = lfExpr.getFromAny
          decodeType(fromAny.getType) match {
            case TTyCon(tmplId) =>
              EFromAnyTemplate(tmplId = tmplId, body = decodeExpr(fromAny.getExpr, definition))
            case ty => throw ParseError(s"FROM_ANY must be applied to a template type but got $ty")
          }

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
            name(variant.getVariantCase match {
              case PLF.CaseAlt.Variant.VariantCase.VARIANT_NAME =>
                variant.getVariantName
              case PLF.CaseAlt.Variant.VariantCase.VARIANT_INTERNED_ID =>
                getInternedString(variant.getVariantInternedId)
              case PLF.CaseAlt.Variant.VariantCase.VARIANT_NOT_SET =>
                throw ParseError("CaseAlt.Variant.VARIANT_NOT_SET")
            }),
            name(variant.getBinderCase match {
              case PLF.CaseAlt.Variant.BinderCase.BINDER_NAME =>
                variant.getBinderName
              case PLF.CaseAlt.Variant.BinderCase.BINDER_INTERNED_ID =>
                getInternedString(variant.getBinderInternedId)
              case PLF.CaseAlt.Variant.BinderCase.BINDER_NOT_SET =>
                throw ParseError("CaseAlt.Variant.BINDER_NOT_SET")
            })
          )
        case PLF.CaseAlt.SumCase.ENUM =>
          assertSince(LV.Features.enum, "CaseAlt.Enum")
          val enum = lfCaseAlt.getEnum
          CPEnum(
            decodeTypeConName(enum.getCon),
            name(enum.getConstructorCase match {
              case PLF.CaseAlt.Enum.ConstructorCase.NAME => enum.getName
              case PLF.CaseAlt.Enum.ConstructorCase.INTERNED_ID =>
                getInternedString(enum.getInternedId)
              case PLF.CaseAlt.Enum.ConstructorCase.CONSTRUCTOR_NOT_SET =>
                throw ParseError("CaseAlt.Enum.CONSTRUCTOR_NOT_SET")
            })
          )
        case PLF.CaseAlt.SumCase.PRIM_CON =>
          CPPrimCon(decodePrimCon(lfCaseAlt.getPrimCon))
        case PLF.CaseAlt.SumCase.NIL =>
          CPNil
        case PLF.CaseAlt.SumCase.CONS =>
          val cons = lfCaseAlt.getCons
          CPCons(
            name(cons.getVarHeadCase match {
              case PLF.CaseAlt.Cons.VarHeadCase.VAR_HEAD_NAME => cons.getVarHeadName
              case PLF.CaseAlt.Cons.VarHeadCase.VAR_HEAD_INTERNED_ID =>
                getInternedString(cons.getVarHeadInternedId)
              case PLF.CaseAlt.Cons.VarHeadCase.VARHEAD_NOT_SET =>
                throw ParseError("CaseAlt.Cons.VARHEAD_NOT_SET")
            }),
            name(cons.getVarTailCase match {
              case PLF.CaseAlt.Cons.VarTailCase.VAR_TAIL_NAME => cons.getVarTailName
              case PLF.CaseAlt.Cons.VarTailCase.VAR_TAIL_INTERNED_ID =>
                getInternedString(cons.getVarTailInternedId)
              case PLF.CaseAlt.Cons.VarTailCase.VARTAIL_NOT_SET =>
                throw ParseError("CaseAlt.Cons.VARTAIL_NOT_SET")
            })
          )

        case PLF.CaseAlt.SumCase.OPTIONAL_NONE =>
          assertSince(LV.Features.optional, "CaseAlt.OptionalNone")
          CPNone

        case PLF.CaseAlt.SumCase.OPTIONAL_SOME =>
          assertSince(LV.Features.optional, "CaseAlt.OptionalSome")
          CPSome(name(lfCaseAlt.getOptionalSome.getVarBodyCase match {
            case PLF.CaseAlt.OptionalSome.VarBodyCase.NAME => lfCaseAlt.getOptionalSome.getName
            case PLF.CaseAlt.OptionalSome.VarBodyCase.INTERNED_ID =>
              getInternedString(lfCaseAlt.getOptionalSome.getInternedId)
            case PLF.CaseAlt.OptionalSome.VarBodyCase.VARBODY_NOT_SET =>
              throw ParseError("CaseAlt.OptionalSome.VARBODY_NOT_SET")
          }))

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
            choice = name(exercise.getChoiceCase match {
              case PLF.Update.Exercise.ChoiceCase.NAME => exercise.getName
              case PLF.Update.Exercise.ChoiceCase.INTERNED_ID =>
                getInternedString(exercise.getInternedId)
              case PLF.Update.Exercise.ChoiceCase.CHOICE_NOT_SET =>
                throw ParseError("Update.Exercise.CHOICE_NOT_SET")
            }),
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
      name(lfTypeVarWithKind.getVarCase match {
        case PLF.TypeVarWithKind.VarCase.NAME => lfTypeVarWithKind.getName
        case PLF.TypeVarWithKind.VarCase.INTERNED_ID =>
          getInternedString(lfTypeVarWithKind.getInternedId)
        case PLF.TypeVarWithKind.VarCase.VAR_NOT_SET =>
          throw ParseError("TypeVarWithKind.VAR_NOT_SET")

      }) -> decodeKind(lfTypeVarWithKind.getKind)

    private[this] def decodeBinding(lfBinding: PLF.Binding, definition: String): Binding = {
      val (binder, typ) = decodeBinder(lfBinding.getBinder)
      Binding(Some(binder), typ, decodeExpr(lfBinding.getBound, definition))
    }

    private[this] def decodeBinder(lfBinder: PLF.VarWithType): (ExprVarName, Type) =
      name(lfBinder.getVarCase match {
        case PLF.VarWithType.VarCase.NAME => lfBinder.getName
        case PLF.VarWithType.VarCase.INTERNED_ID => getInternedString(lfBinder.getInternedId)
        case PLF.VarWithType.VarCase.VAR_NOT_SET =>
          throw ParseError("VarWithType.VAR_NOT_SET")
      }) -> decodeType(lfBinder.getType)

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
          assertUntil(LV.Features.numeric, "PrimLit.decimal")
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
        case PLF.PrimLit.SumCase.TEXT_INTERNED_ID =>
          PLText(getInternedString(lfPrimLit.getTextInternedId))
        case PLF.PrimLit.SumCase.DECIMAL_INTERNED_ID =>
          assertUntil(LV.Features.numeric, "PrimLit.decimal")
          Decimal
            .fromString(getInternedString(lfPrimLit.getDecimalInternedId))
            .flatMap(Numeric.fromBigDecimal(Decimal.scale, _))
            .fold(e => throw ParseError("error parsing decimal: " + e), PLNumeric)
        case PLF.PrimLit.SumCase.NUMERIC_INTERNED_ID =>
          assertSince(LV.Features.numeric, "PrimLit.numeric")
          Numeric
            .fromString(getInternedString(lfPrimLit.getNumericInternedId))
            .fold(e => throw ParseError("error parsing numeric: " + e), PLNumeric)
        case PLF.PrimLit.SumCase.PARTY_INTERNED_ID =>
          val p = Party
            .fromString(getInternedString(lfPrimLit.getPartyInternedId))
            .getOrElse(throw ParseError(s"invalid party '${lfPrimLit.getParty}'"))
          PLParty(p)
        case PLF.PrimLit.SumCase.SUM_NOT_SET =>
          throw ParseError("PrimLit.SUM_NOT_SET")
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

  @tailrec
  private def ntimes[A](n: Int, f: A => A, a: A): A =
    if (n == 0) a else ntimes(n - 1, f, f(a))

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
      BuiltinTypeInfo(NUMERIC, BTNumeric, minVersion = numeric),
      BuiltinTypeInfo(ANY, BTAnyTemplate, minVersion = anyTemplate)
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
      implicitDecimalScaleParameters: Int = 0
  )

  val builtinFunctionInfos: List[BuiltinFunctionInfo] = {
    import PLF.BuiltinFunction._, LV.Features._
    List(
      BuiltinFunctionInfo(
        ADD_DECIMAL,
        BAddNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
      ),
      BuiltinFunctionInfo(
        SUB_DECIMAL,
        BSubNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
      ),
      BuiltinFunctionInfo(
        MUL_DECIMAL,
        BMulNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 3
      ),
      BuiltinFunctionInfo(
        DIV_DECIMAL,
        BDivNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 3
      ),
      BuiltinFunctionInfo(
        ROUND_DECIMAL,
        BRoundNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
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
        implicitDecimalScaleParameters = 1
      ),
      BuiltinFunctionInfo(
        DECIMAL_TO_INT64,
        BNumericToInt64,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
      ),
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
        implicitDecimalScaleParameters = 1
      ),
      BuiltinFunctionInfo(LEQ_NUMERIC, BLessEqNumeric, minVersion = numeric),
      BuiltinFunctionInfo(LEQ_TEXT, BLessEqText),
      BuiltinFunctionInfo(LEQ_TIMESTAMP, BLessEqTimestamp),
      BuiltinFunctionInfo(LEQ_PARTY, BLessEqParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(GEQ_INT64, BGreaterEqInt64),
      BuiltinFunctionInfo(
        GEQ_DECIMAL,
        BGreaterEqNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
      ),
      BuiltinFunctionInfo(GEQ_NUMERIC, BGreaterEqNumeric, minVersion = numeric),
      BuiltinFunctionInfo(GEQ_TEXT, BGreaterEqText),
      BuiltinFunctionInfo(GEQ_TIMESTAMP, BGreaterEqTimestamp),
      BuiltinFunctionInfo(GEQ_PARTY, BGreaterEqParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(LESS_INT64, BLessInt64),
      BuiltinFunctionInfo(
        LESS_DECIMAL,
        BLessNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
      ),
      BuiltinFunctionInfo(LESS_NUMERIC, BLessNumeric, minVersion = numeric),
      BuiltinFunctionInfo(LESS_TEXT, BLessText),
      BuiltinFunctionInfo(LESS_TIMESTAMP, BLessTimestamp),
      BuiltinFunctionInfo(LESS_PARTY, BLessParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(GREATER_INT64, BGreaterInt64),
      BuiltinFunctionInfo(
        GREATER_DECIMAL,
        BGreaterNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
      ),
      BuiltinFunctionInfo(GREATER_NUMERIC, BGreaterNumeric, minVersion = numeric),
      BuiltinFunctionInfo(GREATER_TEXT, BGreaterText),
      BuiltinFunctionInfo(GREATER_TIMESTAMP, BGreaterTimestamp),
      BuiltinFunctionInfo(GREATER_PARTY, BGreaterParty, minVersion = partyOrdering),
      BuiltinFunctionInfo(TO_TEXT_INT64, BToTextInt64),
      BuiltinFunctionInfo(
        TO_TEXT_DECIMAL,
        BToTextNumeric,
        maxVersion = Some(numeric),
        implicitDecimalScaleParameters = 1
      ),
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
        implicitDecimalScaleParameters = 1,
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
        implicitDecimalScaleParameters = 1
      ),
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
