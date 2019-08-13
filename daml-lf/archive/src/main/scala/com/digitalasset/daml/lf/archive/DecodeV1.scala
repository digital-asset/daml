// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package archive

import com.digitalasset.daml.lf.archive.Decode.ParseError
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{Decimal, ImmArray, Time}
import ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion => LV}
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

    var definition: String = "<unknown>"

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
      definition = lfValue.getNameWithType.getNameList.asScala.mkString(".")
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
      definition = s"${tpl}:key"
      val keyExpr = key.getKeyExprCase match {
        case PLF.DefTemplate.DefKey.KeyExprCase.KEY =>
          decodeKeyExpr(key.getKey, tplVar)
        case PLF.DefTemplate.DefKey.KeyExprCase.COMPLEX_KEY => {
          assertSince(LV.Features.complexContactKeys, "DefTemplate.DefKey.complex_key")
          decodeExpr(key.getComplexKey)
        }
        case PLF.DefTemplate.DefKey.KeyExprCase.KEYEXPR_NOT_SET =>
          throw ParseError("DefKey.KEYEXPR_NOT_SET")
      }
      definition = s"${tpl}:maintainer"
      val maintainer = decodeExpr(key.getMaintainers)
      TemplateKey(
        decodeType(key.getType),
        keyExpr,
        maintainer
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
      definition = s"${tpl}:ensure"
      val precond = if (lfTempl.hasPrecond) decodeExpr(lfTempl.getPrecond) else ETrue
      definition = s"${tpl}:signatory"
      val signatories = decodeExpr(lfTempl.getSignatories)
      definition = s"${tpl}:agreement"
      val agreementText = decodeExpr(lfTempl.getAgreement)
      definition = s"${tpl}:observer"
      val observers = decodeExpr(lfTempl.getObservers)

      Template(
        param = name(lfTempl.getParam),
        precond = precond,
        signatories = signatories,
        agreementText = agreementText,
        choices = lfTempl.getChoicesList.asScala
          .map(decodeChoice(tpl, _))
          .map(ch => (ch.name, ch)),
        observers = observers,
        key =
          if (lfTempl.hasKey) Some(decodeTemplateKey(tpl, lfTempl.getKey, name(lfTempl.getParam)))
          else None
      )
    }

    private[this] def decodeChoice(tpl: String, lfChoice: PLF.TemplateChoice): TemplateChoice = {
      val (v, t) = decodeBinder(lfChoice.getArgBinder)
      val chName = lfChoice.getName
      definition = s"${tpl}:${chName}:controller"
      val controllers = decodeExpr(lfChoice.getControllers)
      definition = s"${tpl}:${chName}:choice"
      val update = decodeExpr(lfChoice.getUpdate)

      TemplateChoice(
        name = name(chName),
        consuming = lfChoice.getConsuming,
        controllers = controllers,
        selfBinder = name(lfChoice.getSelfBinder),
        argBinder = Some(v) -> t,
        returnType = decodeType(lfChoice.getRetType),
        update = update
      )
    }

    private[this] def decodeKind(lfKind: PLF.Kind): Kind =
      lfKind.getSumCase match {
        case PLF.Kind.SumCase.STAR => KStar
        case PLF.Kind.SumCase.ARROW =>
          val kArrow = lfKind.getArrow
          val params = kArrow.getParamsList.asScala
          assertNonEmpty(params, "params")
          (params :\ decodeKind(kArrow.getResult))((param, kind) => KArrow(decodeKind(param), kind))
        case PLF.Kind.SumCase.NAT =>
          // FixMe: https://github.com/digital-asset/daml/issues/2289
          throw new Error("nat kind not supported")
        case PLF.Kind.SumCase.SUM_NOT_SET =>
          throw ParseError("Kind.SUM_NOT_SET")
      }

    private[this] def decodeType(lfType: PLF.Type): Type =
      lfType.getSumCase match {
        case PLF.Type.SumCase.VAR =>
          val tvar = lfType.getVar
          tvar.getArgsList.asScala
            .foldLeft[Type](TVar(name(tvar.getVar)))((typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.CON =>
          val tcon = lfType.getCon
          (TTyCon(decodeTypeConName(tcon.getTycon)) /: [Type] tcon.getArgsList.asScala)(
            (typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.PRIM =>
          val prim = lfType.getPrim
          val (tPrim, minVersion) = primTypeTable(prim.getPrim)
          assertSince(minVersion, prim.getPrim.getValueDescriptor.getFullName)
          (TBuiltin(tPrim) /: [Type] prim.getArgsList.asScala)((typ, arg) =>
            TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.FUN =>
          assertUntil(LV.Features.default, "Type.Fun")
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
        case PLF.Type.SumCase.NAT =>
          // FixMe: https://github.com/digital-asset/daml/issues/2289
          throw new Error("nat type not supported")

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

    private[this] def decodeExpr(lfExpr: PLF.Expr): Expr =
      decodeLocation(lfExpr) match {
        case None => decodeExprBody(lfExpr)
        case Some(loc) => ELocation(loc, decodeExprBody(lfExpr))
      }

    private[this] def decodeExprBody(lfExpr: PLF.Expr): Expr =
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
          val (builtin, minVersion) = DecodeV1.builtinFunctionMap(lfExpr.getBuiltin)
          assertSince(minVersion, lfExpr.getBuiltin.getValueDescriptor.getFullName)
          EBuiltin(builtin)

        case PLF.Expr.SumCase.REC_CON =>
          val recCon = lfExpr.getRecCon
          ERecCon(
            tycon = decodeTypeConApp(recCon.getTycon),
            fields = ImmArray(recCon.getFieldsList.asScala).map(field =>
              name(field.getField) -> decodeExpr(field.getExpr))
          )

        case PLF.Expr.SumCase.REC_PROJ =>
          val recProj = lfExpr.getRecProj
          ERecProj(
            tycon = decodeTypeConApp(recProj.getTycon),
            field = name(recProj.getField),
            record = decodeExpr(recProj.getRecord))

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          ERecUpd(
            tycon = decodeTypeConApp(recUpd.getTycon),
            field = name(recUpd.getField),
            record = decodeExpr(recUpd.getRecord),
            update = decodeExpr(recUpd.getUpdate))

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          EVariantCon(
            decodeTypeConApp(varCon.getTycon),
            name(varCon.getVariantCon),
            decodeExpr(varCon.getVariantArg))

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
              name(field.getField) -> decodeExpr(field.getExpr))
          )

        case PLF.Expr.SumCase.TUPLE_PROJ =>
          val tupleProj = lfExpr.getTupleProj
          ETupleProj(name(tupleProj.getField), decodeExpr(tupleProj.getTuple))

        case PLF.Expr.SumCase.TUPLE_UPD =>
          val tupleUpd = lfExpr.getTupleUpd
          ETupleUpd(
            field = name(tupleUpd.getField),
            tuple = decodeExpr(tupleUpd.getTuple),
            update = decodeExpr(tupleUpd.getUpdate))

        case PLF.Expr.SumCase.APP =>
          val app = lfExpr.getApp
          val args = app.getArgsList.asScala
          assertNonEmpty(args, "args")
          (decodeExpr(app.getFun) /: args)((e, arg) => EApp(e, decodeExpr(arg)))

        case PLF.Expr.SumCase.ABS =>
          val lfAbs = lfExpr.getAbs
          val params = lfAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          // val params = lfAbs.getParamList.asScala.map(decodeBinder)
          (params :\ decodeExpr(lfAbs.getBody))((param, e) =>
            EAbs(decodeBinder(param), e, currentDefinitionRef))

        case PLF.Expr.SumCase.TY_APP =>
          val tyapp = lfExpr.getTyApp
          val args = tyapp.getTypesList.asScala
          assertNonEmpty(args, "args")
          (decodeExpr(tyapp.getExpr) /: args)((e, arg) => ETyApp(e, decodeType(arg)))

        case PLF.Expr.SumCase.TY_ABS =>
          val lfTyAbs = lfExpr.getTyAbs
          val params = lfTyAbs.getParamList.asScala
          assertNonEmpty(params, "params")
          (params :\ decodeExpr(lfTyAbs.getBody))((param, e) =>
            ETyAbs(decodeTypeVarWithKind(param), e))

        case PLF.Expr.SumCase.LET =>
          val lfLet = lfExpr.getLet
          val bindings = lfLet.getBindingsList.asScala
          assertNonEmpty(bindings, "bindings")
          (bindings :\ decodeExpr(lfLet.getBody))((binding, e) => {
            val (v, t) = decodeBinder(binding.getBinder)
            ELet(Binding(Some(v), t, decodeExpr(binding.getBound)), e)
          })

        case PLF.Expr.SumCase.NIL =>
          ENil(decodeType(lfExpr.getNil.getType))

        case PLF.Expr.SumCase.CONS =>
          val cons = lfExpr.getCons
          val front = cons.getFrontList.asScala
          assertNonEmpty(front, "front")
          val typ = decodeType(cons.getType)
          ECons(typ, ImmArray(front.map(decodeExpr)), decodeExpr(cons.getTail))

        case PLF.Expr.SumCase.CASE =>
          val case_ = lfExpr.getCase
          ECase(
            decodeExpr(case_.getScrut),
            ImmArray(case_.getAltsList.asScala).map(decodeCaseAlt)
          )

        case PLF.Expr.SumCase.UPDATE =>
          EUpdate(decodeUpdate(lfExpr.getUpdate))

        case PLF.Expr.SumCase.SCENARIO =>
          EScenario(decodeScenario(lfExpr.getScenario))

        case PLF.Expr.SumCase.OPTIONAL_NONE =>
          assertSince(LV.Features.optional, "Expr.OptionalNone")
          ENone(decodeType(lfExpr.getOptionalNone.getType))

        case PLF.Expr.SumCase.OPTIONAL_SOME =>
          assertSince(LV.Features.optional, "Expr.OptionalSome")
          val some = lfExpr.getOptionalSome
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
            choice = name(exercise.getChoice),
            cidE = decodeExpr(exercise.getCid),
            actorsE =
              if (exercise.hasActor)
                Some(decodeExpr(exercise.getActor))
              else {
                assertSince(LV.Features.optionalExerciseActor, "Update.Exercise.actors optional")
                None
              },
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
          assertSince(LV.Features.contractKeys, "fetchByKey")
          UpdateFetchByKey(decodeRetrieveByKey(lfUpdate.getFetchByKey))

        case PLF.Update.SumCase.LOOKUP_BY_KEY =>
          assertSince(LV.Features.contractKeys, "lookupByKey")
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
            bindings = ImmArray(block.getBindingsList.asScala).map(decodeBinding),
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
        lfTypeVarWithKind: PLF.TypeVarWithKind): (TypeVarName, Kind) =
      name(lfTypeVarWithKind.getVar) -> decodeKind(lfTypeVarWithKind.getKind)

    private[this] def decodeBinding(lfBinding: PLF.Binding): Binding = {
      val (binder, typ) = decodeBinder(lfBinding.getBinder)
      Binding(Some(binder), typ, decodeExpr(lfBinding.getBound))
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
        case PLF.PrimLit.SumCase.NUMERIC =>
          checkDecimal(lfPrimLit.getNumeric)
          val d = Decimal.fromString(lfPrimLit.getNumeric)
          d.fold(e => throw ParseError("error parsing decimal: " + e), PLDecimal)
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

  private def assertUntil(maxVersion: LV, description: String): Unit =
    if (LV.ordering.gt(languageVersion, maxVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertSince(minVersion: LV, description: String): Unit =
    if (LV.ordering.lt(languageVersion, minVersion))
      throw ParseError(s"$description is not supported by DAML-LF 1.$minor")

  private def assertNonEmpty(s: Seq[_], description: String): Unit =
    if (s.isEmpty) throw ParseError(s"Unexpected empty $description")

  private def assertEmpty(s: Seq[_], description: String): Unit =
    if (s.nonEmpty) throw ParseError(s"Unexpected non-empty $description")

}

private[lf] object DecodeV1 {

  val primTypeTable: Map[PLF.PrimType, (BuiltinType, LV)] = {
    import PLF.PrimType._, LV.Features._
    Map(
      UNIT -> (BTUnit -> default),
      BOOL -> (BTBool -> default),
      TEXT -> (BTText -> default),
      INT64 -> (BTInt64 -> default),
      NUMERIC -> (BTDecimal -> default),
      TIMESTAMP -> (BTTimestamp -> default),
      PARTY -> (BTParty -> default),
      LIST -> (BTList -> default),
      UPDATE -> (BTUpdate -> default),
      SCENARIO -> (BTScenario -> default),
      CONTRACT_ID -> (BTContractId -> default),
      DATE -> (BTDate -> default),
      OPTIONAL -> (BTOptional -> optional),
      MAP -> (BTMap -> optional),
      ARROW -> (BTArrow -> arrowType),
    )
  }

  val builtinFunctionMap = {
    import PLF.BuiltinFunction._, LV.Features._

    Map[PLF.BuiltinFunction, (Ast.BuiltinFunction, LV)](
      ADD_NUMERIC -> (BAddDecimal -> default),
      SUB_NUMERIC -> (BSubDecimal -> default),
      MUL_NUMERIC -> (BMulDecimal -> default),
      DIV_NUMERIC -> (BDivDecimal -> default),
      ROUND_NUMERIC -> (BRoundDecimal -> default),
      ADD_INT64 -> (BAddInt64 -> default),
      SUB_INT64 -> (BSubInt64 -> default),
      MUL_INT64 -> (BMulInt64 -> default),
      DIV_INT64 -> (BDivInt64 -> default),
      MOD_INT64 -> (BModInt64 -> default),
      EXP_INT64 -> (BExpInt64 -> default),
      INT64_TO_NUMERIC -> (BInt64ToDecimal -> default),
      NUMERIC_TO_INT64 -> (BDecimalToInt64 -> default),
      FOLDL -> (BFoldl -> default),
      FOLDR -> (BFoldr -> default),
      MAP_EMPTY -> (BMapEmpty -> map),
      MAP_INSERT -> (BMapInsert -> map),
      MAP_LOOKUP -> (BMapLookup -> map),
      MAP_DELETE -> (BMapDelete -> map),
      MAP_TO_LIST -> (BMapToList -> map),
      MAP_SIZE -> (BMapSize -> map),
      APPEND_TEXT -> (BAppendText -> default),
      ERROR -> (BError -> default),
      LEQ_INT64 -> (BLessEqInt64 -> default),
      LEQ_NUMERIC -> (BLessEqDecimal -> default),
      LEQ_TEXT -> (BLessEqText -> default),
      LEQ_TIMESTAMP -> (BLessEqTimestamp -> default),
      LEQ_PARTY -> (BLessEqParty -> partyOrdering),
      GEQ_INT64 -> (BGreaterEqInt64 -> default),
      GEQ_NUMERIC -> (BGreaterEqDecimal -> default),
      GEQ_TEXT -> (BGreaterEqText -> default),
      GEQ_TIMESTAMP -> (BGreaterEqTimestamp -> default),
      GEQ_PARTY -> (BGreaterEqParty -> partyOrdering),
      LESS_INT64 -> (BLessInt64 -> default),
      LESS_NUMERIC -> (BLessDecimal -> default),
      LESS_TEXT -> (BLessText -> default),
      LESS_TIMESTAMP -> (BLessTimestamp -> default),
      LESS_PARTY -> (BLessParty -> partyOrdering),
      GREATER_INT64 -> (BGreaterInt64 -> default),
      GREATER_NUMERIC -> (BGreaterDecimal -> default),
      GREATER_TEXT -> (BGreaterText -> default),
      GREATER_TIMESTAMP -> (BGreaterTimestamp -> default),
      GREATER_PARTY -> (BGreaterParty -> partyOrdering),
      TO_TEXT_INT64 -> (BToTextInt64 -> default),
      TO_TEXT_NUMERIC -> (BToTextDecimal -> default),
      TO_TEXT_TIMESTAMP -> (BToTextTimestamp -> default),
      TO_TEXT_PARTY -> (BToTextParty -> partyTextConversions),
      TO_TEXT_TEXT -> (BToTextText -> default),
      TO_QUOTED_TEXT_PARTY -> (BToQuotedTextParty -> default),
      TEXT_FROM_CODE_POINTS -> (BToTextCodePoints -> textPacking),
      FROM_TEXT_PARTY -> (BFromTextParty -> partyTextConversions),
      FROM_TEXT_INT64 -> (BFromTextInt64 -> numberParsing),
      FROM_TEXT_NUMERIC -> (BFromTextDecimal -> numberParsing),
      TEXT_TO_CODE_POINTS -> (BFromTextCodePoints -> textPacking),
      SHA256_TEXT -> (BSHA256Text -> shaText),
      DATE_TO_UNIX_DAYS -> (BDateToUnixDays -> default),
      EXPLODE_TEXT -> (BExplodeText -> default),
      IMPLODE_TEXT -> (BImplodeText -> default),
      GEQ_DATE -> (BGreaterEqDate -> default),
      LEQ_DATE -> (BLessEqDate -> default),
      LESS_DATE -> (BLessDate -> default),
      TIMESTAMP_TO_UNIX_MICROSECONDS -> (BTimestampToUnixMicroseconds -> default),
      TO_TEXT_DATE -> (BToTextDate -> default),
      UNIX_DAYS_TO_DATE -> (BUnixDaysToDate -> default),
      UNIX_MICROSECONDS_TO_TIMESTAMP -> (BUnixMicrosecondsToTimestamp -> default),
      GREATER_DATE -> (BGreaterDate -> default),
      EQUAL_INT64 -> (BEqualInt64 -> default),
      EQUAL_NUMERIC -> (BEqualDecimal -> default),
      EQUAL_TEXT -> (BEqualText -> default),
      EQUAL_TIMESTAMP -> (BEqualTimestamp -> default),
      EQUAL_DATE -> (BEqualDate -> default),
      EQUAL_PARTY -> (BEqualParty -> default),
      EQUAL_BOOL -> (BEqualBool -> default),
      EQUAL_LIST -> (BEqualList -> default),
      EQUAL_CONTRACT_ID -> (BEqualContractId -> default),
      TRACE -> (BTrace -> default),
      COERCE_CONTRACT_ID -> (BCoerceContractId -> coerceContractId),
    ).withDefault(_ => throw ParseError("BuiltinFunction.UNRECOGNIZED"))
  }

}
