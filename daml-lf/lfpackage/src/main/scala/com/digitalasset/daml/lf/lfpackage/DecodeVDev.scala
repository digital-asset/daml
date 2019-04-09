// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package lfpackage

import com.digitalasset.daml.lf.archive.LanguageVersion

import scala.collection.JavaConverters._
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data.{Decimal, ImmArray, Time}
import com.digitalasset.daml_lf.{DamlLfDev => PLF}

import scala.collection.mutable

private[lf] object DecodeVDev extends Decode.OfPackage[PLF.Package] {

  import Decode._

  override def decodePackage(packageId: SimpleString, lfPackage: PLF.Package): Package =
    Package(lfPackage.getModulesList.asScala.map(ModuleDecoder(packageId, _).decode()))

  private[this] def eitherToParseError[A](x: Either[String, A]): A = {
    x.fold(err => throw new ParseError(err), identity)
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
        defs += defName -> decodeDefDataType(defn)
      }

      // collect values
      lfModule.getValuesList.asScala.foreach { defn =>
        val defName =
          eitherToParseError(DottedName.fromSegments(defn.getNameWithType.getNameList.asScala))
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        defs += defName -> decodeDefValue(defn)
      }

      // collect templates
      lfModule.getTemplatesList.asScala.foreach { defn =>
        val defName =
          eitherToParseError(DottedName.fromSegments(defn.getTycon.getSegmentsList.asScala))
        currentDefinitionRef = Some(DefinitionRef(packageId, QualifiedName(moduleName, defName)))
        templates += defName -> decodeTemplate(defn)
      }

      Module(
        moduleName,
        defs,
        templates,
        LanguageVersion.defaultVDev,
        decodeFeatureFlags(lfModule.getFlags))
    }

    // -----------------------------------------------------------------------

    private[this] def decodeFeatureFlags(flags: PLF.FeatureFlags): FeatureFlags = {
      if (!flags.getDontDivulgeContractIdsInCreateArguments || !flags.getDontDiscloseNonConsumingChoicesToObservers) {
        throw new ParseError("Deprecated feature flag settings detected, refusing to parse package")
      }
      FeatureFlags(
        forbidPartyLiterals = flags.getForbidPartyLiterals,
        dontDivulgeContractIdsInCreateArguments = true,
        dontDiscloseNonConsumingChoicesToObservers = true
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

    private[this] def decodeKeyExpr(templateParameter: String, lfKeyExpr: PLF.KeyExpr): Expr = {
      lfKeyExpr.getSumCase match {
        case PLF.KeyExpr.SumCase.PROJECTIONS =>
          val projections = lfKeyExpr.getProjections
          projections.getProjectionsList.asScala.foldLeft[Expr](EVar(templateParameter)) {
            case (rec, proj) =>
              ERecProj(
                tycon = decodeTypeConApp(proj.getTycon),
                field = proj.getField,
                record = rec
              )
          }

        case PLF.KeyExpr.SumCase.RECORD =>
          val rec = lfKeyExpr.getRecord
          ERecCon(
            tycon = decodeTypeConApp(rec.getTycon),
            fields = ImmArray(rec.getFieldsList.asScala).map { field =>
              (field.getField, decodeKeyExpr(templateParameter, field.getExpr))
            }
          )

        case PLF.KeyExpr.SumCase.SUM_NOT_SET =>
          throw ParseError("KeyExpr.SUM_NOT_SET")
      }
    }

    private[this] def decodeTemplateKey(
        templateParameter: String,
        lfTemplKey: PLF.DefTemplate.DefKey): TemplateKey = {
      TemplateKey(
        decodeType(lfTemplKey.getType),
        decodeKeyExpr(templateParameter, lfTemplKey.getKey),
        decodeExpr(lfTemplKey.getMaintainers),
      )
    }

    private[this] def decodeTemplate(lfTempl: PLF.DefTemplate): Template = {
      val mbKey = if (lfTempl.hasKey) {
        Some(decodeTemplateKey(lfTempl.getParam, lfTempl.getKey))
      } else {
        None
      }
      Template(
        param = lfTempl.getParam,
        precond = if (lfTempl.hasPrecond) decodeExpr(lfTempl.getPrecond) else EPrimCon(PCTrue),
        signatories = decodeExpr(lfTempl.getSignatories),
        agreementText = decodeExpr(lfTempl.getAgreement),
        choices = lfTempl.getChoicesList.asScala
          .map(decodeChoice)
          .map(ch => (ch.name, ch)),
        observers = decodeExpr(lfTempl.getObservers),
        key = mbKey
      )
    }

    private[this] def decodeChoice(lfChoice: PLF.TemplateChoice): TemplateChoice =
      TemplateChoice(
        name = lfChoice.getName,
        consuming = lfChoice.getConsuming,
        controllers = decodeExpr(lfChoice.getControllers),
        selfBinder = lfChoice.getSelfBinder,
        argBinder = decodeBinder(lfChoice.getArgBinder),
        returnType = decodeType(lfChoice.getRetType),
        update = decodeExpr(lfChoice.getUpdate)
      )

    private[this] def decodeKind(lfKind: PLF.Kind): Kind =
      lfKind.getSumCase match {
        case PLF.Kind.SumCase.STAR => KStar
        case PLF.Kind.SumCase.ARROW =>
          val karrow = lfKind.getArrow
          karrow.getParamsList.asScala.foldRight(decodeKind(karrow.getResult))((param, kind) =>
            KArrow(decodeKind(param), kind))
        case PLF.Kind.SumCase.SUM_NOT_SET =>
          throw ParseError("Kind.SUM_NOT_SET")
      }

    private[this] val primTypeTable = Map[PLF.PrimType, BuiltinType](
      PLF.PrimType.UNIT -> BTUnit,
      PLF.PrimType.BOOL -> BTBool,
      PLF.PrimType.TEXT -> BTText,
      PLF.PrimType.INT64 -> BTInt64,
      PLF.PrimType.DECIMAL -> BTDecimal,
      PLF.PrimType.TIMESTAMP -> BTTimestamp,
      PLF.PrimType.PARTY -> BTParty,
      PLF.PrimType.LIST -> BTList,
      PLF.PrimType.UPDATE -> BTUpdate,
      PLF.PrimType.SCENARIO -> BTScenario,
      PLF.PrimType.CONTRACT_ID -> BTContractId,
      PLF.PrimType.DATE -> BTDate,
      PLF.PrimType.OPTIONAL -> BTOptional,
      PLF.PrimType.MAP -> BTMap,
      PLF.PrimType.ARROW -> BTArrow,
    )

    private[this] def decodeType(lfType: PLF.Type): Type =
      lfType.getSumCase match {
        case PLF.Type.SumCase.VAR =>
          val tvar = lfType.getVar
          tvar.getArgsList.asScala
            .foldLeft[Type](TVar(tvar.getVar))((typ, arg) => TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.CON =>
          val tcon = lfType.getCon
          tcon.getArgsList.asScala
            .foldLeft[Type](TTyCon(decodeTypeConName(tcon.getTycon)))((typ, arg) =>
              TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.PRIM =>
          val tprim = lfType.getPrim
          tprim.getArgsList.asScala
            .foldLeft[Type](TBuiltin(primTypeTable(tprim.getPrim)))((typ, arg) =>
              TApp(typ, decodeType(arg)))
        case PLF.Type.SumCase.FORALL =>
          val tforall = lfType.getForall
          (tforall.getVarsList.asScala :\ decodeType(tforall.getBody))((binder, acc) =>
            TForall(decodeTypeVarWithKind(binder), acc))
        case PLF.Type.SumCase.TUPLE =>
          TTuple(
            ImmArray(lfType.getTuple.getFieldsList.asScala)
              .map(ft => (ft.getField, decodeType(ft.getType)))
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
      val name = eitherToParseError(DottedName.fromSegments(lfVal.getNameList().asScala))
      ValueRef(packageId, QualifiedName(module, name))
    }

    private[this] def decodeBuiltinFunction(lfBuiltin: PLF.BuiltinFunction): BuiltinFunction =
      lfBuiltin match {
        case PLF.BuiltinFunction.ADD_DECIMAL => BAddDecimal
        case PLF.BuiltinFunction.SUB_DECIMAL => BSubDecimal
        case PLF.BuiltinFunction.MUL_DECIMAL => BMulDecimal
        case PLF.BuiltinFunction.DIV_DECIMAL => BDivDecimal
        case PLF.BuiltinFunction.ROUND_DECIMAL =>
          BRoundDecimal
        case PLF.BuiltinFunction.ADD_INT64 => BAddInt64
        case PLF.BuiltinFunction.SUB_INT64 => BSubInt64
        case PLF.BuiltinFunction.MUL_INT64 => BMulInt64
        case PLF.BuiltinFunction.DIV_INT64 => BDivInt64
        case PLF.BuiltinFunction.MOD_INT64 => BModInt64
        case PLF.BuiltinFunction.EXP_INT64 => BExpInt64
        case PLF.BuiltinFunction.INT64_TO_DECIMAL =>
          BInt64ToDecimal
        case PLF.BuiltinFunction.DECIMAL_TO_INT64 =>
          BDecimalToInt64
        case PLF.BuiltinFunction.FOLDL => BFoldl
        case PLF.BuiltinFunction.FOLDR => BFoldr
        case PLF.BuiltinFunction.MAP_EMPTY => BMapEmpty
        case PLF.BuiltinFunction.MAP_INSERT => BMapInsert
        case PLF.BuiltinFunction.MAP_LOOKUP => BMapLookup
        case PLF.BuiltinFunction.MAP_DELETE => BMapDelete
        case PLF.BuiltinFunction.MAP_TO_LIST => BMapToList
        case PLF.BuiltinFunction.MAP_SIZE => BMapSize
        case PLF.BuiltinFunction.APPEND_TEXT => BAppendText
        case PLF.BuiltinFunction.ERROR => BError
        case PLF.BuiltinFunction.LEQ_INT64 =>
          BLessEqInt64
        case PLF.BuiltinFunction.LEQ_DECIMAL =>
          BLessEqDecimal
        case PLF.BuiltinFunction.LEQ_TEXT => BLessEqText
        case PLF.BuiltinFunction.LEQ_TIMESTAMP =>
          BLessEqTimestamp
        case PLF.BuiltinFunction.LEQ_PARTY =>
          BLessEqParty
        case PLF.BuiltinFunction.GEQ_INT64 =>
          BGreaterEqInt64
        case PLF.BuiltinFunction.GEQ_DECIMAL =>
          BGreaterEqDecimal
        case PLF.BuiltinFunction.GEQ_TEXT => BGreaterEqText
        case PLF.BuiltinFunction.GEQ_TIMESTAMP =>
          BGreaterEqTimestamp
        case PLF.BuiltinFunction.GEQ_PARTY =>
          BGreaterEqParty
        case PLF.BuiltinFunction.LESS_INT64 =>
          BLessInt64
        case PLF.BuiltinFunction.LESS_DECIMAL =>
          BLessDecimal
        case PLF.BuiltinFunction.LESS_TEXT => BLessText
        case PLF.BuiltinFunction.LESS_TIMESTAMP =>
          BLessTimestamp
        case PLF.BuiltinFunction.LESS_PARTY =>
          BLessParty
        case PLF.BuiltinFunction.GREATER_INT64 =>
          BGreaterInt64
        case PLF.BuiltinFunction.GREATER_DECIMAL =>
          BGreaterDecimal
        case PLF.BuiltinFunction.GREATER_TEXT =>
          BGreaterText
        case PLF.BuiltinFunction.GREATER_TIMESTAMP =>
          BGreaterTimestamp
        case PLF.BuiltinFunction.GREATER_PARTY =>
          BGreaterParty
        case PLF.BuiltinFunction.TO_TEXT_INT64 =>
          BToTextInt64
        case PLF.BuiltinFunction.TO_TEXT_DECIMAL =>
          BToTextDecimal
        case PLF.BuiltinFunction.TO_TEXT_TIMESTAMP =>
          BToTextTimestamp
        case PLF.BuiltinFunction.TO_TEXT_PARTY =>
          BToTextParty
        case PLF.BuiltinFunction.TO_TEXT_TEXT => BToTextText
        case PLF.BuiltinFunction.DATE_TO_UNIX_DAYS =>
          BDateToUnixDays
        case PLF.BuiltinFunction.EXPLODE_TEXT =>
          BExplodeText
        case PLF.BuiltinFunction.IMPLODE_TEXT =>
          BImplodeText
        case PLF.BuiltinFunction.GEQ_DATE => BGreaterEqDate
        case PLF.BuiltinFunction.LEQ_DATE => BLessEqDate
        case PLF.BuiltinFunction.LESS_DATE => BLessDate
        case PLF.BuiltinFunction.TIMESTAMP_TO_UNIX_MICROSECONDS =>
          BTimestampToUnixMicroseconds
        case PLF.BuiltinFunction.TO_TEXT_DATE => BToTextDate
        case PLF.BuiltinFunction.SHA256_TEXT => BSHA256Text
        case PLF.BuiltinFunction.UNIX_DAYS_TO_DATE =>
          BUnixDaysToDate
        case PLF.BuiltinFunction.UNIX_MICROSECONDS_TO_TIMESTAMP =>
          BUnixMicrosecondsToTimestamp
        case PLF.BuiltinFunction.GREATER_DATE =>
          BGreaterDate
        case PLF.BuiltinFunction.EQUAL_INT64 => BEqualInt64
        case PLF.BuiltinFunction.EQUAL_DECIMAL =>
          BEqualDecimal
        case PLF.BuiltinFunction.EQUAL_TEXT => BEqualText
        case PLF.BuiltinFunction.EQUAL_TIMESTAMP =>
          BEqualTimestamp
        case PLF.BuiltinFunction.EQUAL_DATE => BEqualDate
        case PLF.BuiltinFunction.EQUAL_PARTY => BEqualParty
        case PLF.BuiltinFunction.EQUAL_BOOL => BEqualBool
        case PLF.BuiltinFunction.EQUAL_LIST => BEqualList
        case PLF.BuiltinFunction.EQUAL_CONTRACT_ID =>
          BEqualContractId
        case PLF.BuiltinFunction.TRACE => BTrace
        case PLF.BuiltinFunction.TO_QUOTED_TEXT_PARTY => BToQuotedTextParty
        case PLF.BuiltinFunction.FROM_TEXT_PARTY => BFromTextParty
        case PLF.BuiltinFunction.UNRECOGNIZED =>
          throw ParseError("BuiltinFunction.UNRECOGNIZED")
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
        ImmArray(lfTyConApp.getArgsList.asScala).map(decodeType(_)))

    private[this] def decodeExpr(lfExpr: PLF.Expr): Expr =
      decodeLocation(lfExpr) match {
        case None => decodeExprBody(lfExpr)
        case Some(loc) => ELocation(loc, decodeExprBody(lfExpr))
      }

    private[this] def decodeExprBody(lfExpr: PLF.Expr): Expr =
      lfExpr.getSumCase match {
        case PLF.Expr.SumCase.VAR => EVar(lfExpr.getVar)
        case PLF.Expr.SumCase.VAL => EVal(decodeValName(lfExpr.getVal))
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
          EBuiltin(decodeBuiltinFunction(lfExpr.getBuiltin))
        case PLF.Expr.SumCase.REC_CON =>
          val recCon = lfExpr.getRecCon
          ERecCon(
            tycon = decodeTypeConApp(recCon.getTycon),
            fields = ImmArray(recCon.getFieldsList.asScala).map { field =>
              (field.getField, decodeExpr(field.getExpr))
            })

        case PLF.Expr.SumCase.REC_PROJ =>
          val recProj = lfExpr.getRecProj
          ERecProj(
            tycon = decodeTypeConApp(recProj.getTycon),
            field = recProj.getField,
            record = decodeExpr(recProj.getRecord))

        case PLF.Expr.SumCase.REC_UPD =>
          val recUpd = lfExpr.getRecUpd
          ERecUpd(
            tycon = decodeTypeConApp(recUpd.getTycon),
            field = recUpd.getField,
            record = decodeExpr(recUpd.getRecord),
            update = decodeExpr(recUpd.getUpdate))

        case PLF.Expr.SumCase.VARIANT_CON =>
          val varCon = lfExpr.getVariantCon
          EVariantCon(
            decodeTypeConApp(varCon.getTycon),
            varCon.getVariantCon,
            decodeExpr(varCon.getVariantArg))

        case PLF.Expr.SumCase.TUPLE_CON =>
          val tupleCon = lfExpr.getTupleCon
          ETupleCon(
            ImmArray(tupleCon.getFieldsList.asScala).map { field =>
              (field.getField, decodeExpr(field.getExpr))
            }
          )

        case PLF.Expr.SumCase.TUPLE_PROJ =>
          val tupleProj = lfExpr.getTupleProj
          ETupleProj(tupleProj.getField, decodeExpr(tupleProj.getTuple))

        case PLF.Expr.SumCase.TUPLE_UPD =>
          val tupleUpd = lfExpr.getTupleUpd
          ETupleUpd(
            field = tupleUpd.getField,
            tuple = decodeExpr(tupleUpd.getTuple),
            update = decodeExpr(tupleUpd.getUpdate))

        case PLF.Expr.SumCase.APP =>
          val lfApp = lfExpr.getApp
          val fun = decodeExpr(lfApp.getFun)
          val args = lfApp.getArgsList.asScala
          args.foldLeft(fun)((e, arg) => EApp(e, decodeExpr(arg)))
        case PLF.Expr.SumCase.ABS =>
          val lfAbs = lfExpr.getAbs
          val params = lfAbs.getParamList.asScala.map(decodeBinder)
          params.foldRight(decodeExpr(lfAbs.getBody))((param, e) =>
            EAbs((param._1.fold("")((v: String) => v), param._2), e, currentDefinitionRef))

        case PLF.Expr.SumCase.TY_APP =>
          val tyapp = lfExpr.getTyApp
          val args = tyapp.getTypesList.asScala
          val body = decodeExpr(tyapp.getExpr)
          args.foldLeft(body)((e, arg) => ETyApp(e, decodeType(arg)))
        case PLF.Expr.SumCase.TY_ABS =>
          val lfTyAbs = lfExpr.getTyAbs
          val params = lfTyAbs.getParamList.asScala.map(decodeTypeVarWithKind)
          params.foldRight(decodeExpr(lfTyAbs.getBody))((param, e) =>
            ETyAbs((param._1, param._2), e))

        case PLF.Expr.SumCase.LET =>
          val lfLet = lfExpr.getLet
          val body = decodeExpr(lfLet.getBody)
          lfLet.getBindingsList.asScala.foldRight(body)((binding, e) => {
            val v = binding.getBinder.getVar
            ELet(
              Binding(
                if (v == "") { None } else { Some(v) },
                decodeType(binding.getBinder.getType),
                decodeExpr(binding.getBound)),
              e)
          })

        case PLF.Expr.SumCase.NIL =>
          ENil(decodeType(lfExpr.getNil.getType))

        case PLF.Expr.SumCase.CONS =>
          val cons = lfExpr.getCons
          val typ = decodeType(cons.getType)
          ECons(
            typ,
            ImmArray(cons.getFrontList.asScala).map(decodeExpr(_)),
            decodeExpr(cons.getTail))

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
          ENone(decodeType(lfExpr.getNone.getType))

        case PLF.Expr.SumCase.SOME =>
          val some = lfExpr.getSome
          ESome(decodeType(some.getType), decodeExpr(some.getBody))

        case PLF.Expr.SumCase.SUM_NOT_SET =>
          throw ParseError("Expr.SUM_NOT_SET")
      }

    private[this] def decodeCaseAlt(lfCaseAlt: PLF.CaseAlt): CaseAlt = {
      val pat: CasePat = lfCaseAlt.getSumCase match {
        case PLF.CaseAlt.SumCase.DEFAULT => CPDefault
        case PLF.CaseAlt.SumCase.VARIANT =>
          val variant = lfCaseAlt.getVariant
          CPVariant(decodeTypeConName(variant.getCon), variant.getVariant, variant.getBinder)
        case PLF.CaseAlt.SumCase.PRIM_CON =>
          CPPrimCon(decodePrimCon(lfCaseAlt.getPrimCon))
        case PLF.CaseAlt.SumCase.NIL =>
          CPNil
        case PLF.CaseAlt.SumCase.CONS =>
          val cons = lfCaseAlt.getCons
          CPCons(cons.getVarHead, cons.getVarTail)
        case PLF.CaseAlt.SumCase.NONE =>
          CPNone
        case PLF.CaseAlt.SumCase.SOME =>
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

        case PLF.Update.SumCase.EMBED_EXPR =>
          val embedExpr = lfUpdate.getEmbedExpr
          UpdateEmbedExpr(decodeType(embedExpr.getType), decodeExpr(embedExpr.getBody))

        case PLF.Update.SumCase.FETCH_BY_KEY =>
          UpdateFetchByKey(decodeRetrieveByKey(lfUpdate.getFetchByKey))

        case PLF.Update.SumCase.LOOKUP_BY_KEY =>
          UpdateLookupByKey(decodeRetrieveByKey(lfUpdate.getLookupByKey))

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
        lfTypeVarWithKind: PLF.TypeVarWithKind): (TypeVarName, Kind) =
      (lfTypeVarWithKind.getVar, decodeKind(lfTypeVarWithKind.getKind))

    private[this] def decodeBinding(lfBinding: PLF.Binding): Binding = {
      val (binder, typ) = decodeBinder(lfBinding.getBinder)
      Binding(binder, typ, decodeExpr(lfBinding.getBound))
    }

    private[this] def decodeBinder(lfBinder: PLF.VarWithType): (Option[ExprVarName], Type) = {
      val v = lfBinder.getVar
      (if (v == "") { None } else { Some(v) }, decodeType(lfBinder.getType))
    }

    private[this] def decodePrimCon(lfPrimCon: PLF.PrimCon): PrimCon =
      lfPrimCon match {
        case PLF.PrimCon.CON_UNIT => PCUnit
        case PLF.PrimCon.CON_FALSE => PCFalse
        case PLF.PrimCon.CON_TRUE => PCTrue
        case _ => throw ParseError("Unknown PrimCon: " + lfPrimCon.toString)
      }

    private[this] def decodePrimLit(lfPrimLit: PLF.PrimLit): PrimLit =
      lfPrimLit.getSumCase match {
        case PLF.PrimLit.SumCase.INT64 =>
          PLInt64(lfPrimLit.getInt64)
        case PLF.PrimLit.SumCase.DECIMAL =>
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
}
