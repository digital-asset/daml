// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.archive

import com.digitalasset.daml.lf.archive.DecodeV1
import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.LanguageMajorVersion.V1
import com.digitalasset.daml.lf.language.LanguageMinorVersion
import com.digitalasset.daml_lf.{DamlLf1 => PLF}

import scala.annotation.tailrec
import scala.language.implicitConversions

// Important: do not use this in production code. It is designed for testing only.
private[digitalasset] class EncodeV1(val minor: LanguageMinorVersion) {

  import EncodeV1._
  import LanguageMinorVersion.Implicits._
  import Name.ordering

  def encodePackage(pkgId: PackageId, pkg: Package): PLF.Package = {
    val moduleEncoder = new ModuleEncoder(pkgId)

    PLF.Package
      .newBuilder()
      .accumulateLeft(pkg.modules.sortByKey.values)(_ addModules moduleEncoder.encode(_))
      .build()
  }

  class ModuleEncoder(selfPkgId: PackageId) {

    def encode(module: Module): PLF.Module = {

      def addDefinition(
          builder: PLF.Module.Builder,
          nameWithDefinition: (DottedName, Definition)
      ): PLF.Module.Builder = {
        val (name, definition) = nameWithDefinition
        definition match {
          case dataType @ DDataType(_, _, cons) =>
            builder.addDataTypes(name -> dataType)
            cons match {
              case DataRecord(_, Some(template)) =>
                builder.addTemplates(name -> template)
              case _ =>
            }
          case value @ DValue(_, _, _, _) =>
            builder.addValues(name -> value)
        }
        builder
      }

      PLF.Module
        .newBuilder()
        .setName(module.name)
        .setFlags(
          PLF.FeatureFlags
            .newBuilder()
            .setForbidPartyLiterals(module.featureFlags.forbidPartyLiterals)
            .setDontDivulgeContractIdsInCreateArguments(true)
            .setDontDiscloseNonConsumingChoicesToObservers(true)
        )
        .accumulateLeft(module.definitions.sortByKey)(addDefinition)
        .build()
    }

    /** * Encode Reference ***/
    private val unit = PLF.Unit.newBuilder().build()

    private val selfPgkId = PLF.PackageRef.newBuilder().setSelf(unit).build()

    private implicit def encodePackageId(pkgId: PackageId): PLF.PackageRef =
      if (pkgId == this.selfPkgId)
        selfPgkId
      else
        PLF.PackageRef.newBuilder().setPackageId(pkgId).build()

    private implicit def encodeDottedName(name: DottedName): PLF.DottedName =
      PLF.DottedName.newBuilder().accumulateLeft(name.segments)(_ addSegments _).build()

    @inline
    private implicit def encodeModuleRef(modRef: (PackageId, ModuleName)): PLF.ModuleRef = {
      val (pkgId, modName) = modRef
      PLF.ModuleRef.newBuilder().setPackageRef(pkgId).setModuleName(modName).build()
    }

    private implicit def encodeTypeConName(identifier: Identifier): PLF.TypeConName =
      PLF.TypeConName
        .newBuilder()
        .setModule(identifier.moduleRef)
        .setName(identifier.name)
        .build()

    private implicit def encodeValName(identifier: Identifier): PLF.ValName =
      PLF.ValName
        .newBuilder()
        .setModule(identifier.moduleRef)
        .accumulateLeft(identifier.name.segments)(_ addName _)
        .build()

    /** * Encoding of Kinds ***/
    private val star =
      PLF.Kind.newBuilder().setStar(PLF.Unit.newBuilder()).build()

    private val KArrows = RightRecMatcher[Kind, Kind]({
      case KArrow(param, result) => (param, result)
    })

    private implicit def encodeKind(k: Kind): PLF.Kind =
      k match {
        case KArrows(params, result) =>
          expect(result == KStar)
          PLF.Kind
            .newBuilder()
            .setArrow(
              PLF.Kind.Arrow
                .newBuilder()
                .accumulateLeft(params)(_ addParams encodeKind(_))
                .setResult(star)
            )
            .build()
        case KStar =>
          star
      }

    /** * Encoding of types ***/
    private val builtinTypeMap =
      DecodeV1.primTypeTable.map {
        case (proto, (scala, sinceVersion)) => scala -> (proto -> sinceVersion)
      }

    private implicit def encodeBuiltinType(bType: BuiltinType): PLF.PrimType = {
      val (builtin, minVersion) = builtinTypeMap(bType)
      assertSince(minVersion, bType.toString)
      builtin
    }

    @inline
    private implicit def encodeTypeBinder(binder: (String, Kind)): PLF.TypeVarWithKind = {
      val (varName, kind) = binder
      PLF.TypeVarWithKind.newBuilder().setVar(varName).setKind(kind).build()
    }

    @inline
    private implicit def encodeFieldWithType(nameWithType: (String, Type)): PLF.FieldWithType = {
      val (name, typ) = nameWithType
      PLF.FieldWithType.newBuilder().setField(name).setType(typ).build()
    }

    private val TForalls = RightRecMatcher[(TypeVarName, Kind), Type]({
      case TForall(binder, body) => binder -> body
    })
    private val TApps = LeftRecMatcher[Type, Type]({
      case TApp(fun, arg) => fun -> arg
    })

    private implicit def encodeType(typ: Type): PLF.Type =
      encodeTypeBuilder(typ).build()

    private def encodeTypeBuilder(typ0: Type): PLF.Type.Builder = {
      val (typ, args) =
        typ0 match {
          case TApps(typ1, args1) => typ1 -> args1
          case _ => typ0 -> ImmArray.empty
        }
      val builder = PLF.Type.newBuilder()
      typ match {
        case TVar(varName) =>
          builder.setVar(
            PLF.Type.Var.newBuilder().setVar(varName).accumulateLeft(args)(_ addArgs _))
        case TTyCon(tycon) =>
          builder.setCon(
            PLF.Type.Con.newBuilder().setTycon(tycon).accumulateLeft(args)(_ addArgs _))
        case TBuiltin(bType) =>
          if (bType == BTArrow && V1.minorVersionOrdering.lteq(minor, "0")) {
            args match {
              case ImmArraySnoc(firsts, last) =>
                builder.setFun(
                  PLF.Type.Fun.newBuilder().accumulateLeft(firsts)(_ addParams _).setResult(last))
              case _ =>
                sys.error("unexpected errors")
            }
          } else {
            builder.setPrim(
              PLF.Type.Prim.newBuilder().setPrim(bType).accumulateLeft(args)(_ addArgs _))
          }
        case TApp(_, _) =>
          sys.error("unexpected error")
        case TForalls(binders, body) =>
          expect(args.isEmpty)
          builder.setForall(
            PLF.Type.Forall.newBuilder().accumulateLeft(binders)(_ addVars _).setBody(body))
        case TTuple(fields) =>
          expect(args.isEmpty)
          builder.setTuple(PLF.Type.Tuple.newBuilder().accumulateLeft(fields)(_ addFields _))
      }
    }

    /** * Encoding Expression ***/
    private val builtinFunctionMap =
      DecodeV1.builtinFunctionMap.map {
        case (proto, (scala, sinceVersion)) => scala -> (proto -> sinceVersion)
      }

    @inline
    private implicit def encodeBuiltins(builtinFunction: BuiltinFunction): PLF.BuiltinFunction =
      builtinFunctionMap(builtinFunction)._1

    private implicit def encodeTyConApp(tyCon: TypeConApp): PLF.Type.Con =
      PLF.Type.Con
        .newBuilder()
        .setTycon(tyCon.tycon)
        .accumulateLeft(tyCon.args)(_ addArgs _)
        .build()

    @inline
    private implicit def encodeFieldWithExpr(fieldWithExpr: (Name, Expr)): PLF.FieldWithExpr = {
      val (name, expr) = fieldWithExpr
      PLF.FieldWithExpr.newBuilder().setField(name).setExpr(expr).build()
    }

    @inline
    private implicit def encodeExprBinder(binder: (String, Type)): PLF.VarWithType = {
      val (varName, typ) = binder
      PLF.VarWithType.newBuilder().setVar(varName).setType(typ).build()
    }

    private implicit def encodeLocation(loc: Location): PLF.Location = {
      val Location(packageId, module, (startLine, startCol), (endLine, endCol)) = loc
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

    private implicit def encodeRetrieveByKey(rbk: RetrieveByKey): PLF.Update.RetrieveByKey =
      PLF.Update.RetrieveByKey.newBuilder().setTemplate(rbk.templateId).setKey(rbk.key).build()

    private implicit def encodeUpdate(upd0: Update): PLF.Update = {
      val builder = PLF.Update.newBuilder()
      upd0 match {
        case UpdatePure(typ, expr) =>
          builder.setPure(PLF.Pure.newBuilder().setType(typ).setExpr(expr))
        case UpdateBlock(binding, body) =>
          builder.setBlock(
            PLF.Block.newBuilder().accumulateLeft(binding)(_ addBindings _).setBody(body))
        case UpdateCreate(templateId, arg) =>
          builder.setCreate(PLF.Update.Create.newBuilder().setTemplate(templateId).setExpr(arg))
        case UpdateFetch(templateId, contractId) =>
          builder.setFetch(PLF.Update.Fetch.newBuilder().setTemplate(templateId).setCid(contractId))
        case UpdateExercise(templateId, choice, cid, actors, arg) =>
          if (actors.isEmpty) assertSince("5", "Update.Exercise.actors optional")
          builder.setExercise(
            PLF.Update.Exercise
              .newBuilder()
              .setTemplate(templateId)
              .setChoice(choice)
              .setCid(cid)
              .accumulateLeft(actors)(_ setActor _)
              .setArg(arg)
          )
        case UpdateGetTime =>
          builder.setGetTime(unit)
        case UpdateFetchByKey(rbk) =>
          assertSince("2", "fetchByKey")
          builder.setFetchByKey(rbk)
        case UpdateLookupByKey(rbk) =>
          assertSince("2", "lookupByKey")
          builder.setLookupByKey(rbk)
        case UpdateEmbedExpr(typ, body) =>
          builder.setEmbedExpr(PLF.Update.EmbedExpr.newBuilder().setType(typ).setBody(body))
      }
      builder.build()
    }

    private implicit def encodeScenario(s: Scenario): PLF.Scenario = {
      val builder = PLF.Scenario.newBuilder()
      s match {
        case ScenarioPure(typ, expr) =>
          builder.setPure(PLF.Pure.newBuilder().setType(typ).setExpr(expr))
        case ScenarioBlock(binding, body) =>
          builder.setBlock(
            PLF.Block.newBuilder().accumulateLeft(binding)(_ addBindings _).setBody(body))
        case ScenarioCommit(party, update, retType) =>
          builder.setCommit(
            PLF.Scenario.Commit.newBuilder().setParty(party).setExpr(update).setRetType(retType))
        case ScenarioMustFailAt(party, update, retType) =>
          builder.setMustFailAt(
            PLF.Scenario.Commit.newBuilder().setParty(party).setExpr(update).setRetType(retType))
        case ScenarioPass(relTime) =>
          builder.setPass(relTime)
        case ScenarioGetTime =>
          builder.setGetTime(unit)
        case ScenarioGetParty(name: Expr) =>
          builder.setGetParty(name)
        case ScenarioEmbedExpr(typ, body) =>
          builder.setEmbedExpr(PLF.Scenario.EmbedExpr.newBuilder().setType(typ).setBody(body))
      }
      builder.build()
    }

    private implicit def encodePrimCon(primCon: PrimCon): PLF.PrimCon =
      primCon match {
        case PCTrue => PLF.PrimCon.CON_TRUE
        case PCFalse => PLF.PrimCon.CON_FALSE
        case PCUnit => PLF.PrimCon.CON_UNIT
      }

    private implicit def encodePrimLit(primLit: PrimLit): PLF.PrimLit = {
      val builder = PLF.PrimLit.newBuilder()
      primLit match {
        case PLInt64(value) => builder.setInt64(value)
        case PLDecimal(value) => builder.setDecimal(Decimal.toString(value))
        case PLText(value) => builder.setText(value)
        case PLTimestamp(value) => builder.setTimestamp(value.micros)
        case PLParty(party) => builder.setParty(party)
        case PLDate(date) => builder.setDate(date.days)
      }
      builder.build()
    }

    private implicit def encodeCaseAlternative(alt: CaseAlt): PLF.CaseAlt = {
      val builder = PLF.CaseAlt.newBuilder().setBody(alt.expr)
      alt.pattern match {
        case CPVariant(tyCon, variant, binder) =>
          builder.setVariant(
            PLF.CaseAlt.Variant.newBuilder().setCon(tyCon).setVariant(variant).setBinder(binder))
        case CPEnum(tycon, con) =>
          assertSince("dev", "CaseAlt.Enum")
          builder.setEnum(PLF.CaseAlt.Enum.newBuilder().setCon(tycon).setConstructor(con))
        case CPPrimCon(primCon) =>
          builder.setPrimCon(primCon)
        case CPNil =>
          builder.setNil(unit)
        case CPCons(head, tail) =>
          builder.setCons(PLF.CaseAlt.Cons.newBuilder().setVarHead(head).setVarTail(tail))
        case CPNone =>
          assertSince("1", "CaseAlt.OptionalNone")
          builder.setOptionalNone(unit)
        case CPSome(x) =>
          assertSince("1", "CaseAlt.OptionalSome")
          builder.setOptionalSome(PLF.CaseAlt.OptionalSome.newBuilder().setVarBody(x))
        case CPDefault =>
          builder.setDefault(unit)
      }
      builder.build()
    }

    private val EApps = LeftRecMatcher[Expr, Expr]({
      case EApp(fun, arg) => fun -> arg
    })
    private val ETyApps = LeftRecMatcher[Expr, Type]({
      case ETyApp(exp, typ) => exp -> typ
    })
    private val EAbss = RightRecMatcher[(ExprVarName, Type), Expr]({
      case EAbs(binder, body, _) => binder -> body
    })
    private val ETyAbss = RightRecMatcher[(TypeVarName, Kind), Expr]({
      case ETyAbs(binder, body) => binder -> body
    })

    private def encodeExprBuilder(expr0: Expr): PLF.Expr.Builder = {
      def newBuilder = PLF.Expr.newBuilder()

      expr0 match {
        case EVar(value) =>
          newBuilder.setVar(value)
        case EVal(value) =>
          newBuilder.setVal(value)
        case EBuiltin(value) =>
          newBuilder.setBuiltin(value)
        case EPrimCon(primCon) =>
          newBuilder.setPrimCon(primCon)
        case EPrimLit(primLit) =>
          newBuilder.setPrimLit(primLit)
        case ERecCon(tyCon, fields) =>
          newBuilder.setRecCon(
            PLF.Expr.RecCon.newBuilder().setTycon(tyCon).accumulateLeft(fields)(_ addFields _))
        case ERecProj(tycon, field, expr) =>
          newBuilder.setRecProj(
            PLF.Expr.RecProj.newBuilder().setTycon(tycon).setField(field).setRecord(expr))
        case ERecUpd(tyCon, field, expr, update) =>
          newBuilder.setRecUpd(
            PLF.Expr.RecUpd
              .newBuilder()
              .setTycon(tyCon)
              .setField(field)
              .setRecord(expr)
              .setUpdate(update))
        case EVariantCon(tycon, variant, arg) =>
          newBuilder.setVariantCon(
            PLF.Expr.VariantCon
              .newBuilder()
              .setTycon(tycon)
              .setVariantCon(variant)
              .setVariantArg(arg))
        case EEnumCon(tyCon, con) =>
          assertSince("dev", "Expr.Enum")
          newBuilder.setEnumCon(PLF.Expr.EnumCon.newBuilder().setTycon(tyCon).setEnumCon(con))
        case ETupleCon(fields) =>
          newBuilder.setTupleCon(
            PLF.Expr.TupleCon.newBuilder().accumulateLeft(fields)(_ addFields _))
        case ETupleProj(field, expr) =>
          newBuilder.setTupleProj(PLF.Expr.TupleProj.newBuilder().setField(field).setTuple(expr))
        case ETupleUpd(field, tuple, update) =>
          newBuilder.setTupleUpd(
            PLF.Expr.TupleUpd.newBuilder().setField(field).setTuple(tuple).setUpdate(update))
        case EApps(fun, args) =>
          newBuilder.setApp(PLF.Expr.App.newBuilder().setFun(fun).accumulateLeft(args)(_ addArgs _))
        case ETyApps(expr, typs) =>
          newBuilder.setTyApp(
            PLF.Expr.TyApp.newBuilder().setExpr(expr).accumulateLeft(typs)(_ addTypes _))
        case EAbss(binders, body) =>
          newBuilder.setAbs(
            PLF.Expr.Abs.newBuilder().accumulateLeft(binders)(_ addParam _).setBody(body))
        case ETyAbss(binders, body) =>
          newBuilder.setTyAbs(
            PLF.Expr.TyAbs.newBuilder().accumulateLeft(binders)(_ addParam _).setBody(body))
        case ECase(scrut, alts) =>
          newBuilder.setCase(
            PLF.Case.newBuilder().setScrut(scrut).accumulateLeft(alts)(_ addAlts _))
        case ENil(typ) =>
          newBuilder.setNil(PLF.Expr.Nil.newBuilder().setType(typ))
        case ECons(typ, front, tail) =>
          newBuilder.setCons(
            PLF.Expr.Cons
              .newBuilder()
              .setType(typ)
              .accumulateLeft(front)(_ addFront _)
              .setTail(tail))
        case ENone(typ) =>
          assertSince("1", "Expr.OptionalNone")
          newBuilder.setOptionalNone(PLF.Expr.OptionalNone.newBuilder().setType(typ))
        case ESome(typ, x) =>
          assertSince("1", "Expr.OptionalSome")
          newBuilder.setOptionalSome(PLF.Expr.OptionalSome.newBuilder().setType(typ).setBody(x))
        case ELocation(loc, expr) =>
          encodeExprBuilder(expr).setLocation(loc)
        case EUpdate(u) =>
          newBuilder.setUpdate(u)
        case EScenario(s) =>
          newBuilder.setScenario(s)
      }
    }

    private implicit def encodeDataDef(nameWithDef: (DottedName, DDataType)): PLF.DefDataType = {
      val (name, dataType) = nameWithDef
      val builder =
        PLF.DefDataType
          .newBuilder()
          .setName(name)
          .accumulateLeft(dataType.params)(_ addParams _)
          .setSerializable(dataType.serializable)
      dataType.cons match {
        case DataRecord(fields, _) =>
          builder.setRecord(
            PLF.DefDataType.Fields.newBuilder().accumulateLeft(fields)(_ addFields _))
        case DataVariant(variants) =>
          builder.setVariant(
            PLF.DefDataType.Fields.newBuilder().accumulateLeft(variants)(_ addFields _))
        case DataEnum(constructors) =>
          builder.setEnum(
            PLF.DefDataType.EnumConstructors
              .newBuilder()
              .accumulateLeft(constructors)(_ addConstructors _))
      }
      builder.build()
    }

    private implicit def encodeValueDef(nameWithDef: (DottedName, DValue)): PLF.DefValue = {
      val (name, value) = nameWithDef
      PLF.DefValue
        .newBuilder()
        .setNameWithType(
          PLF.DefValue.NameWithType.newBuilder
            .accumulateLeft(name.segments)(_ addName _)
            .setType(value.typ))
        .setExpr(value.body)
        .setNoPartyLiterals(value.noPartyLiterals)
        .setIsTest(value.isTest)
        .build()
    }

    private implicit def encodeChoice(
        nameWithChoice: (ChoiceName, TemplateChoice)
    ): PLF.TemplateChoice = {
      val (name, choice) = nameWithChoice
      PLF.TemplateChoice
        .newBuilder()
        .setName(name)
        .setConsuming(choice.consuming)
        .setControllers(choice.controllers)
        .setArgBinder(choice.argBinder._1.getOrElse("") -> choice.argBinder._2)
        .setRetType(choice.returnType)
        .setUpdate(choice.update)
        .setSelfBinder(choice.selfBinder)
        .build()
    }

    private implicit def encodeTemplateKey(key: TemplateKey): PLF.DefTemplate.DefKey =
      PLF.DefTemplate.DefKey
        .newBuilder()
        .setType(key.typ)
        .setComplexKey(key.body)
        .setMaintainers(key.maintainers)
        .build()

    private implicit def encodeTemplate(
        nameWithTemplate: (DottedName, Template)): PLF.DefTemplate = {
      val (name, template) = nameWithTemplate
      PLF.DefTemplate
        .newBuilder()
        .setTycon(name)
        .setParam(template.param)
        .setPrecond(template.precond)
        .setSignatories(template.signatories)
        .setAgreement(template.agreementText)
        .accumulateLeft(template.choices.sortByKey)(_ addChoices _)
        .setObservers(template.observers)
        .accumulateLeft(template.key)(_ setKey _)
        .build()
    }

  }

  private def assertSince(minMinorVersion: LanguageMinorVersion, description: String): Unit =
    if (V1.minorVersionOrdering.lt(minor, minMinorVersion))
      throw EncodeError(s"$description is not supported by DAML-LF 1.$minor")

}

object EncodeV1 {

  case class EncodeError(message: String) extends RuntimeException

  private def unexpectedError(): Unit =
    throw EncodeError("unexpected error")

  private sealed abstract class LeftRecMatcher[Left, Right] {
    def unapply(arg: Left): Option[(Left, ImmArray[Right])]
  }

  private object LeftRecMatcher {
    def apply[Left, Right](
        split: PartialFunction[Left, (Left, Right)]
    ): LeftRecMatcher[Left, Right] = new LeftRecMatcher[Left, Right] {
      @tailrec
      private def go(
          x: Left,
          stack: FrontStack[Right] = FrontStack.empty
      ): Option[(Left, ImmArray[Right])] =
        if (split.isDefinedAt(x)) {
          val (left, right) = split(x)
          go(left, right +: stack)
        } else if (stack.nonEmpty) {
          Some(x -> stack.toImmArray)
        } else {
          None
        }

      @inline
      final def unapply(arg: Left): Option[(Left, ImmArray[Right])] = go(x = arg)
    }
  }

  private sealed abstract class RightRecMatcher[Left, Right] {
    def unapply(arg: Right): Option[(ImmArray[Left], Right)]
  }

  private object RightRecMatcher {
    def apply[Left, Right](
        split: PartialFunction[Right, (Left, Right)]
    ): RightRecMatcher[Left, Right] = new RightRecMatcher[Left, Right] {

      @tailrec
      private def go(
          stack: BackStack[Left] = BackStack.empty,
          x: Right
      ): Option[(ImmArray[Left], Right)] =
        if (split.isDefinedAt(x)) {
          val (left, right) = split(x)
          go(stack :+ left, right)
        } else if (stack.nonEmpty) {
          Some(stack.toImmArray -> x)
        } else {
          None
        }

      @inline
      final def unapply(arg: Right): Option[(ImmArray[Left], Right)] = go(x = arg)
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

  private def expect(b: Boolean): Unit =
    if (!b) unexpectedError()

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

}
