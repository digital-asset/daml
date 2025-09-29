// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.parser

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.{LanguageVersion => LV, Util => AstUtil}
import com.digitalasset.daml.lf.testing.parser.Parsers._
import com.digitalasset.daml.lf.testing.parser.Token._
import com.daml.scalautil.Statement.discard

import scala.Ordering.Implicits.infixOrderingOps

private[parser] class ModParser[P](parameters: ParserParameters[P]) {

  import ModParser._

  private[parser] val exprParser: ExprParser[P] = new ExprParser(parameters)
  import exprParser.typeParser.{argTyp, fullIdentifier, typ, typeBinder}
  import exprParser.{expr, expr0}

  private def split(defs: Seq[Def]) = {
    val definitions = Seq.newBuilder[(Ref.DottedName, Definition)]
    val templates = Seq.newBuilder[(Ref.DottedName, Template)]
    val exceptions = Seq.newBuilder[(Ref.DottedName, DefException)]
    val interfaces = Seq.newBuilder[(Ref.DottedName, DefInterface)]
    defs.foreach[Unit] {
      case DataDef(name, defn) =>
        discard(definitions += name -> defn)
      case TemplDef(name, defn) =>
        discard(templates += name -> defn)
      case ExcepDef(name, defn) =>
        discard(exceptions += name -> defn)
      case IfaceDef(name, iface) =>
        discard(definitions += name -> DDataType.Interface)
        discard(interfaces += name -> iface)
    }
    (definitions.result(), templates.result(), exceptions.result(), interfaces.result())
  }

  lazy val pkg: Parser[Package] =
    metadata ~ rep(mod) ^^ { case metadata ~ modules =>
      Package.build(
        modules = modules,
        directDeps = modules.iterator
          .flatMap(AstUtil.collectIdentifiers)
          .map(_.packageId)
          .toSet - parameters.defaultPackageId,
        languageVersion = parameters.languageVersion,
        metadata = metadata,
        imports =
          if (parameters.languageVersion >= LV.Features.explicitPkgImports)
            Right(PackageIdCollector.collect(modules))
          else
            Left("LF too low, package created by com.digitalasset.daml.lf.testing.parser:ModParser"),
      )
    }

  private lazy val metadata: Parser[PackageMetadata] =
    Id("metadata") ~ `(` ~> pkgName ~ `:` ~ pkgVersion <~ `)` ^^ { case name ~ _ ~ version =>
      // TODO: https://github.com/digital-asset/daml/issues/16151
      // add support for upgradedPackageId
      PackageMetadata(name, version, None)
    }

  lazy val mod: Parser[Module] =
    Id("module") ~! dottedName ~ `{` ~ rep(definition <~ `;`) <~ `}` ^^ {
      case _ ~ modName ~ _ ~ defs =>
        val (definitions, templates, exceptions, interfaces) = split(defs)
        Module.build(modName, definitions, templates, exceptions, interfaces, FeatureFlags.default)
    }

  private lazy val definition: Parser[Def] =
    synDefinition | recDefinition | variantDefinition | enumDefinition | valDefinition | templateDefinition | exceptionDefinition | interfaceDefinition

  private def tags(allowed: Set[Ref.Name]): Parser[Set[Ref.Name]] = Parser { in =>
    val parser = rep(`@` ~> id) ^^ { tags =>
      tags.foreach { t =>
        if (!allowed(t))
          throw ParsingError(
            s"found tag $t but expected one of ${allowed.toList.mkString(",")}.",
            in.pos,
          )
      }
      tags.toSet
    }
    parser(in)
  }

  private lazy val binder: Parser[(Ref.Name, Type)] =
    id ~ `:` ~ typ ^^ { case id ~ _ ~ typ => id -> typ }

  private lazy val synDefinition: Parser[DataDef] =
    Id("synonym") ~>! dottedName ~ rep(typeBinder) ~
      (`=` ~> typ) ^^ { case id ~ params ~ typ =>
        DataDef(id, DTypeSyn(params.to(ImmArray), typ))
      }

  private lazy val recDefinition: Parser[DataDef] =
    Id("record") ~>! tags(dataDefTags) ~ dottedName ~ rep(typeBinder) ~
      (`=` ~ `{` ~> repsep(binder, `,`) <~ `}`) ^^ { case defTags ~ id ~ params ~ fields =>
        DataDef(
          id,
          DDataType(defTags(serializableTag), params.to(ImmArray), DataRecord(fields.to(ImmArray))),
        )
      }

  private lazy val variantDefinition: Parser[DataDef] =
    Id("variant") ~>! tags(dataDefTags) ~ dottedName ~ rep(typeBinder) ~
      (`=` ~> repsep(binder, `|`)) ^^ { case defTags ~ id ~ params ~ variants =>
        DataDef(
          id,
          DDataType(
            defTags(serializableTag),
            params.to(ImmArray),
            DataVariant(variants.to(ImmArray)),
          ),
        )
      }

  private lazy val enumDefinition: Parser[DataDef] =
    Id("enum") ~>! tags(dataDefTags) ~ dottedName ~ (`=` ~> repsep(id, `|`)) ^^ {
      case defTags ~ id ~ constructors =>
        DataDef(
          id,
          DDataType(defTags(serializableTag), ImmArray.Empty, DataEnum(constructors.to(ImmArray))),
        )
    }

  private lazy val valDefinition: Parser[DataDef] =
    Id("val") ~>! dottedName ~ `:` ~ typ ~ `=` ~ expr ^^ { case id ~ _ ~ typ ~ _ ~ expr =>
      DataDef(id, DValue(typ, expr))
    }

  private lazy val templateKey: Parser[TemplateKey] =
    argTyp ~ expr0 ~ expr0 ^^ { case t ~ body ~ maintainers =>
      TemplateKey(t, body, maintainers)
    }

  private lazy val method: Parser[InterfaceInstanceMethod] =
    Id("method") ~>! id ~ `=` ~ expr ^^ { case (name ~ _ ~ value) =>
      InterfaceInstanceMethod(name, value)
    }

  private lazy val interfaceInstanceBody: Parser[InterfaceInstanceBody] =
    `{` ~> (implementsView <~ `;`) ~ rep(method <~ `;`) <~ `}` ^^ { case view ~ methods =>
      InterfaceInstanceBody.build(
        methods,
        view,
      )
    }

  private lazy val implementsView: Parser[Expr] =
    Id("view") ~>! `=` ~>! expr

  private lazy val implements: Parser[TemplateImplements] =
    Id("implements") ~>! fullIdentifier ~ interfaceInstanceBody ^^ { case ifaceId ~ body =>
      TemplateImplements.build(
        ifaceId,
        body,
      )
    }

  private lazy val templateDefinition: Parser[TemplDef] =
    (Id("template") ~ `(` ~> id ~ `:` ~ dottedName ~ `)` ~ `=` ~ `{` ~
      (Id("precondition") ~> expr <~ `;`) ~
      (Id("signatories") ~> expr <~ `;`) ~
      (Id("observers") ~> expr <~ `;`) ~
      rep(templateChoice <~ `;`) ~
      rep(implements <~ `;`) ~
      opt(Id("key") ~> templateKey <~ `;`) <~
      `}`) ^^ {
      case x ~ _ ~ tycon ~ _ ~ _ ~ _ ~
          precon ~
          signatories ~
          observers ~
          choices ~
          implements ~
          key =>
        TemplDef(
          tycon,
          Template.build(
            param = x,
            precond = precon,
            signatories = signatories,
            choices = choices,
            observers = observers,
            key = key,
            implements = implements.reverse, // we want insertion order here.
          ),
        )
    }

  private lazy val exceptionDefinition: Parser[ExcepDef] =
    Id("exception") ~> dottedName ~ `=` ~ `{` ~ (Id("message") ~> expr) <~ `}` ^^ {
      case tycon ~ _ ~ _ ~ message => ExcepDef(tycon, DefException(message))
    }

  private lazy val choiceParam: Parser[(Ref.Name, Type)] =
    `(` ~> id ~ `:` ~ typ <~ `)` ^^ { case name ~ _ ~ typ => name -> typ }

  private lazy val selfBinder: Parser[Ref.Name] =
    `(` ~> id <~ `)`

  private lazy val templateChoice: Parser[TemplateChoice] =
    Id("choice") ~> tags(templateChoiceTags) ~ id ~ selfBinder ~ choiceParam ~
      (`:` ~> typ) ~
      (`,` ~> Id("controllers") ~> expr) ~
      opt(`,` ~> Id("observers") ~> expr) ~
      opt(`,` ~> Id("authorizers") ~> expr) ~
      (`to` ~> expr) ^^ {
        case choiceTags ~ name ~ self ~ param ~ retTyp ~ controllers ~ optObservers ~ optAuthorizers ~ update =>
          TemplateChoice(
            name,
            !choiceTags(nonConsumingTag),
            controllers,
            optObservers,
            optAuthorizers,
            self,
            param,
            retTyp,
            update,
          )
      }

  private val interfaceDefinition: Parser[IfaceDef] =
    Id("interface") ~ `(` ~> id ~ `:` ~ dottedName ~ `)` ~ `=` ~ `{` ~
      (interfaceView <~ `;`) ~
      rep(interfaceRequires <~ `;`) ~
      rep(interfaceMethod <~ `;`) ~
      rep(templateChoice <~ `;`) ~
      rep(coImplements <~ `;`) <~
      `}` ^^ {
        case x ~ _ ~ tycon ~ _ ~ _ ~ _ ~
            view ~
            requires ~
            methods ~
            choices ~
            coImplements =>
          IfaceDef(
            tycon,
            DefInterface.build(
              requires = Set.from(requires),
              param = x,
              choices = choices,
              methods = methods,
              view = view,
              coImplements = coImplements,
            ),
          )
      }
  private val interfaceView: Parser[Type] =
    Id("viewtype") ~>! typ

  private val interfaceRequires: Parser[Ref.TypeConId] =
    Id("requires") ~>! fullIdentifier

  private val interfaceMethod: Parser[InterfaceMethod] =
    Id("method") ~>! id ~ `:` ~ typ ^^ { case name ~ _ ~ typ =>
      InterfaceMethod(name, typ)
    }

  private lazy val coImplements: Parser[InterfaceCoImplements] =
    Id("coimplements") ~>! fullIdentifier ~ interfaceInstanceBody ^^ { case tplId ~ body =>
      InterfaceCoImplements.build(
        tplId,
        body,
      )
    }

  private val serializableTag = Ref.Name.assertFromString("serializable")
  private val nonConsumingTag = Ref.Name.assertFromString("nonConsuming")

  private val dataDefTags = Set(serializableTag)
  private val templateChoiceTags = Set(nonConsumingTag)

}

object ModParser {

  private sealed trait Def extends Product with Serializable

  private final case class DataDef(name: Ref.DottedName, defn: Definition) extends Def
  private final case class TemplDef(name: Ref.DottedName, defn: Template) extends Def
  private final case class ExcepDef(name: Ref.DottedName, defn: DefException) extends Def
  private final case class IfaceDef(name: Ref.DottedName, iface: DefInterface) extends Def

}

import com.digitalasset.daml.lf.data.Ref._
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.language.Ast._

object PackageIdCollector {

  /** Collects all unique PackageIds referenced within a given Daml-LF Package
    * represented by its modules.
    */
  def collect(modules: List[GenModule[Expr]]) = {
    modules.foldLeft(Set.empty[PackageId]) { (acc, module) =>
      acc ++ collectFromModule(module)
    }
  }

  // --- Helper functions for traversing the AST ---

  private def collectFromModule(module: Ast.Module): Set[PackageId] = {
    val definitionIds = module.definitions.values.flatMap(collectFromDefinition)
    val templateIds = module.templates.values.flatMap(collectFromTemplate)
    val exceptionIds = module.exceptions.values.flatMap(collectFromException)
    val interfaceIds = module.interfaces.values.flatMap(collectFromInterface)

    definitionIds.toSet ++ templateIds ++ exceptionIds ++ interfaceIds
  }

  private def collectFromDefinition(defn: Ast.Definition): Set[PackageId] = defn match {
    case DTypeSyn(_, typ) => collectFromType(typ)
    case DDataType(_, _, cons) => collectFromDataCons(cons)
    case DValue(typ, body) => collectFromType(typ) ++ collectFromExpr(body)
  }

  private def collectFromType(typ: Ast.Type): Set[PackageId] = typ match {
    case TVar(_) | TNat(_) | TBuiltin(_) => Set.empty
    case TTyCon(tycon) => Set(tycon.packageId)
    case TSynApp(tysyn, args) => Set(tysyn.packageId) ++ args.toSeq.flatMap(collectFromType)
    case TApp(tyfun, arg) => collectFromType(tyfun) ++ collectFromType(arg)
    case TForall((_, _), body) => collectFromType(body)
    case TStruct(fields) => fields.values.flatMap(collectFromType).toSet
  }

  private def collectFromExpr(expr: Ast.Expr): Set[PackageId] = expr match {
    // Atomics with no nested structures or IDs
    case EVar(_) | EBuiltinFun(_) | EBuiltinCon(_) | EBuiltinLit(_) => Set.empty

    // Atomics with IDs
    case EVal(value) => Set(value.packageId)
    case EEnumCon(tyCon, _) => Set(tyCon.packageId)

    // Expressions with Types
    case ENil(typ) => collectFromType(typ)
    case ENone(typ) => collectFromType(typ)
    case ESome(typ, body) => collectFromType(typ) ++ collectFromExpr(body)
    case EToAny(ty, body) => collectFromType(ty) ++ collectFromExpr(body)
    case EFromAny(ty, body) => collectFromType(ty) ++ collectFromExpr(body)
    case ETypeRep(typ) => collectFromType(typ)
    case EToAnyException(typ, value) => collectFromType(typ) ++ collectFromExpr(value)
    case EFromAnyException(typ, value) => collectFromType(typ) ++ collectFromExpr(value)
    case EThrow(retT, excT, exc) =>
      collectFromType(retT) ++ collectFromType(excT) ++ collectFromExpr(exc)

    // Expressions with TypeConApp or TypeConId
    case ERecCon(tycon, fields) =>
      collectFromTypeConApp(tycon) ++ fields.toSeq.flatMap(f => collectFromExpr(f._2))
    case ERecProj(tycon, _, record) => collectFromTypeConApp(tycon) ++ collectFromExpr(record)
    case ERecUpd(tycon, _, record, update) =>
      collectFromTypeConApp(tycon) ++ collectFromExpr(record) ++ collectFromExpr(update)
    case EVariantCon(tycon, _, arg) => collectFromTypeConApp(tycon) ++ collectFromExpr(arg)

    // Recursive Expressions
    case EStructCon(fields) => fields.toSeq.flatMap(f => collectFromExpr(f._2)).toSet
    case EStructProj(_, struct) => collectFromExpr(struct)
    case EStructUpd(_, struct, update) => collectFromExpr(struct) ++ collectFromExpr(update)
    case EApp(fun, arg) => collectFromExpr(fun) ++ collectFromExpr(arg)
    case ETyApp(e, typ) => collectFromExpr(e) ++ collectFromType(typ)
    case EAbs((_, typ), body) => collectFromType(typ) ++ collectFromExpr(body)
    case ETyAbs(_, body) => collectFromExpr(body)
    case ELet(binding, body) => collectFromBinding(binding) ++ collectFromExpr(body)
    case ECase(scrut, alts) => collectFromExpr(scrut) ++ alts.toSeq.flatMap(collectFromCaseAlt)
    case ECons(typ, front, tail) =>
      collectFromType(typ) ++ front.toSeq.flatMap(collectFromExpr) ++ collectFromExpr(tail)
    case ELocation(_, e) => collectFromExpr(e)
    case EUpdate(u) => collectFromUpdate(u)
    case EExperimental(_, typ) => collectFromType(typ)

    // Interface-related expressions
    case EToInterface(ifaceId, tmplId, value) =>
      Set(ifaceId.packageId, tmplId.packageId) ++ collectFromExpr(value)
    case EFromInterface(ifaceId, tmplId, value) =>
      Set(ifaceId.packageId, tmplId.packageId) ++ collectFromExpr(value)
    case EUnsafeFromInterface(ifaceId, tmplId, cid, iface) =>
      Set(ifaceId.packageId, tmplId.packageId) ++ collectFromExpr(cid) ++ collectFromExpr(iface)
    case EToRequiredInterface(reqIface, requiringIface, body) =>
      Set(reqIface.packageId, requiringIface.packageId) ++ collectFromExpr(body)
    case EFromRequiredInterface(reqIface, requiringIface, body) =>
      Set(reqIface.packageId, requiringIface.packageId) ++ collectFromExpr(body)
    case EUnsafeFromRequiredInterface(reqIface, requiringIface, cid, iface) =>
      Set(reqIface.packageId, requiringIface.packageId) ++ collectFromExpr(cid) ++ collectFromExpr(
        iface
      )
    case ECallInterface(ifaceId, _, value) => Set(ifaceId.packageId) ++ collectFromExpr(value)
    case ESignatoryInterface(ifaceId, body) => Set(ifaceId.packageId) ++ collectFromExpr(body)
    case EObserverInterface(ifaceId, body) => Set(ifaceId.packageId) ++ collectFromExpr(body)
    case EViewInterface(ifaceId, e) => Set(ifaceId.packageId) ++ collectFromExpr(e)
    case EInterfaceTemplateTypeRep(ifaceId, body) => Set(ifaceId.packageId) ++ collectFromExpr(body)
    case EChoiceController(tmplId, _, contract, choiceArg) =>
      Set(tmplId.packageId) ++ collectFromExpr(contract) ++ collectFromExpr(choiceArg)
    case EChoiceObserver(tmplId, _, contract, choiceArg) =>
      Set(tmplId.packageId) ++ collectFromExpr(contract) ++ collectFromExpr(choiceArg)
  }

  private def collectFromUpdate(update: Ast.Update): Set[PackageId] = update match {
    case UpdatePure(t, expr) => collectFromType(t) ++ collectFromExpr(expr)
    case UpdateBlock(bindings, body) =>
      bindings.toSeq.flatMap(collectFromBinding).toSet ++ collectFromExpr(body)
    case UpdateCreate(templateId, arg) => Set(templateId.packageId) ++ collectFromExpr(arg)
    case UpdateCreateInterface(interfaceId, arg) =>
      Set(interfaceId.packageId) ++ collectFromExpr(arg)
    case UpdateFetchTemplate(templateId, contractId) =>
      Set(templateId.packageId) ++ collectFromExpr(contractId)
    case UpdateFetchInterface(interfaceId, contractId) =>
      Set(interfaceId.packageId) ++ collectFromExpr(contractId)
    case UpdateExercise(templateId, _, cidE, argE) =>
      Set(templateId.packageId) ++ collectFromExpr(cidE) ++ collectFromExpr(argE)
    case UpdateExerciseInterface(ifaceId, _, cidE, argE, g) =>
      Set(ifaceId.packageId) ++ collectFromExpr(cidE) ++ collectFromExpr(argE) ++ g
        .map(collectFromExpr)
        .getOrElse(Set.empty)
    case UpdateExerciseByKey(templateId, _, keyE, argE) =>
      Set(templateId.packageId) ++ collectFromExpr(keyE) ++ collectFromExpr(argE)
    case UpdateGetTime => Set.empty
    case UpdateLedgerTimeLT(time) => collectFromExpr(time)
    case UpdateFetchByKey(templateId) => Set(templateId.packageId)
    case UpdateLookupByKey(templateId) => Set(templateId.packageId)
    case UpdateEmbedExpr(typ, body) => collectFromType(typ) ++ collectFromExpr(body)
    case UpdateTryCatch(typ, body, _, handler) =>
      collectFromType(typ) ++ collectFromExpr(body) ++ collectFromExpr(handler)
  }

  private def collectFromBinding(binding: Ast.Binding): Set[PackageId] =
    collectFromType(binding.typ) ++ collectFromExpr(binding.bound)

  private def collectFromCaseAlt(alt: Ast.CaseAlt): Set[PackageId] =
    collectFromCasePat(alt.pattern) ++ collectFromExpr(alt.expr)

  private def collectFromCasePat(pat: Ast.CasePat): Set[PackageId] = pat match {
    case CPVariant(tycon, _, _) => Set(tycon.packageId)
    case CPEnum(tycon, _) => Set(tycon.packageId)
    case CPNil | CPDefault | CPNone | CPSome(_) | CPCons(_, _) | CPBuiltinCon(_) => Set.empty
  }

  private def collectFromDataCons(cons: Ast.DataCons): Set[PackageId] = cons match {
    case DataRecord(fields) => fields.toSeq.flatMap(f => collectFromType(f._2)).toSet
    case DataVariant(variants) => variants.toSeq.flatMap(v => collectFromType(v._2)).toSet
    case DataEnum(_) | DataInterface => Set.empty
  }

  private def collectFromTypeConApp(tyconapp: Ast.TypeConApp): Set[PackageId] =
    Set(tyconapp.tycon.packageId) ++ tyconapp.args.toSeq.flatMap(collectFromType)

  private def collectFromTemplate(template: Ast.Template): Set[PackageId] = {
    val precond = collectFromExpr(template.precond)
    val signatories = collectFromExpr(template.signatories)
    val observers = collectFromExpr(template.observers)
    val choices = template.choices.values.flatMap(collectFromChoice).toSet
    val key = template.key.map(collectFromKey).getOrElse(Set.empty)
    val impls = template.implements.values.flatMap(collectFromImplements).toSet

    precond ++ signatories ++ observers ++ choices ++ key ++ impls
  }

  private def collectFromChoice(choice: Ast.TemplateChoice): Set[PackageId] = {
    val controllers = collectFromExpr(choice.controllers)
    val observers = choice.choiceObservers.map(collectFromExpr).getOrElse(Set.empty)
    val authorizers = choice.choiceAuthorizers.map(collectFromExpr).getOrElse(Set.empty)
    val argType = collectFromType(choice.argBinder._2)
    val retType = collectFromType(choice.returnType)
    val update = collectFromExpr(choice.update)

    controllers ++ observers ++ authorizers ++ argType ++ retType ++ update
  }

  private def collectFromKey(key: Ast.TemplateKey): Set[PackageId] = {
    collectFromType(key.typ) ++ collectFromExpr(key.body) ++ collectFromExpr(key.maintainers)
  }

  private def collectFromImplements(impl: Ast.TemplateImplements): Set[PackageId] = {
    Set(impl.interfaceId.packageId) ++ collectFromInterfaceInstanceBody(impl.body)
  }

  private def collectFromException(ex: Ast.DefException): Set[PackageId] = {
    collectFromExpr(ex.message)
  }

  private def collectFromInterface(iface: Ast.DefInterface): Set[PackageId] = {
    val requires = iface.requires.map(_.packageId)
    val choices = iface.choices.values.flatMap(collectFromChoice).toSet
    val methods = iface.methods.values.flatMap(m => collectFromType(m.returnType)).toSet
    val coimpls = iface.coImplements.values.flatMap(collectFromCoImplements).toSet
    val view = collectFromType(iface.view)

    requires ++ choices ++ methods ++ coimpls ++ view
  }

  private def collectFromCoImplements(coimpl: Ast.InterfaceCoImplements): Set[PackageId] = {
    Set(coimpl.templateId.packageId) ++ collectFromInterfaceInstanceBody(coimpl.body)
  }

  private def collectFromInterfaceInstanceBody(body: Ast.InterfaceInstanceBody): Set[PackageId] = {
    val methods = body.methods.values.map(m => collectFromExpr(m.value)).toSet.flatten
    val view = collectFromExpr(body.view)
    methods ++ view
  }
}
