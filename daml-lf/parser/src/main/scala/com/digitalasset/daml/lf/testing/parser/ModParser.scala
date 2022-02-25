// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package testing.parser

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.language.Ast._
import com.daml.lf.testing.parser.Parsers._
import com.daml.lf.testing.parser.Token._
import com.daml.scalautil.Statement.discard

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
    opt(metadata) ~ rep(mod) ^^ { case metadata ~ modules =>
      Package.build(modules, List.empty, parameters.languageVersion, metadata)
    }

  private lazy val metadata: Parser[PackageMetadata] =
    Id("metadata") ~ `(` ~> pkgName ~ `:` ~ pkgVersion <~ `)` ^^ { case name ~ _ ~ version =>
      PackageMetadata(name, version)
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
    Id("val") ~>! tags(valDefTags) ~ dottedName ~ `:` ~ typ ~ `=` ~ expr ^^ {
      case defTags ~ id ~ _ ~ typ ~ _ ~ expr =>
        DataDef(id, DValue(typ, expr, defTags(isTestTag)))
    }

  private lazy val templateKey: Parser[TemplateKey] =
    argTyp ~ expr0 ~ expr0 ^^ { case t ~ body ~ maintainers =>
      TemplateKey(t, body, maintainers)
    }

  private lazy val method: Parser[TemplateImplementsMethod] =
    Id("method") ~>! id ~ `=` ~ expr ^^ { case (name ~ _ ~ value) =>
      TemplateImplementsMethod(name, value)
    }

  private lazy val inheritedChoice: Parser[Ref.Name] =
    Id("choice") ~>! id

  private lazy val implements: Parser[TemplateImplements] =
    Id("implements") ~>! fullIdentifier ~ (`{` ~>
      rep(method <~ `;`) ~ rep(inheritedChoice <~ `;`)
      <~ `}`) ^^ { case ifaceId ~ (methods ~ inheritedChoices) =>
      TemplateImplements.build(ifaceId, methods, inheritedChoices)
    }

  private lazy val templateDefinition: Parser[TemplDef] =
    (Id("template") ~ `(` ~> id ~ `:` ~ dottedName ~ `)` ~ `=` ~ `{` ~
      (Id("precondition") ~> expr <~ `;`) ~
      (Id("signatories") ~> expr <~ `;`) ~
      (Id("observers") ~> expr <~ `;`) ~
      (Id("agreement") ~> expr <~ `;`) ~
      rep(templateChoice <~ `;`) ~
      rep(implements <~ `;`) ~
      opt(Id("key") ~> templateKey <~ `;`) <~
      `}`) ^^ {
      case x ~ _ ~ tycon ~ _ ~ _ ~ _ ~
          precon ~
          signatories ~
          observers ~
          agreement ~
          choices ~
          implements ~
          key =>
        TemplDef(
          tycon,
          Template.build(
            param = x,
            precond = precon,
            signatories = signatories,
            agreementText = agreement,
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
      (`to` ~> expr) ^^ {
        case choiceTags ~ name ~ self ~ param ~ retTyp ~ controllers ~ choiceObservers ~ update =>
          TemplateChoice(
            name,
            !choiceTags(nonConsumingTag),
            controllers,
            choiceObservers,
            self,
            param,
            retTyp,
            update,
          )
      }

  private val interfaceDefinition: Parser[IfaceDef] =
    Id("interface") ~ `(` ~> id ~ `:` ~ dottedName ~ `)` ~ `=` ~ `{` ~
      (Id("precondition") ~> expr <~ `;`) ~
      rep(interfaceMethod <~ `;`) ~
      rep(templateChoice <~ `;`) <~
      `}` ^^ {
        case x ~ _ ~ tycon ~ _ ~ _ ~ _ ~
            precond ~
            methods ~
            choices =>
          IfaceDef(
            tycon,
            DefInterface.build(Set.empty, x, choices, methods, precond),
          )
      }

  private val interfaceMethod: Parser[InterfaceMethod] =
    Id("method") ~>! id ~ `:` ~ typ ^^ { case name ~ _ ~ typ =>
      InterfaceMethod(name, typ)
    }

  private val serializableTag = Ref.Name.assertFromString("serializable")
  private val isTestTag = Ref.Name.assertFromString("isTest")
  private val nonConsumingTag = Ref.Name.assertFromString("nonConsuming")

  private val dataDefTags = Set(serializableTag)
  private val templateChoiceTags = Set(nonConsumingTag)
  private val valDefTags = Set(isTestTag)

}

object ModParser {

  private sealed trait Def extends Product with Serializable

  private final case class DataDef(name: Ref.DottedName, defn: Definition) extends Def
  private final case class TemplDef(name: Ref.DottedName, defn: Template) extends Def
  private final case class ExcepDef(name: Ref.DottedName, defn: DefException) extends Def
  private final case class IfaceDef(name: Ref.DottedName, iface: DefInterface) extends Def

}
