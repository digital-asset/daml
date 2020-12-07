// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package testing.parser

import com.daml.lf.data.ImmArray
import com.daml.lf.data.Ref.{ChoiceName, DottedName, Name}
import com.daml.lf.language.Ast._
import com.daml.lf.testing.parser.Parsers._
import com.daml.lf.testing.parser.Token._

private[parser] class ModParser[P](parameters: ParserParameters[P]) {

  import ModParser._

  private[parser] val exprParser: ExprParser[P] = new ExprParser(parameters)
  import exprParser.typeParser.{argTyp, typ, typeBinder}
  import exprParser.{expr, expr0}

  private def split(defs: Seq[Def]) = {
    defs.foldLeft((Seq.empty[(DottedName, Definition)], Seq.empty[(DottedName, Template)])) {
      case ((definitions, templates), DataDef(name, defn)) =>
        ((name -> defn) +: definitions, templates)
      case ((definitions, templates), TemplDef(name, defn)) =>
        (definitions, (name -> defn) +: templates)
    }
  }

  lazy val pkg: Parser[Package] =
    opt(metadata) ~ rep(mod) ^^ {
      case metadata ~ modules => Package(modules, Set.empty, parameters.languageVersion, metadata)
    }

  private lazy val metadata: Parser[PackageMetadata] =
    Id("metadata") ~ `(` ~> pkgName ~ `:` ~ pkgVersion <~ `)` ^^ {
      case name ~ _ ~ version => PackageMetadata(name, version)
    }

  lazy val mod: Parser[Module] =
    Id("module") ~! tags(modTags) ~ dottedName ~ `{` ~ rep(definition <~ `;`) <~ `}` ^^ {
      case _ ~ modTag ~ modName ~ _ ~ defs =>
        val (definitions, templates) = split(defs)
        val flags = FeatureFlags(
          forbidPartyLiterals = modTag(noPartyLitsTag)
        )
        Module(modName, definitions, templates, flags)
    }

  private lazy val definition: Parser[Def] =
    synDefinition | recDefinition | variantDefinition | enumDefinition | valDefinition | templateDefinition

  private def tags(allowed: Set[Name]): Parser[Set[Name]] = Parser { in =>
    val parser = rep(`@` ~> id) ^^ { tags =>
      tags.foreach { t =>
        if (!allowed(t))
          throw ParsingError(
            s"found tag $t but expected one of ${allowed.toList.mkString(",")}.",
            in.pos)
      }
      tags.toSet
    }
    parser(in)
  }

  private lazy val binder: Parser[(Name, Type)] =
    id ~ `:` ~ typ ^^ { case id ~ _ ~ typ => id -> typ }

  private lazy val synDefinition: Parser[DataDef] =
    Id("synonym") ~>! dottedName ~ rep(typeBinder) ~
      (`=` ~> typ) ^^ {
      case id ~ params ~ typ =>
        DataDef(id, DTypeSyn(ImmArray(params), typ))
    }

  private lazy val recDefinition: Parser[DataDef] =
    Id("record") ~>! tags(dataDefTags) ~ dottedName ~ rep(typeBinder) ~
      (`=` ~ `{` ~> repsep(binder, `,`) <~ `}`) ^^ {
      case defTags ~ id ~ params ~ fields =>
        DataDef(
          id,
          DDataType(defTags(serializableTag), ImmArray(params), DataRecord(ImmArray(fields)))
        )
    }

  private lazy val variantDefinition: Parser[DataDef] =
    Id("variant") ~>! tags(dataDefTags) ~ dottedName ~ rep(typeBinder) ~
      (`=` ~> repsep(binder, `|`)) ^^ {
      case defTags ~ id ~ params ~ variants =>
        DataDef(
          id,
          DDataType(defTags(serializableTag), ImmArray(params), DataVariant(ImmArray(variants)))
        )
    }

  private lazy val enumDefinition: Parser[DataDef] =
    Id("enum") ~>! tags(dataDefTags) ~ dottedName ~ (`=` ~> repsep(id, `|`)) ^^ {
      case defTags ~ id ~ constructors =>
        DataDef(
          id,
          DDataType(defTags(serializableTag), ImmArray.empty, DataEnum(ImmArray(constructors)))
        )
    }

  private lazy val valDefinition: Parser[DataDef] =
    Id("val") ~>! tags(valDefTags) ~ dottedName ~ `:` ~ typ ~ `=` ~ expr ^^ {
      case defTags ~ id ~ _ ~ typ ~ _ ~ expr =>
        DataDef(id, DValue(typ, defTags(noPartyLitsTag), expr, defTags(isTestTag)))
    }

  private lazy val templateKey: Parser[TemplateKey] =
    argTyp ~ expr0 ~ expr0 ^^ {
      case t ~ body ~ maintainers => TemplateKey(t, body, maintainers)
    }

  private lazy val templateDefinition: Parser[TemplDef] =
    Id("template") ~ `(` ~> id ~ `:` ~ dottedName ~ `)` ~ `=` ~ `{` ~
      (Id("precondition") ~> expr) ~
      (`,` ~> Id("signatories") ~> expr) ~
      (`,` ~> Id("observers") ~> expr) ~
      (`,` ~> Id("agreement") ~> expr) ~
      (`,` ~> Id("choices") ~ `{` ~> repsep(templateChoice, `,`) <~ `}`) ~
      opt(`,` ~> Id("key") ~> templateKey) <~
      `}` ^^ {
      case x ~ _ ~ tycon ~ _ ~ _ ~ _ ~
            precon ~
            signatories ~
            observers ~
            agreement ~
            choices ~
            key =>
        TemplDef(tycon, Template(x, precon, signatories, agreement, choices, observers, key))
    }

  private lazy val choiceParam: Parser[(Name, Type)] =
    `(` ~> id ~ `:` ~ typ <~ `)` ^^ { case name ~ _ ~ typ => name -> typ }

  private lazy val selfBinder: Parser[Name] =
    `(` ~> id <~ `)`

  private lazy val templateChoice: Parser[(ChoiceName, TemplateChoice)] =
    Id("choice") ~> tags(templateChoiceTags) ~ id ~ selfBinder ~ choiceParam ~
      (`:` ~> typ) ~
      (`,` ~> Id("controllers") ~> expr) ~
      opt(`,` ~> Id("observers") ~> expr) ~
      (`to` ~> expr) ^^ {
      case choiceTags ~ name ~ self ~ param ~ retTyp ~ controllers ~ choiceObservers ~ update =>
        name -> TemplateChoice(
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

  private val serializableTag = Name.assertFromString("serializable")
  private val noPartyLitsTag = Name.assertFromString("noPartyLiterals")
  private val isTestTag = Name.assertFromString("isTest")
  private val nonConsumingTag = Name.assertFromString("nonConsuming")

  private val dataDefTags = Set(serializableTag)
  private val templateChoiceTags = Set(nonConsumingTag)
  private val valDefTags = Set(noPartyLitsTag, isTestTag)
  private val modTags = Set(noPartyLitsTag)

}

object ModParser {

  private sealed trait Def extends Product with Serializable

  private final case class DataDef(name: DottedName, defn: Definition) extends Def
  private final case class TemplDef(name: DottedName, defn: Template) extends Def

}
