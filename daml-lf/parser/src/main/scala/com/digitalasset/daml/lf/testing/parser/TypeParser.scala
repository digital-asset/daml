// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast._
import com.digitalasset.daml.lf.language.Util._
import com.digitalasset.daml.lf.testing.parser.Parsers._
import com.digitalasset.daml.lf.testing.parser.Token._

private[parser] class TypeParser[P](parameters: ParserParameters[P]) {

  private def builtinTypes = Map[String, BuiltinType](
    "Int64" -> BTInt64,
    "Numeric" -> BTNumeric,
    "Text" -> BTText,
    "Timestamp" -> BTTimestamp,
    "Party" -> BTParty,
    "Bool" -> BTBool,
    "Unit" -> BTUnit,
    "Option" -> BTOptional,
    "List" -> BTList,
    "Update" -> BTUpdate,
    "Scenario" -> BTScenario,
    "Date" -> BTDate,
    "ContractId" -> BTContractId,
    "Arrow" -> BTArrow,
    "Map" -> BTMap,
    "AnyTemplate" -> BTAnyTemplate,
  )

  private[parser] def fullIdentifier: Parser[Ref.Identifier] =
    opt(pkgId <~ `:`) ~ dottedName ~ `:` ~ dottedName ^^ {
      case pkgId ~ modName ~ _ ~ name =>
        Ref.Identifier(
          pkgId.getOrElse(parameters.defaultPackageId),
          Ref.QualifiedName(modName, name))
    }

  private[parser] lazy val typeBinder: Parser[(TypeVarName, Kind)] =
    `(` ~> id ~ `:` ~ KindParser.kind <~ `)` ^^ { case name ~ _ ~ k => name -> k } |
      id ^^ (_ -> KStar)

  private[parser] def tNat: Parser[TNat] =
    accept("Number", { case Number(l) if l.toInt == l => TNat(l.toInt) })

  private lazy val tForall: Parser[Type] =
    `forall` ~>! rep1(typeBinder) ~ `.` ~ typ ^^ { case bs ~ _ ~ t => (bs :\ t)(TForall) }

  private lazy val fieldType: Parser[(FieldName, Type)] =
    id ~ `:` ~ typ ^^ { case name ~ _ ~ t => name -> t }

  private lazy val tTuple: Parser[Type] =
    `<` ~>! rep1sep(fieldType, `,`) <~ `>` ^^ (fs => TTuple(ImmArray(fs)))

  lazy val typ0: Parser[Type] =
    `(` ~> typ <~ `)` |
      tNat |
      tForall |
      tTuple |
      (id ^? builtinTypes) ^^ TBuiltin |
      fullIdentifier ^^ TTyCon.apply |
      id ^^ TVar.apply

  private lazy val typ1: Parser[Type] = rep1(typ0) ^^ (_.reduceLeft(TApp))

  lazy val typ: Parser[Type] = rep1sep(typ1, `->`) ^^ (_.reduceRight(TFun))

  private[parser] lazy val argTyp: Parser[Type] = `@` ~> typ0

}
