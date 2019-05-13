// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data.ImmArray
import com.digitalasset.daml.lf.lfpackage.Ast._
import com.digitalasset.daml.lf.lfpackage.Util._
import com.digitalasset.daml.lf.testing.parser.Parsers._
import com.digitalasset.daml.lf.testing.parser.Token._

object TypeParser {

  private def builtinTypes = Map[String, BuiltinType](
    "Int64" -> BTInt64,
    "Decimal" -> BTDecimal,
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
  )

  private[parser] lazy val typeBinder: Parser[(TypeVarName, Kind)] =
    `(` ~> id ~ `:` ~ KindParser.kind <~ `)` ^^ { case name ~ _ ~ k => name -> k } |
      id ^^ (_ -> KStar)

  private lazy val tForall: Parser[Type] =
    `forall` ~>! rep1(typeBinder) ~ `.` ~ typ ^^ { case bs ~ _ ~ t => (bs :\ t)(TForall) }

  private lazy val fieldType: Parser[(FieldName, Type)] =
    id ~ `:` ~ typ ^^ { case name ~ _ ~ t => name -> t }

  private lazy val tTuple: Parser[Type] =
    `<` ~>! rep1sep(fieldType, `,`) <~ `>` ^^ (fs => TTuple(ImmArray(fs)))

  lazy val typ0: Parser[Type] =
    `(` ~> typ <~ `)` |
      tForall |
      tTuple |
      (id ^? builtinTypes) ^^ TBuiltin |
      fullIdentifier ^^ TTyCon.apply |
      id ^^ TVar.apply

  private lazy val typ1: Parser[Type] = rep1(typ0) ^^ (_.reduceLeft(TApp))

  lazy val typ: Parser[Type] = rep1sep(typ1, `->`) ^^ (_.reduceRight(TFun))

  private[parser] lazy val argTyp: Parser[Type] = `@` ~> typ0

}
