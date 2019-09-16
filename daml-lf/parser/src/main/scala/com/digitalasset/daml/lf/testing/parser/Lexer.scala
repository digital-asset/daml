// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.data
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.testing.parser.Token._

import scala.util.Try
import scala.util.parsing.combinator.RegexParsers

private[parser] object Lexer extends RegexParsers {

  protected override val whiteSpace = """(\s|//.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r

  def lex(s: String): List[Token] = parseAll(phrase(rep(token)), s) match {
    case Success(l, _) => l
    case f: NoSuccess => throw LexingError(f.msg)
  }

  val keywords: Map[String, Token] = Map(
    "Cons" -> `cons`,
    "Nil" -> `nil`,
    "Some" -> `some`,
    "None" -> `none`,
    "forall" -> `forall`,
    "let" -> `let`,
    "in" -> `in`,
    "with" -> `with`,
    "case" -> `case`,
    "of" -> `of`,
    "sbind" -> `sbind`,
    "ubind" -> `ubind`,
    "create" -> `create`,
    "fetch" -> `fetch`,
    "exercise" -> `exercise`,
    "exercise_with_actors" -> `exercise_with_actors`,
    "fetch_by_key" -> `fetch_by_key`,
    "lookup_by_key" -> `lookup_by_key`,
    "by" -> `by`,
    "to" -> `to`,
    "to_any_template" -> `to_any_template`,
    "from_any_template" -> `from_any_template`
  )

  val token: Parser[Token] =
    "->" ^^^ `->` |
      "<-" ^^^ `<-` |
      "@" ^^^ `@` |
      "\\" ^^^ `\\` |
      "/\\" ^^^ `/\\` |
      "." ^^^ `.` |
      ":" ^^^ `:` |
      "," ^^^ `,` |
      ";" ^^^ `;` |
      "(" ^^^ `(` |
      ")" ^^^ `)` |
      "<" ^^^ `<` |
      ">" ^^^ `>` |
      "{" ^^^ `{` |
      "}" ^^^ `}` |
      "[" ^^^ `[` |
      "]" ^^^ `]` |
      "*" ^^^ `*` |
      "=" ^^^ `=` |
      "_" ^^^ Token.`_` |
      "|" ^^^ `|` |
      """[a-zA-Z\$_][\w\$]*""".r ^^ (s => keywords.getOrElse(s, Id(s))) |
      """#\w+""".r ^^ ContractId |
      """\'([^\\\']|\\\'|\\\\)+\'""".r >> toSimpleString |
      """\"([^\\\"]|\\n|\\r|\\\"|\\\'|\\\\)*\"""".r >> toText |
      """\d+-\d+-\d+T\d+:\d+:\d+(\.\d+)?Z""".r >> toTimestamp |
      """\d{4}-\d{2}-\d{2}""".r >> toDate |
      """-?\d+\.\d*""".r >> toNumeric |
      """-?\d+""".r >> toNumber

  private def toTimestamp(s: String): Parser[Timestamp] =
    (in: Input) =>
      Time.Timestamp.fromString(s) match {
        case Right(x) => Success(Timestamp(x), in)
        case Left(_) =>
          Error(s"cannot interpret $s as a Timestamp", in)
    }

  private def toDate(s: String): Parser[Date] =
    (in: Input) =>
      Time.Date.fromString(s) match {
        case Right(x) => Success(Date(x), in)
        case Left(_) =>
          Error(s"cannot interpret $s as a Timestamp", in)
    }

  private def toNumeric(s: String): Parser[Numeric] =
    (in: Input) =>
      data.Numeric.fromString(s) match {
        case Right(x) => Success(Numeric(x), in)
        case Left(_) => Error(s"cannot interpret $s as a Decimal", in)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def toNumber(s: String): Parser[Number] =
    (in: Input) =>
      Try(Success(Number(s.toLong), in))
        .getOrElse(Error(s"cannot interpret $s as a Number", in))

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def toText(s: String): Parser[Text] =
    (in: Input) =>
      Try(Success(Text(StringContext.treatEscapes(s.drop(1).dropRight(1))), in))
        .getOrElse(Error(s"cannot interpret $s as a Text", in))

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def toSimpleString(s: String): Parser[SimpleString] =
    (in: Input) =>
      Try(Success(SimpleString(StringContext.treatEscapes(s.drop(1).dropRight(1))), in))
        .getOrElse(Error(s"cannot interpret $s as a SimpleText", in))

}
