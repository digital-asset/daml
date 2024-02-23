// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import com.daml.lf.data
import com.daml.lf.data.Time
import com.daml.lf.testing.parser.Token._

import scala.util.Try
import scala.util.matching.Regex
import scala.util.parsing.combinator.RegexParsers
import scala.util.parsing.input.Position

private[parser] object Lexer extends RegexParsers {

  protected override val whiteSpace: Regex = """(\s|//.*|(?m)/\*(\*(?!/)|[^*])*\*/)+""".r

  def lex(s: String): List[(Position, Token)] = {
    parseAll(phrase(rep(positionedToken)), s) match {
      case Success(l, _) => l
      case f: NoSuccess => throw LexingError(f.msg)
    }
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
    "to" -> `to`,
    "to_any" -> `to_any`,
    "from_any" -> `from_any`,
    "type_rep" -> `type_rep`,
    "loc" -> `loc`,
    "to_any_exception" -> `to_any_exception`,
    "from_any_exception" -> `from_any_exception`,
    "throw" -> `throw`,
    "catch" -> `catch`,
    "to_interface" -> `to_interface`,
    "to_required_interface" -> `to_required_interface`,
    "from_interface" -> `from_interface`,
    "from_required_interface" -> `from_required_interface`,
    "unsafe_from_interface" -> `unsafe_from_interface`,
    "unsafe_from_required_interface" -> `unsafe_from_required_interface`,
    "call_method" -> `call_method`,
    "interface_template_type_rep" -> `interface_template_type_rep`,
    "signatory_interface" -> `signatory_interface`,
    "observer_interface" -> `observer_interface`,
    "choice_controller" -> `choice_controller`,
    "choice_observer" -> `choice_observer`,
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
      "|" ^^^ `|` |
      """[a-zA-Z_\$][\w\$]*""".r ^^ (s => keywords.getOrElse(s, Id(s))) |
      """#\w+""".r ^^ ContractId |
      """\'([^\\\']|\\\'|\\\\)+\'""".r >> toSimpleString |
      """\"([^\\\"]|\\n|\\r|\\\"|\\\'|\\\\)*\"""".r >> toText |
      """\d+-\d+-\d+T\d+:\d+:\d+(\.\d+)?Z""".r >> toTimestamp |
      """\d{4}-\d{2}-\d{2}""".r >> toDate |
      """-?\d+\.\d*""".r >> toNumeric |
      """-?\d+""".r >> toNumber

  private val positionedToken: Parser[(Position, Token)] = Parser { in =>
    token(in).map(in.pos -> _)
  }

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
        case Left(_) => Error(s"cannot interpret $s as a Numeric", in)
      }

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def toNumber(s: String): Parser[Number] =
    (in: Input) =>
      Try(Success(Number(s.toLong), in))
        .getOrElse[ParseResult[Number]](Error(s"cannot interpret $s as a Number", in))

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def toText(s: String): Parser[Text] =
    (in: Input) =>
      Try(Success(Text(StringContext.processEscapes(s.drop(1).dropRight(1))), in))
        .getOrElse[ParseResult[Text]](Error(s"cannot interpret $s as a Text", in))

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  private def toSimpleString(s: String): Parser[SimpleString] =
    (in: Input) =>
      Try(Success(SimpleString(StringContext.processEscapes(s.drop(1).dropRight(1))), in))
        .getOrElse[ParseResult[SimpleString]](Error(s"cannot interpret $s as a SimpleText", in))

}
