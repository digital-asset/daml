// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.testing.parser

import com.daml.lf.data.Ref
import com.daml.lf.testing.parser.Token._

import scala.util.parsing.input.{NoPosition, Position}

private[parser] object Parsers extends scala.util.parsing.combinator.Parsers {

  type Elem = Token

  case class Reader[A](l: Seq[(Position, A)]) extends util.parsing.input.Reader[A] {
    override def first: A = l.head._2

    override def rest: Reader[A] = Reader(l.tail)

    override def pos: Position = l.headOption.map(_._1).getOrElse(NoPosition)

    override def atEnd: Boolean = l.isEmpty

    override def toString: String = l.mkString(" ")
  }

  val id: Parser[Ref.Name] = accept(
    "Identifier",
    Function unlift {
      case Id(s) => Ref.Name.fromString(s).toOption
      case _ => None
    },
  )
  val text: Parser[String] = accept("Text", { case Text(s) => s })
  val pkgId: Parser[Ref.PackageId] = accept(
    "PackageId",
    Function unlift {
      case SimpleString(s) => Ref.PackageId.fromString(s).toOption
      case _ => None
    },
  )

  val pkgName: Parser[Ref.PackageName] = accept(
    "PackageName",
    Function unlift {
      case SimpleString(s) => Ref.PackageName.fromString(s).toOption
      case _ => None
    },
  )

  val pkgVersion: Parser[Ref.PackageVersion] = accept(
    "PackageVersion",
    Function unlift {
      case SimpleString(s) => Ref.PackageVersion.fromString(s).toOption
      case _ => None
    },
  )

  val dottedName: Parser[Ref.DottedName] =
    rep1sep(id, `.`) ^^ (s => Ref.DottedName.assertFromSegments(s))

  def parseAll[A](p: Parser[A], s: String): A =
    phrase(p)(Reader(Lexer.lex(s))) match {
      case Success(l, _) => l
      case e: NoSuccess => throw ParsingError(e.msg, e.next.pos)
    }

  /* backport ~>! and <~! from parser combinator 1.1.x */
  implicit class ParserOps[T](val parser: Parser[T]) extends AnyVal {

    def ~>![U](q: => Parser[U]): Parser[U] = {
      lazy val p = q // lazy argument
      OnceParser {
        (for (_ <- parser; b <- commit(p)) yield b).named("~>!")
      }
    }

    def <~![U](q: => Parser[U]): Parser[T] = {
      lazy val p = q // lazy argument
      OnceParser {
        (for (a <- parser; _ <- commit(p)) yield a).named("<~!")
      }
    }
  }

  implicit class TokenOps[T](val token: Token) extends AnyVal {
    def ~>![U](q: => Parser[U]): Parser[U] = elem(token) ~>! q
  }

}
