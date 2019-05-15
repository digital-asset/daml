// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import scalaz.Equal

object Ref {

  /* Location annotation */
  case class Location(packageId: PackageId, module: ModuleName, start: (Int, Int), end: (Int, Int))

  // we do not use String.split because `":foo".split(":")`
  // results in `List("foo")` rather than `List("", "foo")`
  private def split(s: String, splitCh: Char): ImmArray[String] = {
    val splitCodepoint = splitCh.toInt
    val segments = ImmArray.newBuilder[String]
    val currentString = new java.lang.StringBuilder()
    s.codePoints()
      .forEach(ch => {
        if (ch == splitCodepoint) {
          segments += currentString.toString
          currentString.setLength(0)
        } else {
          val _ = currentString.appendCodePoint(ch)
        }
      })
    segments += currentString.toString
    segments.result()
  }

  // We are very restrictive with regards to identifiers, taking inspiration
  // from the lexical structure of Java:
  // <https://docs.oracle.com/javase/specs/jls/se10/html/jls-3.html#jls-3.8>.
  //
  // In a language like C# you'll need to use some other unicode char for `$`.
  val Name = MatchingStringModule("""[A-Za-z\$_][A-Za-z0-9\$_]*""")
  type Name = Name.T
  implicit def `Name equal instance`: Equal[Name] = Name.equalInstance

  final class DottedName private (val segments: ImmArray[Name]) extends Equals {
    def dottedName: String = segments.toSeq.mkString(".")

    override def equals(obj: Any): Boolean =
      obj match {
        case that: DottedName => segments == that.segments
        case _ => false
      }

    override def hashCode(): Int = segments.hashCode()

    def canEqual(that: Any): Boolean = that.isInstanceOf[DottedName]

    override def toString: String = dottedName
  }

  object DottedName {
    type T = DottedName

    def fromString(s: String): Either[String, DottedName] =
      if (s.isEmpty)
        Left(s"Expected a non-empty string")
      else
        fromSegments(split(s, '.').toSeq)

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): T =
      assert(fromString(s))

    def fromSegments(strings: Iterable[String]): Either[String, DottedName] = {
      val init: Either[String, BackStack[Name]] = Right(BackStack.empty)
      val validatedSegments = strings.foldLeft(init)((acc, string) =>
        for {
          stack <- acc
          segment <- Name.fromString(string)
        } yield stack :+ segment)
      for {
        segments <- validatedSegments
        name <- fromNames(segments.toImmArray)
      } yield name
    }

    @throws[IllegalArgumentException]
    def assertFromSegments(s: Iterable[String]): DottedName =
      assert(fromSegments(s))

    def fromNames(names: ImmArray[Name]): Either[String, DottedName] =
      Either.cond(names.nonEmpty, new DottedName(names), "No segments provided")

    @throws[IllegalArgumentException]
    def assertFromNames(names: ImmArray[Name]): DottedName =
      assert(fromNames(names))

    /** You better know what you're doing if you use this one -- specifically you need to comply
      * to the lexical specification embodied by `fromStrings`.
      */
    def unsafeFromNames(segments: ImmArray[Name]): DottedName = {
      new DottedName(segments)
    }
  }

  case class QualifiedName private (module: ModuleName, name: DottedName) {
    override def toString: String = module.toString + ":" + name.toString
    def qualifiedName: String = toString
  }
  object QualifiedName {
    type T = QualifiedName

    def fromString(s: String): Either[String, QualifiedName] = {
      val segments = split(s, ':')
      if (segments.length != 2)
        Left(s"Expecting two segments in $s, but got ${segments.length}")
      else
        ModuleName.fromString(segments(0)).flatMap { module =>
          DottedName.fromString(segments(1)).map { name =>
            QualifiedName(module, name)
          }
        }
    }

    @throws[IllegalArgumentException]
    final def assertFromString(s: String): T =
      assert(fromString(s))
  }

  /* A fully-qualified identifier pointing to a definition in the
   * specified package. */
  case class Identifier(packageId: PackageId, qualifiedName: QualifiedName)

  /* Choice name in a template. */
  type ChoiceName = Name

  type ModuleName = DottedName
  val ModuleName = DottedName

  private def isAsciiAlphaNum(c: Char) =
    'A' <= c && c <= 'Z' || 'a' <= c && c <= 'z' || '0' <= c && c <= '9'

  /** Party are non empty US-ASCII strings built with letters, digits, space, minus and,
      underscore. We use them to represent [PackageId]s and [Party] literals. In this way, we avoid
      empty identifiers, escaping problems, and other similar pitfalls.
    */
  val Party = ConcatenableMatchingStringModule(c => isAsciiAlphaNum(c) || "-_ ".contains(c))
  type Party = Party.T

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  val PackageId = ConcatenableMatchingStringModule(c => isAsciiAlphaNum(c) || "-_ ".contains(c))
  type PackageId = PackageId.T

  /** Reference to a value defined in the specified module. */
  type ValueRef = Identifier
  val ValueRef = Identifier

  /** Reference to a value defined in the specified module. */
  type DefinitionRef = Identifier
  val DefinitionRef = Identifier

  /** Reference to a type constructor. */
  type TypeConName = Identifier
  val TypeConName = Identifier

  /**
    * Used to reference to leger objects like contractIds, ledgerIds,
    * transactionId, ... We use the same type for those ids, because we
    * construct some by concatenating the others.
    */
  val LedgerName = ConcatenableMatchingStringModule(c => isAsciiAlphaNum(c) || "._:-#".contains(c))
  type LedgerName = LedgerName.T

  /** Identifier for a contractId */
  type ContractId = LedgerName

  /** Identifier for the ledger */
  type LedgerId = LedgerName

  /** Identifiers for transactions. */
  type TransactionId = LedgerName

}
