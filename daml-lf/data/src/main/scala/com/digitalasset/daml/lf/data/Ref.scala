// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package data

import scalaz.Equal

object Ref {

  val IdString: IdString = new IdStringImpl

  type Name = IdString.Name
  val Name: IdString.Name.type = IdString.Name
  implicit def `Name equal instance`: Equal[Name] = Name.equalInstance

  /** Party identifiers are non-empty US-ASCII strings built from letters, digits, space, colon, minus and,
      underscore. We use them to represent [Party] literals. In this way, we avoid
      empty identifiers, escaping problems, and other similar pitfalls.
    */
  type Party = IdString.Party
  val Party: IdString.Party.type = IdString.Party

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  type PackageId = IdString.PackageId
  val PackageId: IdString.PackageId.type = IdString.PackageId

  /** Identifiers for a contractIds */
  type ContractIdString = IdString.ContractIdString
  val ContractIdString: IdString.ContractIdString.type = IdString.ContractIdString

  type LedgerString = IdString.LedgerString
  val LedgerString: IdString.LedgerString.type = IdString.LedgerString

  type ParticipantId = IdString.ParticipantId
  val ParticipantId: IdString.ParticipantId.type = IdString.ParticipantId

  /* Location annotation */
  case class Location(
      packageId: PackageId,
      module: ModuleName,
      definition: String,
      start: (Int, Int),
      end: (Int, Int),
  )

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

  final class DottedName private (val segments: ImmArray[Name])
      extends Equals
      with Ordered[DottedName] {
    def dottedName: String = segments.toSeq.mkString(".")

    override def equals(obj: Any): Boolean =
      obj match {
        case that: DottedName => segments == that.segments
        case _ => false
      }

    override def hashCode(): Int = segments.hashCode()

    def canEqual(that: Any): Boolean = that.isInstanceOf[DottedName]

    override def toString: String = dottedName

    override def compare(that: DottedName): Int = {
      import scala.math.Ordering.Implicits._
      import Name.ordering

      implicitly[Ordering[Seq[Name]]].compare(segments.toSeq, that.segments.toSeq)
    }

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
      assertRight(fromString(s))

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
      assertRight(fromSegments(s))

    def fromNames(names: ImmArray[Name]): Either[String, DottedName] =
      Either.cond(names.nonEmpty, new DottedName(names), "No segments provided")

    @throws[IllegalArgumentException]
    def assertFromNames(names: ImmArray[Name]): DottedName =
      assertRight(fromNames(names))

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
      assertRight(fromString(s))
  }

  /* A fully-qualified identifier pointing to a definition in the
   * specified package. */
  case class Identifier(packageId: PackageId, qualifiedName: QualifiedName)

  /* Choice name in a template. */
  type ChoiceName = Name
  val ChoiceName = Name

  type ModuleName = DottedName
  val ModuleName = DottedName

  /** Reference to a value defined in the specified module. */
  type ValueRef = Identifier
  val ValueRef = Identifier

  /** Reference to a value defined in the specified module. */
  type DefinitionRef = Identifier
  val DefinitionRef = Identifier

  /** Reference to a type constructor. */
  type TypeConName = Identifier
  val TypeConName = Identifier

  /** Reference to a type synonym. */
  type TypeSynName = Identifier
  val TypeSynName = Identifier

}
