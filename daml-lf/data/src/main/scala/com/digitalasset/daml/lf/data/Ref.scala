// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

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
  val Name = MatchingStringModule("""[A-Za-z\$_][A-Za-z0-9\$_]*""".r)
  type Name = Name.T

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
    def fromString(s: String): Either[String, DottedName] =
      if (s.isEmpty)
        Left(s"Expected a non-empty string")
      else
        fromStrings(split(s, '.'))

    @throws[IllegalArgumentException]
    def assertFromString(s: String): DottedName =
      assert(fromString(s))

    def fromStrings(strings: ImmArray[String]): Either[String, DottedName] = {
      val init: Either[String, BackStack[Name]] = Right(BackStack.empty)
      val validatedSegments = strings.foldLeft(init)((acc, string) =>
        for {
          stack <- acc
          segment <- Name.fromString(string)
        } yield stack :+ segment)
      for {
        segments <- validatedSegments
        name <- fromSegments(segments.toImmArray)
      } yield name
    }

    @throws[IllegalArgumentException]
    def assertFromStrings(s: ImmArray[String]): DottedName =
      assert(fromStrings(s))

    def fromSegments(segments: ImmArray[Name]): Either[String, DottedName] =
      Either.cond(segments.nonEmpty, new DottedName(segments), "No segments provided")

    @throws[IllegalArgumentException]
    def assertFromSegment(segments: ImmArray[Name]): DottedName =
      assert(fromStrings(segments))

    /** You better know what you're doing if you use this one -- specifically you need to comply
      * to the lexical specification embodied by `fromStrings`.
      */
    def unsafeFromSegments(segments: ImmArray[Name]): DottedName = {
      new DottedName(segments)
    }
  }

  case class QualifiedName private (module: ModuleName, name: DottedName) {
    override def toString: String = module.toString + ":" + name.toString
    def qualifiedName: String = toString
  }
  object QualifiedName {
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
    def assertFromString(s: String): QualifiedName =
      assert(fromString(s))
  }

  /* A fully-qualified identifier pointing to a definition in the
   * specified package. */
  case class DefinitionRef(packageId: PackageId, qualifiedName: QualifiedName)

  /* Choice name in a template. */
  type ChoiceName = Name

  type ModuleName = DottedName
  val ModuleName = DottedName

  /** Party are non empty US-ASCII strings built with letters, digits, space, minus and,
      underscore. We use them to represent [PackageId]s and [Party] literals. In this way, we avoid
      empty identifiers, escaping problems, and other similar pitfalls.
    */
  val Party = MatchingStringModule("""[a-zA-Z0-9\-_ ]+""".r)
  type Party = Party.T

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  val PackageId = MatchingStringModule("""[a-zA-Z0-9\-_ ]+""".r)
  type PackageId = PackageId.T

  /** Reference to a value defined in the specified module. */
  type ValueRef = DefinitionRef

  /** Reference to a type constructor. */
  type TypeConName = DefinitionRef

  private def assert[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)

}
