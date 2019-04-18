// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

object Ref {

  // SimpleString are non empty US-ASCII strings built with letters, digits, space, minus and,
  // underscore. We use them to represent packageIds and party literals. In this way, we avoid
  // empty identifiers, escaping problems, and other similar pitfalls.
  final case class SimpleString private (underlyingString: String) extends Ordered[SimpleString] {
    def compare(that: SimpleString): Int =
      underlyingString.compareTo(that.underlyingString)
  }

  object SimpleString {

    /** Crashes if the string is not a valid [[SimpleString]]. We provide this for
      * backwards compat, but generally we prefer `assertX` methods for other
      * similar classes.
      */
    @deprecated(
      "use SimpleString.fromString, SimpleString.assertFromString, Party.fromString, or Party.assertFromString",
      since = "44.0.1")
    @throws[IllegalArgumentException]
    def apply(s: String): Party = assertFromString(s)

    private def valid(c: Char) =
      ('a' <= c && c <= 'z') ||
        ('A' <= c && c <= 'Z') ||
        ('0' <= c && c <= '9') ||
        c == ' ' || c == '-' || c == '_'

    def fromString(string: String): Either[String, SimpleString] =
      if (string.isEmpty)
        Left(s"Expected a non-empty string")
      else
        string.find(c => !valid(c)) match {
          case None =>
            Right(new SimpleString(string))
          case Some(c) =>
            Left(s"""Invalid character ${c.toInt.formatted("%#x")} found in "$string"""")
        }

    @throws[IllegalArgumentException]
    def assertFromString(s: String): SimpleString =
      assert(fromString(s))
  }

  type Party = SimpleString
  val Party = SimpleString

  /* Location annotation */
  case class Location(packageId: PackageId, module: ModuleName, start: (Int, Int), end: (Int, Int))

  /* Choice name in a template. */
  type ChoiceName = String

  type ModuleName = DottedName
  val ModuleName = DottedName

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

  case class DottedName private (segments: ImmArray[String]) {
    override def toString: String = segments.toSeq.mkString(".")
    def dottedName: String = toString
  }

  object DottedName {
    // We are very restrictive with regards to names, taking inspiration
    // from the lexical structure of Java:
    // <https://docs.oracle.com/javase/specs/jls/se10/html/jls-3.html#jls-3.8>.
    //
    // In a language like C# you'll need to use some other unicode char for `$`.
    private val asciiLetter: Set[Char] = Set('a' to 'z': _*) ++ Set('A' to 'Z': _*)
    private val asciiDigit: Set[Char] = Set('0' to '9': _*)
    private val allowedSymbols: Set[Char] = Set('_', '$')
    private val segmentStart: Set[Char] = asciiLetter ++ allowedSymbols
    private val segmentPart: Set[Char] = asciiLetter ++ asciiDigit ++ allowedSymbols

    def fromString(s: String): Either[String, DottedName] = {
      if (s.isEmpty)
        return Left(s"Expected a non-empty string")
      val segments = split(s, '.')
      fromSegments(segments.toSeq)
    }

    @throws[IllegalArgumentException]
    def assertFromString(s: String): DottedName =
      assert(fromString(s))

    def fromSegments(segments: Iterable[String]): Either[String, DottedName] = {
      if (segments.isEmpty) {
        return Left(s"No segments provided")
      }
      var validatedSegments = BackStack.empty[String]
      for (segment <- segments) {
        val segmentChars = segment.toArray
        if (segmentChars.length() == 0) {
          return Left(s"Empty dotted segment provided in segments ${segments.toList}")
        }
        val err = s"Dotted segment $segment contains invalid characters"
        if (!segmentStart.contains(segmentChars(0))) {
          return Left(err)
        }
        if (!segmentChars.tail.forall(segmentPart.contains)) {
          return Left(err)
        }
        validatedSegments = validatedSegments :+ segment
      }
      Right(DottedName(validatedSegments.toImmArray))
    }

    @throws[IllegalArgumentException]
    def assertFromSegments(segments: Iterable[String]): DottedName =
      assert(fromSegments(segments))

    /** You better know what you're doing if you use this one -- specifically you need to comply
      * to the lexical specification embodied by `fromSegments`.
      */
    def unsafeFromSegments(segments: ImmArray[String]): DottedName = {
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
      if (segments.length != 2) {
        return Left(s"Expecting two segments in $s, but got ${segments.length}")
      }
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
  case class Identifier(packageId: PackageId, qualifiedName: QualifiedName)

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  type PackageId = SimpleString
  val PackageId = SimpleString

  /** Reference to a value defined in the specified module. */
  type ValueRef = Identifier
  val ValueRef = Identifier

  /** Reference to a value defined in the specified module. */
  type DefinitionRef[PkgId] = Identifier
  val DefinitionRef = Identifier

  /** Reference to a type constructor. */
  type TypeConName = Identifier
  val TypeConName = Identifier

  private def assert[X](either: Either[String, X]): X =
    either.fold(e => throw new IllegalArgumentException(e), identity)

}
