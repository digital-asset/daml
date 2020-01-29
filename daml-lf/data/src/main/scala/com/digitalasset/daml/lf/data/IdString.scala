// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.digitalasset.daml.lf.data

import scalaz.Equal

sealed trait StringModule[T] {

  def fromString(s: String): Either[String, T]

  @throws[IllegalArgumentException]
  def assertFromString(s: String): T

  implicit def equalInstance: Equal[T]

  implicit def ordering: Ordering[T]

  // We provide the following array factory instead of a ClassTag
  // because the latter lets people easily reinterpret any string as a T.
  // See
  //  * https://github.com/digital-asset/daml/pull/983#discussion_r282513324
  //  * https://github.com/scala/bug/issues/9565
  val Array: ArrayFactory[T]

  def toStringMap[V](map: Map[T, V]): Map[String, V]
}

/** ConcatenableMatchingString are non empty US-ASCII strings built with letters, digits,
  * and some other (parameterizable) extra characters.
  * We use them to represent identifiers. In this way, we avoid
  * empty identifiers, escaping problems, and other similar pitfalls.
  *
  * ConcatenableMatchingString has the advantage over MatchingStringModule of being
  * concatenable and can be generated from numbers without extra checks.
  * Those properties are heavily use to generate some ids by combining other existing
  * ids.
  */
sealed trait ConcatenableStringModule[T <: String] extends StringModule[T] {

  def encode(a: Array[Byte]): T

  def fromLong(i: Long): T

  def fromInt(i: Int): T

  def concat(s: T, ss: T*): T
}

sealed trait UnionStringModule[T <: String, TV0 <: T, TV1 <: T] extends StringModule[T] {

  def toEither(s: T): Either[TV0, TV1]

  def toV0(s: T): Option[TV0]

  def assertToV0(s: T): TV0

  def toV1(s: T): Option[TV1]

  def assertToV1(s: T): TV1

}

sealed abstract class IdString {

  // We are very restrictive with regards to identifiers, taking inspiration
  // from the lexical structure of Java:
  // <https://docs.oracle.com/javase/specs/jls/se10/html/jls-3.html#jls-3.8>.
  //
  // In a language like C# you'll need to use some other unicode char for `$`.
  type Name <: String

  /** Party identifiers are non-empty US-ASCII strings built from letters, digits, space, colon, minus and,
      underscore. We use them to represent [Party] literals. In this way, we avoid
      empty identifiers, escaping problems, and other similar pitfalls.
    */
  type Party <: String

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  type PackageId <: String

  /**
    * Used to reference to leger objects like legacy contractIds, ledgerIds,
    * transactionId, ... We use the same type for those ids, because we
    * construct some by concatenating the others.
    */
  type LedgerString <: String

  /** Identifier for a contractId */
  type ContractIdString <: String

  /** Legacy Identifier for a contractId */
  type ContractIdStringV0 <: ContractIdString

  /**
    * New contractId ordered with relative contractIds.
    */
  type ContractIdStringV1 <: ContractIdString

  val Name: StringModule[Name]
  val Party: ConcatenableStringModule[Party]
  val PackageId: ConcatenableStringModule[PackageId]
  val LedgerString: ConcatenableStringModule[LedgerString]
  val ContractIdStringV0: StringModule[ContractIdStringV0]
  val ContractIdStringV1: StringModule[ContractIdStringV1]
  val ContractIdString: UnionStringModule[ContractIdString, ContractIdStringV0, ContractIdStringV1]

}

private sealed abstract class StringModuleImpl extends StringModule[String] {

  type T = String

  final implicit def equalInstance: Equal[T] = scalaz.std.string.stringInstance

  final val Array: ArrayFactory[T] = new ArrayFactory[T]

  def fromString(str: String): Either[String, String]

  final def assertFromString(s: String): String =
    assertRight(fromString(s))

  final implicit val ordering: Ordering[String] =
    _ compareTo _

  final def toStringMap[V](map: Map[String, V]): Map[String, V] =
    map
}

private final class MatchingStringModule(string_regex: String) extends StringModuleImpl {

  private val regex = string_regex.r
  private val pattern = regex.pattern

  override def fromString(s: String): Either[String, T] =
    Either.cond(pattern.matcher(s).matches(), s, s"""string "$s" does not match regex "$regex"""")

}

/** ConcatenableMatchingString are non empty US-ASCII strings built with letters, digits,
  * and some other (parameterizable) extra characters.
  * We use them to represent identifiers. In this way, we avoid
  * empty identifiers, escaping problems, and other similar pitfalls.
  *
  * ConcatenableMatchingString has the advantage over MatchingStringModule of being
  * concatenable and can be generated from numbers without extra checks.
  * Those properties are heavily use to generate some ids by combining other existing
  * ids.
  */
private final class ConcatenableMatchingStringModule(
    extraAllowedChars: Char => Boolean,
    maxLength: Int = Int.MaxValue
) extends StringModuleImpl
    with ConcatenableStringModule[String] {

  override def fromString(s: String): Either[String, T] =
    if (s.isEmpty)
      Left(s"""empty string""")
    else if (s.length > maxLength)
      Left(s"""string too long""")
    else
      s.find(c => c > 0x7f || !(c.isLetterOrDigit || extraAllowedChars(c)))
        .fold[Either[String, T]](Right(s))(c =>
          Left(s"""non expected character 0x${c.toInt.toHexString} in "$s""""))

  override def encode(a: Array[Byte]): String = a.map("%02x" format _).mkString

  override def fromLong(i: Long): T = i.toString

  override def fromInt(i: Int): T = fromLong(i.toLong)

  override def concat(s: T, ss: T*): T = {
    val b = StringBuilder.newBuilder
    b ++= s
    ss.foreach(b ++= _)
    b.result()
  }

}

private[data] final class IdStringImpl extends IdString {

  // We are very restrictive with regards to identifiers, taking inspiration
  // from the lexical structure of Java:
  // <https://docs.oracle.com/javase/specs/jls/se10/html/jls-3.html#jls-3.8>.
  //
  // In a language like C# you'll need to use some other unicode char for `$`.
  override type Name = String
  override val Name: StringModule[Name] =
    new MatchingStringModule("""[A-Za-z\$_][A-Za-z0-9\$_]*""")

  /** Party identifiers are non-empty US-ASCII strings built from letters, digits, space, colon, minus and,
    *underscore. We use them to represent [Party] literals. In this way, we avoid
    * empty identifiers, escaping problems, and other similar pitfalls.
    */
  override type Party = String
  override val Party: ConcatenableStringModule[Party] =
    new ConcatenableMatchingStringModule(":-_ ".contains(_))

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  override type PackageId = String
  override val PackageId: ConcatenableStringModule[PackageId] =
    new ConcatenableMatchingStringModule("-_ ".contains(_))

  /**
    * Used to reference to leger objects like contractIds, ledgerIds,
    * transactionId, ... We use the same type for those ids, because we
    * construct some by concatenating the others.
    */
  // We allow space because the navigator's applicationId used it.
  override type LedgerString = String
  override val LedgerString: ConcatenableStringModule[LedgerString] =
    new ConcatenableMatchingStringModule("._:-#/ ".contains(_), 255)

  /**
    * Legacy contractIds.
    */
  override type ContractIdStringV0 = LedgerString
  override val ContractIdStringV0: StringModule[LedgerString] = LedgerString

  /**
    * New contractId ordered with relative contractIds.
    */
  // Prefixed with "$0" which is not a valid substring of ContractIdV0.
  override type ContractIdStringV1 = String
  override val ContractIdStringV1: StringModule[ContractIdStringV1] =
    new MatchingStringModule("""\$0[0-9a-f]{64}[A-Za-z0-9:\-_]{189}""")

  /** Identifier for a contractIs, union of `ContractIdStringV0` and `ContractIdStringV1` */
  override type ContractIdString = String
  override val ContractIdString
    : UnionStringModule[ContractIdString, ContractIdStringV0, ContractIdStringV1] =
    new StringModuleImpl
    with UnionStringModule[ContractIdString, ContractIdStringV0, ContractIdStringV1] {

      override def toEither(s: ContractIdString): Either[String, String] =
        Either.cond(s.startsWith("$0"), s, s)

      override def fromString(s: String): Either[String, ContractIdString] =
        toEither(s).fold(ContractIdStringV0.fromString, ContractIdStringV1.fromString)

      override def assertToV0(s: ContractIdString): ContractIdStringV0 =
        toEither(s).left
          .getOrElse(throw new IllegalArgumentException("expect V0 ContractId get V1"))

      override def assertToV1(s: ContractIdString): ContractIdStringV1 =
        toEither(s).right
          .getOrElse(throw new IllegalArgumentException("expect V1 ContractId get V0"))

      override def toV0(s: ContractIdString): Option[ContractIdStringV0] =
        toEither(s).left.toOption

      override def toV1(s: ContractIdString): Option[ContractIdStringV1] =
        toEither(s).right.toOption
    }
}
