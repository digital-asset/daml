// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.lf.data

import java.io.{StringReader, StringWriter}

import com.google.common.io.{BaseEncoding, ByteStreams}
import scalaz.{Equal, Order}

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

sealed trait HexStringModule[T <: String] extends StringModule[T] {

  def encode(a: Bytes): T

  def decode(a: T): Bytes

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
sealed trait ConcatenableStringModule[T <: String, HS <: String] extends StringModule[T] {

  def fromLong(i: Long): T

  def fromInt(i: Int): T

  def concat(s: T, ss: T*): Either[String, T]

  def assertConcat(s: T, ss: T*): T

  def fromHexString(s: HS): T
}

sealed abstract class IdString {

  // We are very restrictive with regards to identifiers, taking inspiration
  // from the lexical structure of Java:
  // <https://docs.oracle.com/javase/specs/jls/se10/html/jls-3.html#jls-3.8>.
  //
  // In a language like C# you'll need to use some other unicode char for `$`.
  type Name <: String

  type HexString <: LedgerString

  // Human-readable package names and versions.
  type PackageName <: String
  type PackageVersion <: String

  /** Party identifiers are non-empty US-ASCII strings built from letters, digits, space, colon, minus and,
      underscore. We use them to represent [Party] literals. In this way, we avoid
      empty identifiers, escaping problems, and other similar pitfalls.
    */
  type Party <: String

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  type PackageId <: String

  type ParticipantId <: String

  /**
    * Used to reference to leger objects like legacy contractIds, ledgerIds,
    * transactionId, ... We use the same type for those ids, because we
    * construct some by concatenating the others.
    */
  type LedgerString <: String

  /** Identifiers for contracts */
  type ContractIdString <: String

  val HexString: HexStringModule[HexString]
  val Name: StringModule[Name]
  val PackageName: ConcatenableStringModule[PackageName, HexString]
  val PackageVersion: StringModule[PackageVersion]
  val Party: ConcatenableStringModule[Party, HexString]
  val PackageId: ConcatenableStringModule[PackageId, HexString]
  val ParticipantId: StringModule[ParticipantId]
  val LedgerString: ConcatenableStringModule[LedgerString, HexString]
  val ContractIdString: StringModule[ContractIdString]
}

object IdString {
  import Ref.{Name, Party}
  implicit def `Name order instance`: Order[Name] = Order fromScalaOrdering Name.ordering
  implicit def `Party order instance`: Order[Party] = Order fromScalaOrdering Party.ordering
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

private object HexStringModuleImpl extends StringModuleImpl with HexStringModule[String] {

  private val baseEncode = BaseEncoding.base16().lowerCase()

  override def fromString(str: String): Either[String, String] =
    Either.cond(
      baseEncode.canDecode(str),
      str,
      s"cannot parse HexString $str"
    )

  override def encode(a: Bytes): T = {
    val writer = new StringWriter()
    val os = baseEncode.encodingStream(writer)
    ByteStreams.copy(a.toInputStream, os)
    writer.toString
  }

  override def decode(a: T): Bytes =
    Bytes.fromInputStream(baseEncode.decodingStream(new StringReader(a)))
}

private final class MatchingStringModule(description: String, string_regex: String)
    extends StringModuleImpl {

  private val regex = string_regex.r
  private val pattern = regex.pattern

  override def fromString(s: String): Either[String, T] =
    Either.cond(
      pattern.matcher(s).matches(),
      s,
      s"""$description "$s" does not match regex "$regex"""")

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
    description: String,
    extraAllowedChars: Char => Boolean,
    maxLength: Int = Int.MaxValue,
) extends StringModuleImpl
    with ConcatenableStringModule[String, String] {

  override def fromString(s: String): Either[String, T] =
    if (s.isEmpty)
      Left(s"""$description is empty""")
    else if (s.length > maxLength)
      Left(s"""$description is too long""")
    else
      s.find(c => c > 0x7f || !(c.isLetterOrDigit || extraAllowedChars(c)))
        .fold[Either[String, T]](Right(s))(c =>
          Left(s"""non expected character 0x${c.toInt.toHexString} in $description "$s""""))

  override def fromLong(i: Long): T = i.toString

  override def fromInt(i: Int): T = fromLong(i.toLong)

  override def concat(s: T, ss: T*): Either[String, T] = {
    val b = StringBuilder.newBuilder
    b ++= s
    ss.foreach(b ++= _)
    if (b.length <= maxLength) Right(b.result()) else Left(s"id ${b.result()} too Long")
  }

  override def assertConcat(s: T, ss: T*): T =
    assertRight(concat(s, ss: _*))

  override def fromHexString(s: String): String = s
}

private[data] final class IdStringImpl extends IdString {

  override type HexString = String
  override val HexString: HexStringModule[HexString] = HexStringModuleImpl

  // We are very restrictive with regards to identifiers, taking inspiration
  // from the lexical structure of Java:
  // <https://docs.oracle.com/javase/specs/jls/se10/html/jls-3.html#jls-3.8>.
  //
  // In a language like C# you'll need to use some other unicode char for `$`.
  override type Name = String
  override val Name: StringModule[Name] =
    new MatchingStringModule("DAML LF Name", """[A-Za-z\$_][A-Za-z0-9\$_]*""")

  /** Package names are non-empty US-ASCII strings built from letters, digits, minus and underscore.
    */
  override type PackageName = String
  override val PackageName: ConcatenableStringModule[PackageName, HexString] =
    new ConcatenableMatchingStringModule("DAML LF Package Name", "-_".contains(_))

  /** Package versions are non-empty strings consisting of segments of digits (without leading zeros)
      separated by dots.
    */
  override type PackageVersion = String
  override val PackageVersion: StringModule[PackageVersion] =
    new MatchingStringModule("DAML LF Package Version", """(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*""")

  /** Party identifiers are non-empty US-ASCII strings built from letters, digits, space, colon, minus and,
    * underscore limited to 255 chars. We use them to represent [Party] literals. In this way, we avoid
    * empty identifiers, escaping problems, and other similar pitfalls.
    */
  override type Party = String
  override val Party: ConcatenableStringModule[Party, HexString] =
    new ConcatenableMatchingStringModule("DAML LF Party", ":-_ ".contains(_), 255)

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the DAML LF Archive. */
  override type PackageId = String
  override val PackageId: ConcatenableStringModule[PackageId, HexString] =
    new ConcatenableMatchingStringModule("DAML LF Package ID", "-_ ".contains(_))

  /**
    * Used to reference to leger objects like contractIds, ledgerIds,
    * transactionId, ... We use the same type for those ids, because we
    * construct some by concatenating the others.
    */
  // We allow space because the navigator's applicationId used it.
  override type LedgerString = String
  override val LedgerString: ConcatenableStringModule[LedgerString, HexString] =
    new ConcatenableMatchingStringModule("DAML LF Ledger String", "._:-#/ ".contains(_), 255)

  override type ParticipantId = String
  override val ParticipantId = LedgerString

  /**
    * Legacy contractIds.
    */
  override type ContractIdString = String
  override val ContractIdString: StringModule[ContractIdString] =
    new MatchingStringModule("DAML LF Contract ID", """#[\w._:\-#/ ]{0,254}""")

}
