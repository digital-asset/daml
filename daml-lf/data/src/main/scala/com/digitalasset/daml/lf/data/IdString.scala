// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
package com.daml.lf.data

import com.daml.scalautil.Statement.discard

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
    *      underscore. We use them to represent [Party] literals. In this way, we avoid
    *      empty identifiers, escaping problems, and other similar pitfalls.
    */
  type Party <: String

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the Daml-LF Archive.
    */
  type PackageId <: String

  type ParticipantId <: String

  /** Used to reference to ledger objects like legacy contractIds, ledgerIds,
    * transactionId, ... We use the same type for those ids, because we
    * construct some by concatenating the others.
    */
  type LedgerString <: String

  /** Identifiers for contracts */
  type ContractIdString <: String

  /** Identifiers for applications submitting requests to the Ledger API.
    * See the comments on the actual definitions below for details on application and
    * user ids and their relation.
    */
  type ApplicationId <: String

  /** Identifiers for participant node users */
  type UserId <: String

  val HexString: HexStringModule[HexString]
  val Name: StringModule[Name]
  val PackageName: ConcatenableStringModule[PackageName, HexString]
  val PackageVersion: StringModule[PackageVersion]
  val Party: ConcatenableStringModule[Party, HexString]
  val PackageId: ConcatenableStringModule[PackageId, HexString]
  val ParticipantId: StringModule[ParticipantId]
  val LedgerString: ConcatenableStringModule[LedgerString, HexString]
  val ContractIdString: StringModule[ContractIdString]
  val ApplicationId: StringModule[ApplicationId]
  val UserId: StringModule[UserId]
}

object IdString {
  import Ref.{Name, Party}
  implicit def `Name order instance`: Order[Name] = Order fromScalaOrdering Name.ordering
  implicit def `Party order instance`: Order[Party] = Order fromScalaOrdering Party.ordering

  private[data] def asciiCharsToRejectionArray(s: Iterable[Char]): Array[Boolean] = {
    val array = Array.fill(0x80)(true)
    s.foreach { c =>
      val i = c.toInt
      assert(i < 0x80)
      array(i) = false
    }
    array
  }

  private[data] val letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  private[data] val digits = "0123456789"
}

private sealed abstract class StringModuleImpl extends StringModule[String] {

  type T = String

  final implicit def equalInstance: Equal[T] = scalaz.std.string.stringInstance

  final val Array: ArrayFactory[T] = new ArrayFactory[T]

  def fromString(str: String): Either[String, String] =
    try {
      Right(assertFromString(str))
    } catch {
      case err: IllegalArgumentException =>
        Left(err.getMessage)
    }

  final implicit val ordering: Ordering[String] =
    _ compareTo _

  final def toStringMap[V](map: Map[String, V]): Map[String, V] =
    map
}

private object HexStringModuleImpl extends StringModuleImpl with HexStringModule[String] {

  private val baseEncode = BaseEncoding.base16().lowerCase()

  override def assertFromString(str: String): String =
    if (baseEncode.canDecode(str))
      str
    else
      throw new IllegalArgumentException(s"cannot parse HexString $str")

  override def encode(a: Bytes): T = {
    val writer = new StringWriter()
    val os = baseEncode.encodingStream(writer)
    discard[Long](ByteStreams.copy(a.toInputStream, os))
    writer.toString
  }

  override def decode(a: T): Bytes =
    Bytes.fromInputStream(baseEncode.decodingStream(new StringReader(a)))

}

private final class MatchingStringModule(description: String, string_regex: String)
    extends StringModuleImpl {

  private val regex = string_regex.r
  private val pattern = regex.pattern

  @throws[IllegalArgumentException]
  override def assertFromString(str: String): String =
    if (pattern.matcher(str).matches())
      str
    else
      throw new IllegalArgumentException(s"""$description "$str" does not match regex "$regex"""")

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
    extraAllowedChars: String,
    maxLength: Int = Int.MaxValue,
) extends StringModuleImpl
    with ConcatenableStringModule[String, String] {

  private val disallowedChar =
    IdString.asciiCharsToRejectionArray(IdString.letters ++ IdString.digits ++ extraAllowedChars)

  @throws[IllegalArgumentException]
  override def assertFromString(s: String) = {
    if (s.length == 0)
      throw new IllegalArgumentException(s"""$description is empty""")
    if (s.length > maxLength)
      throw new IllegalArgumentException(s"""$description is too long (max: $maxLength)""")
    var i = 0
    while (i < s.length) {
      val c = s(i).toInt
      if (c > 0x7f || disallowedChar(c))
        throw new IllegalArgumentException(s"""non expected character 0x${s(
            i
          ).toInt.toHexString} in $description "$s"""")
      i += 1
    }
    s
  }

  override def fromLong(i: Long): T = i.toString

  override def fromInt(i: Int): T = fromLong(i.toLong)

  override def concat(s: T, ss: T*): Either[String, T] = {
    val b = new StringBuilder
    discard(b ++= s)
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
  override val Name: StringModule[Name] = new StringModuleImpl {

    val maxLength = 1000

    private[this] val disallowedFirstChar =
      IdString.asciiCharsToRejectionArray(IdString.letters ++ "$_")
    private[this] val disallowedOtherChar =
      IdString.asciiCharsToRejectionArray(IdString.letters ++ IdString.digits ++ "$_")

    @throws[IllegalArgumentException]
    override def assertFromString(s: String) = {
      if (s.isEmpty)
        throw new IllegalArgumentException("Daml-LF Name is empty")
      if (s.length > maxLength)
        throw new IllegalArgumentException(s"""Name is too long (max: $maxLength)""")
      val c = s(0).toInt
      if (c > 0x7f || disallowedFirstChar(c))
        throw new IllegalArgumentException(
          s"""non expected first character 0x${c.toHexString} in Daml-LF Name "$s""""
        )
      var i = 1
      while (i < s.length) {
        val c = s(i).toInt
        if (c > 0x7f || disallowedOtherChar(c))
          throw new IllegalArgumentException(
            s"""non expected non first character 0x${c.toHexString} in Daml-LF Name "$s""""
          )
        i += 1
      }
      s
    }
  }

  /** Package names are non-empty US-ASCII strings built from letters, digits, minus and underscore.
    */
  override type PackageName = String
  override val PackageName: ConcatenableStringModule[PackageName, HexString] =
    new ConcatenableMatchingStringModule("Daml-LF Package Name", "-_")

  /** Package versions are non-empty strings consisting of segments of digits (without leading zeros)
    *      separated by dots.
    */
  override type PackageVersion = String
  override val PackageVersion: StringModule[PackageVersion] =
    new MatchingStringModule("Daml-LF Package Version", """(0|[1-9][0-9]*)(\.(0|[1-9][0-9]*))*""")

  /** Party identifiers are non-empty US-ASCII strings built from letters, digits, space, colon, minus and,
    * underscore limited to 255 chars. We use them to represent [Party] literals. In this way, we avoid
    * empty identifiers, escaping problems, and other similar pitfalls.
    */
  override type Party = String
  override val Party: ConcatenableStringModule[Party, HexString] =
    new ConcatenableMatchingStringModule("Daml-LF Party", ":-_ ", 255)

  /** Reference to a package via a package identifier. The identifier is the ascii7
    * lowercase hex-encoded hash of the package contents found in the Daml-LF Archive.
    */
  override type PackageId = String
  override val PackageId: ConcatenableStringModule[PackageId, HexString] =
    new ConcatenableMatchingStringModule("Daml-LF Package ID", "-_ ", 64)

  /** Used to reference to leger objects like contractIds, ledgerIds,
    * transactionId, ... We use the same type for those ids, because we
    * construct some by concatenating the others.
    */
  // We allow space because the navigator's applicationId used it.
  override type LedgerString = String
  override val LedgerString: ConcatenableStringModule[LedgerString, HexString] =
    new ConcatenableMatchingStringModule("Daml-LF Ledger String", "._:-#/ ", 255)

  override type ParticipantId = String
  override val ParticipantId = LedgerString

  /** Legacy contractIds.
    */
  override type ContractIdString = String
  override val ContractIdString: StringModule[ContractIdString] =
    new MatchingStringModule("Daml-LF Contract ID", """#[\w._:\-#/ ]{0,254}""")

  /** Identifiers for applications as used in custom tokens and requests.
    * They used to be equal to [[LedgerString]], but additionally allow the symbols
    * "!|@^$`+'~" when introducing participant users to ensure that they are a superset of [[UserId]].
    */
  override type ApplicationId = String
  override val ApplicationId: ConcatenableStringModule[LedgerString, HexString] =
    new ConcatenableMatchingStringModule("Application ID", "._:-#/!|@^$`+'~ ", 255)

  /** Identifiers for participant node users are non-empty strings with a length <= 128 that consist of
    * ASCII alphanumeric characters and the symbols "@^$.!`-#+'~_|:".
    * This character set is chosen such that it maximizes the ease of integration with IAM systems.
    * Since the Ledger API needs to be compatible with JWT and uses the "sub" registered claim to
    * represent participant node users, the definition and use of these identifiers must keep in
    * consideration the definition of the aforementioned claim: https://www.rfc-editor.org/rfc/rfc7519#section-4.1.2
    */
  override type UserId = String
  override val UserId: StringModule[UserId] =
    new MatchingStringModule("User ID", """[a-zA-Z0-9@^$.!`\-#+'~_|:]{1,128}""")

}
