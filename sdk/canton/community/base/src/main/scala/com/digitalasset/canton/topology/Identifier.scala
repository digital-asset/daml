// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.Order
import cats.implicits.*
import com.digitalasset.canton.ProtoDeserializationError.ValueConversionError
import com.digitalasset.canton.config.CantonRequireTypes.{String185, String255, String68}
import com.digitalasset.canton.crypto.{Fingerprint, HasFingerprint}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{LfPartyId, ProtoDeserializationError, checked}
import slick.jdbc.{GetResult, SetParameter}

/** utility class to ensure that strings conform to LF specification minus our internal delimiter */
object SafeSimpleString {

  val delimiter = "::"

  def fromProtoPrimitive(str: String): Either[String, String] = {
    for {
      _ <- LfPartyId.fromString(str)
      opt <- Either.cond(
        !str.contains(delimiter),
        str,
        s"String contains reserved delimiter `$delimiter`.",
      )
    } yield opt
  }

}

object Namespace {
  implicit val setParameterNamespace: SetParameter[Namespace] = (v, pp) =>
    pp >> v.toLengthLimitedString
  implicit val namespaceOrder: Order[Namespace] = Order.by[Namespace, String](_.unwrap)
  implicit val setParameterOptionNamespace: SetParameter[Option[Namespace]] = (v, pp) =>
    pp >> v.map(_.toLengthLimitedString)
}

// architecture-handbook-entry-begin: UniqueIdentifier
/** A namespace spanned by the fingerprint of a pub-key
  *
  * This is based on the assumption that the fingerprint is unique to the public-key
  */
final case class Namespace(fingerprint: Fingerprint) extends HasFingerprint with PrettyPrinting {
  def unwrap: String = fingerprint.unwrap
  def toProtoPrimitive: String = fingerprint.toProtoPrimitive
  def toLengthLimitedString: String68 = fingerprint.toLengthLimitedString
  def filterString: String = fingerprint.unwrap
  override def pretty: Pretty[Namespace] = prettyOfParam(_.fingerprint)
}

trait HasNamespace extends HasFingerprint {
  @inline def namespace: Namespace

  @inline final override def fingerprint: Fingerprint = namespace.fingerprint
}

/** a unique identifier within a namespace
  * Based on the Ledger API PartyIds/LedgerStrings being limited to 255 characters, we allocate
  * - 64 + 4 characters to the namespace/fingerprint (essentially SHA256 with extra bytes),
  * - 2 characters as delimiters, and
  * - the last 185 characters for the Identifier.
  */
final case class UniqueIdentifier private (identifier: String185, namespace: Namespace)
    extends HasNamespace
    with PrettyPrinting {
// architecture-handbook-entry-end: UniqueIdentifier

  def toProtoPrimitive: String =
    identifier.toProtoPrimitive + UniqueIdentifier.delimiter + namespace.toProtoPrimitive

  def toLengthLimitedString: String255 = checked(String255.tryCreate(toProtoPrimitive))

  /** Replace the identifier with a new string
    *
    * In many tests, we create new parties based off existing parties. As the constructor is
    * private, using the copy method won't work, but this function here recovers the convenience.
    */
  def tryChangeId(id: String): UniqueIdentifier = UniqueIdentifier.tryCreate(id, namespace)

  // utility to filter UIDs using prefixes obtained via UniqueIdentifier.splitFilter() below
  def matchesPrefixes(idPrefix: String, nsPrefix: String): Boolean =
    identifier.unwrap.startsWith(idPrefix) && namespace.toProtoPrimitive.startsWith(
      nsPrefix
    )

  override def pretty: Pretty[this.type] =
    prettyOfString(uid => uid.identifier.str.show + UniqueIdentifier.delimiter + uid.namespace.show)

}

object UniqueIdentifier {

  /** delimiter used to separate the identifier from the fingerprint */
  val delimiter = "::"

  /** verifies that the string conforms to the lf standard and does not contain the delimiter */
  def verifyValidString(str: String): Either[String, String] = {
    for {
      // use LfPartyId to verify that the string matches the lf specification
      _ <- LfPartyId.fromString(str)
      opt <- Either.cond(
        !str.contains(delimiter),
        str,
        s"String contains reserved delimiter `$delimiter`.",
      )
    } yield opt
  }

  private def validIdentifier(id: String): Either[String, String185] =
    verifyValidString(id).flatMap(String185.create(_))

  def create(id: String, namespace: Namespace): Either[String, UniqueIdentifier] =
    for {
      idf <- validIdentifier(id)
    } yield UniqueIdentifier(idf, namespace)
  def create(id: String, fingerprint: Fingerprint): Either[String, UniqueIdentifier] =
    create(id, Namespace(fingerprint))

  /** Create a unique identifier
    *
    * @param id the identifier (prefix) that can be chosen freely but must conform to the LF standard, not exceed 185 chars and must not use two consecutive columns.
    * @param fingerprint the fingerprint of the namespace, which is normally a hash of the public key, but in some tests might be chosen freely.
    */
  def create(id: String, fingerprint: String): Either[String, UniqueIdentifier] =
    for {
      fp <- Fingerprint.fromProtoPrimitive(fingerprint).leftMap(_.message)
      uid <- create(id, fp)
    } yield uid

  def tryCreate(id: String, namespace: Namespace): UniqueIdentifier =
    create(id, namespace).valueOr(e => throw new IllegalArgumentException(e))

  def tryCreate(id: String, fingerprint: Fingerprint): UniqueIdentifier =
    create(id, fingerprint).valueOr(e => throw new IllegalArgumentException(e))

  def tryCreate(id: String, fingerprint: String): UniqueIdentifier =
    create(id, fingerprint).valueOr(e => throw new IllegalArgumentException(e))

  def tryFromProtoPrimitive(str: String): UniqueIdentifier =
    fromProtoPrimitive_(str).valueOr(e => throw new IllegalArgumentException(e.message))

  def fromProtoPrimitive_(str: String): ParsingResult[UniqueIdentifier] = {
    val pos = str.indexOf(delimiter)
    val ret = if (pos > 0) {
      val s1 = str.substring(0, pos)
      val s2 = str.substring(pos + 2)
      for {
        idf <- validIdentifier(s1)
          .leftMap(x => s"Identifier decoding of `${str.limit(200)}` failed with: $x")
        fp <- Fingerprint
          .fromProtoPrimitive(s2)
          .leftMap(x => s"Fingerprint decoding of `${str.limit(200)}` failed with: $x")
      } yield UniqueIdentifier(idf, Namespace(fp))
    } else if (pos == 0) {
      Left(s"Invalid unique identifier `$str` with empty identifier.")
    } else if (str.isEmpty) {
      Left(s"Empty string is not a valid unique identifier.")
    } else {
      Left(s"Invalid unique identifier `$str` with missing namespace.")
    }
    ret.leftMap(ProtoDeserializationError.StringConversionError)
  }

  def fromProtoPrimitive(
      uid: String,
      fieldName: String,
  ): ParsingResult[UniqueIdentifier] =
    fromProtoPrimitive_(uid).leftMap(err => ValueConversionError(fieldName, err.message))

  // slick instance for deserializing unique identifiers
  // not an implicit because we shouldn't ever need to parse a raw unique identifier
  val getResult: GetResult[UniqueIdentifier] = GetResult(r => deserializeFromDb(r.nextString()))
  val getResultO: GetResult[Option[UniqueIdentifier]] =
    GetResult(r => r.nextStringOption().map(deserializeFromDb))
  implicit val setParameterUid: SetParameter[UniqueIdentifier] = (v, pp) =>
    pp >> v.toLengthLimitedString

  /** @throws com.digitalasset.canton.store.db.DbDeserializationException if the string is not a valid unqiue identifier */
  def deserializeFromDb(uid: String): UniqueIdentifier =
    fromProtoPrimitive_(uid).valueOr(err =>
      throw new DbDeserializationException(s"Failed to parse a unique ID $uid: $err")
    )

  /** Split an uid filter into the two subparts */
  def splitFilter(filter: String, append: String = ""): (String, String) = {
    val items = filter.split(UniqueIdentifier.delimiter)
    val prefix = items(0)
    if (items.lengthCompare(1) > 0) {
      val suffix = items(1)
      (prefix ++ append, suffix ++ append)
    } else (prefix ++ append, append)
  }

}

trait HasUniqueIdentifier extends HasNamespace {
  @inline def uid: UniqueIdentifier

  @inline final override def namespace: Namespace = uid.namespace

  @inline final def identifier: String185 = uid.identifier
}
