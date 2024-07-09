// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.Order
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation as ProtoInvariantViolation
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName.InvalidInstanceName
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.InvalidLengthString
import com.digitalasset.canton.config.RequireTypes.InvariantViolation
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.util.ShowUtil.*
import io.circe.{Encoder, KeyEncoder}
import pureconfig.error.FailureReason
import pureconfig.{ConfigReader, ConfigWriter}
import slick.jdbc.{GetResult, SetParameter}

import java.util.UUID

/** Encapsulates those classes and their utility methods which enforce a given invariant via the use of require. */
object CantonRequireTypes {
  final case class NonEmptyString(private val str: String) extends NoCopy {
    def unwrap: String = str
    require(str.nonEmpty, s"Unable to create a NonEmptyString as the empty string $str was given.")
  }

  object NonEmptyString {
    private[this] def apply(str: String): NonEmptyString =
      throw new UnsupportedOperationException("Use create or tryCreate methods")

    def create(str: String): Either[InvariantViolation, NonEmptyString] = {
      Either.cond(
        str.nonEmpty,
        new NonEmptyString(str),
        InvariantViolation(s"Unable to create a NonEmptyString as the empty string $str was given."),
      )
    }

    def tryCreate(str: String): NonEmptyString = {
      new NonEmptyString(str)
    }

    lazy implicit val nonEmptyStringReader: ConfigReader[NonEmptyString] = {
      ConfigReader.fromString[NonEmptyString] { str =>
        Either.cond(str.nonEmpty, new NonEmptyString(str), EmptyString(str))
      }
    }

    final case class EmptyString(str: String) extends FailureReason {
      override def description: String =
        s"The value you gave for this configuration setting ('$str') was the empty string, but we require a non-empty string for this configuration setting"
    }
  }

  /** This trait wraps a String that is limited to a certain maximum length.
    * The canonical use case is ensuring that we don't write too long strings into the database.
    *
    * You should normally implement [[LengthLimitedString]] or use its subclasses,
    * for strings to be stored in standard string columns.
    *
    * As this class implements fewer checks, this also serves as the basis for longer strings such as CLOBs.
    */
  sealed trait AbstractLengthLimitedString extends NoCopy {
    def str: String

    /** Maximum number of characters allowed.
      *
      * Must not be confused with storage space, which can be up to 4*[[maxLength]] in a UTF8 encoding
      */
    def maxLength: Int
    // optionally give a name for the type of String you are attempting to validate for nicer error messages
    protected def name: Option[String] = None

    // overwriting equals here to improve console UX - see e.g. issue i7071 for context
    @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
    def canEqual(a: Any): Boolean =
      a.isInstanceOf[AbstractLengthLimitedString] || a.isInstanceOf[String]

    override def equals(that: Any): Boolean =
      that match {
        case that: AbstractLengthLimitedString =>
          that.canEqual(this) && this.str == that.str && this.maxLength == that.maxLength
        case that: String => that.canEqual(this) && this.str == that
        case _ => false
      }

    override def hashCode(): Int = str.hashCode()

    require(
      str.length <= maxLength,
      s"The given ${name.getOrElse("string")} has a maximum length of $maxLength but a ${name
          .getOrElse("string")} of length ${str.length} ('$str') was given",
    )

    def unwrap: String = str
    def toProtoPrimitive: String = str

    override def toString: String = str

    def nonEmpty: Boolean = str.nonEmpty
  }

  /** This trait wraps a String that is limited to a certain maximum length.
    * Classes implementing this trait expose `create` and `tryCreate` methods to safely (and non-safely) construct
    * such a String.
    *
    * The canonical use case for [[LengthLimitedString]]s is ensuring that we don't write too long strings into the database:
    * Oracle has a length-limit of 1000 Unicode characters for the ordinary String type `NVARCHAR2` and we are trying to avoid
    * the use of CLOB (as it has pitfalls regarding implicits).
    * This validation generally occurs on the server side and not on the client side. Concretely, this means that the
    * Admin API and Ledger API gRPC services is the point where we validate that the received Protobuf Strings are not too long
    * (and convert them into [[LengthLimitedString]]s). On the client side, e.g. at the console, we generally take normal String types.
    * The console command `set_display_name` and service [[com.digitalasset.canton.participant.admin.grpc.GrpcPartyNameManagementService]]
    * validating `request.displayName` illustrate this.
    *
    * As a rule of thumb: whenever you want to create a column that uses a NVARCHAR2 in Oracle, the value you write to
    * it should use a LengthLimitedString.
    *
    * Some more background on the Oracle issues:
    * NVARCHAR and NVARCHAR2 have both by default a 4000 byte limit, but unicode uses 4-bytes per character (and nvarchar2 uses unicode)
    * Therefore, NVARCHAR has a limit of 4000 and NVARCHAR2 has a limit of 1000 characters
    * If need be, we can extend this to 32 KB by setting the Oracle database string size to 'extended mode' (ALTER SYSTEM SET MAX_STRING_SIZE=EXTENDED)
    *
    * For longer strings, directly inherit from [[AbstractLengthLimitedString]].
    */
  sealed trait LengthLimitedString extends AbstractLengthLimitedString {
    require(
      maxLength > 0 && maxLength <= LengthLimitedString.maxOracleStringLength,
      s"MaxLength needs to be positive and smaller equal than ${LengthLimitedString.maxOracleStringLength} but was $maxLength",
    )

    def tryConcatenate(that: LengthLimitedString): LengthLimitedStringVar =
      new LengthLimitedStringVar(this.unwrap + that.unwrap, this.maxLength + that.maxLength)()

    def tryConcatenate(that: String): LengthLimitedStringVar =
      new LengthLimitedStringVar(this.unwrap + that, this.maxLength + that.length)()
  }

  object LengthLimitedString {
    // Max length of unicode strings we can save as String types in Oracle columns - this can be increased to
    // 1000 for NVARCHAR2 but we set it to 300 for now since we don't need a higher limit and rather want to stay on the
    // conservative side
    val maxOracleStringLength = 300
    // In general, if you would create a case class that would simply wrap a `LengthLimitedString`, use a type alias instead
    // Some very frequently-used classes (like `Identifier` or `DomainAlias`) are however given their 'own' case class
    // despite essentially being a wrapper around `LengthLimitedString255` (because the documentation UX is nicer this way,
    // and one can e.g. write `Fingerprint.tryCreate` instead of `LengthLimitedString68.tryCreate`)
    type DisplayName = String255
    type TopologyRequestId = String255
    type DarName = String255

    def errorMsg(tooLongStr: String, maxLength: Int, name: Option[String] = None): String =
      s"The given ${name.getOrElse("string")} has a maximum length of $maxLength but a ${name
          .getOrElse("string")} of length ${tooLongStr.length} ('${tooLongStr.limit(maxLength + 50)}.') was given"

    val defaultMaxLength = 255

    def tryCreate(str: String, maxLength: Int, name: Option[String] = None): LengthLimitedString = {
      new LengthLimitedStringVar(str, maxLength)(name)
    }

    def getUuid: String36 = String36.tryCreate(UUID.randomUUID().toString)

    def create(
        str: String,
        maxLength: Int,
        name: Option[String] = None,
    ): Either[String, LengthLimitedString] = {
      Either.cond(
        str.length <= maxLength,
        new LengthLimitedStringVar(str, maxLength)(name),
        errorMsg(str, maxLength, name),
      )
    }

    // Should be used rarely - most of the time SetParameter[String255] etc.
    // (defined through LengthLimitedStringCompanion) should be used
    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    implicit val setParameterLengthLimitedString: SetParameter[LengthLimitedString] = (v, pp) =>
      pp.setString(v.unwrap)
    // Commented out so this function never accidentally throws
    //    implicit def getResultLengthLimitedString: GetResult[LengthLimitedString] =
    //      throw new UnsupportedOperationException(
    //        "Avoid attempting to read a generic LengthLimitedString from the database, as this may lead to unexpected " +
    //          "equality-comparisons (since a LengthLimitedString comparison also includes the maximum length and not only the string-content). " +
    //          "Instead refactor your code to expect a specific LengthLimitedString when reading from the database (e.g. via GetResult[String255]). " +
    //          "If you really need this functionality, then you can add this method again. ")

    implicit val orderingLengthLimitedString: Ordering[LengthLimitedString] =
      Ordering.by[LengthLimitedString, String](_.str)
    implicit val lengthLimitedStringOrder: Order[LengthLimitedString] =
      Order.by[LengthLimitedString, String](_.str)

    final case class InvalidLengthString(str: String) extends FailureReason {
      override def description: String =
        s"The string you gave for this configuration setting ('$str') had size ${str.length}, but we require a string with length <= $defaultMaxLength for this configuration setting"
    }
  }

  final case class String1(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String1.maxLength
  }
  object String1 extends LengthLimitedStringCompanion[String1] {
    def fromChar(c: Char): String1 = checked(new String1(c.toString)(None))

    override def maxLength: Int = 1

    override protected def factoryMethod(str: String)(name: Option[String]): String1 =
      new String1(str)(name)
  }

  /** Limit used for enum names. */
  final case class String3(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String3.maxLength
  }

  object String3 extends LengthLimitedStringCompanion[String3] {
    override def maxLength: Int = 3

    override protected def factoryMethod(str: String)(name: Option[String]): String3 =
      new String3(str)(name)
  }

  /** Limit used by a UUID. */
  final case class String36(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String36.maxLength

    def asString255: String255 = String255.tryCreate(str, name)
  }

  object String36 extends LengthLimitedStringCompanion[String36] {
    override def maxLength: Int = 36

    override protected def factoryMethod(str: String)(name: Option[String]): String36 =
      new String36(str)(name)
  }

  /** Limit used by a hash (SHA256 in particular) in a [[com.digitalasset.canton.topology.UniqueIdentifier]].
    *
    * @see com.digitalasset.canton.topology.UniqueIdentifier for documentation on its origin
    */
  final case class String68(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String68.maxLength
  }

  object String68 extends LengthLimitedStringCompanion[String68] {
    override def maxLength: Int = 68

    override def factoryMethod(str: String)(name: Option[String]): String68 =
      new String68(str)(name)
  }

  /** Limit used by a [[com.digitalasset.canton.sequencing.protocol.MessageId]]. */
  final case class String73(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String73.maxLength
  }

  object String73 extends LengthLimitedStringCompanion[String73] {
    override def maxLength: Int = 73

    override protected def factoryMethod(str: String)(name: Option[String]): String73 =
      new String73(str)(name)
  }

  final case class String100(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String100.maxLength
  }
  object String100 extends LengthLimitedStringCompanion[String100] {
    override def maxLength: Int = 100
    override protected def factoryMethod(str: String)(name: Option[String]): String100 =
      new String100(str)(name)
  }

  /** Limit used by [[com.digitalasset.canton.topology.UniqueIdentifier]].
    *
    * @see com.digitalasset.canton.topology.Identifier for documentation on its origin
    */
  final case class String185(str: String)(override protected val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String185.maxLength
  }

  object String185 extends LengthLimitedStringCompanion[String185] {
    override def maxLength: Int = 185

    override def factoryMethod(str: String)(name: Option[String]): String185 =
      new String185(str)(name)
  }

  /** Default [[LengthLimitedString]] that should be used when in doubt.
    * 255 was chosen as it is also the limit used in the upstream code for, e.g., LedgerStrings in the upstream code
    *
    * @param name optionally set it to improve the error message. It is given as an extra argument, so the automatically generated `equals`-method doesn't use it for comparison
    */
  final case class String255(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String255.maxLength

    def asString300: String300 = new String300(str)(name)
    def asString1GB: String256M = new String256M(str)(name)
  }

  object String255 extends LengthLimitedStringCompanion[String255] {
    override def maxLength = 255

    override def factoryMethod(str: String)(name: Option[String]): String255 =
      new String255(str)(name)
  }

  /** Longest limited-length strings that have been needed so far.
    * Typical use case: when a 255-length identifier is combined
    * with other short suffixes or prefixes to further specialize them.
    *
    * @see com.digitalasset.canton.store.db.SequencerClientDiscriminator
    * @see com.digitalasset.canton.crypto.KeyName
    */
  final case class String300(str: String)(override val name: Option[String] = None)
      extends LengthLimitedString {
    override def maxLength: Int = String300.maxLength
  }

  object String300 extends LengthLimitedStringCompanion[String300] {
    override def maxLength = 300

    override def factoryMethod(str: String)(name: Option[String]): String300 =
      new String300(str)(name)
  }

  /** Length limitation for an [[com.digitalasset.canton.protocol.LfTemplateId]].
    * A [[com.digitalasset.canton.protocol.LfTemplateId]] consists of
    * - The module name ([[com.digitalasset.daml.lf.data.Ref.DottedName]])
    * - The template name ([[com.digitalasset.daml.lf.data.Ref.DottedName]])
    * - The package ID
    * - Two separating dots
    * Each [[com.digitalasset.daml.lf.data.Ref.DottedName]] can have 1000 chars ([[com.digitalasset.daml.lf.data.Ref.DottedName.maxLength]]).
    * So a [[com.digitalasset.canton.protocol.LfTemplateId]] serializes to 1000 + 1000 + 64 + 2 = 2066 chars.
    *
    * 2066 is beyond the string size for Oracle's `NVARCHAR2` column type unless `max_string_size` is set to `extended`.
    * Such strings may therefore be written into `VARCHAR2` columns using an encoding that does not exceed the 4000 bytes limit.
    * UTF8 is such an encoding for ASCII-only strings, but we do not yet test that `str` really contains only ASCII characters.
    */
  final case class String2066(str: String)(override val name: Option[String] = None)
      extends AbstractLengthLimitedString {
    override def maxLength: Int = String2066.maxLength
  }
  object String2066 extends LengthLimitedStringCompanion[String2066] {
    override def maxLength: Int = 4000

    override protected def factoryMethod(str: String)(name: Option[String]): String2066 =
      new String2066(str)(name)
  }

  /** Length limitation of a `TEXT` or unbounded `VARCHAR` field in postgres or `CLOB` in Oracle.
    * - Postgres `TEXT` or `VARCHAR` support up to 1GB storage. That is at least `2 ^ 28` characters
    *   in UTF8 encoding as each character needs at most 4 bytes.
    * - Oracle `CLOB` supports up to 4GB storage, i.e., at least `2 ^ 30` UTF8 characters
    *
    * `TEXT`/`VARCHAR`/`CLOB` are only used for the following values (none are indices):
    * - daml_packages.source_description
    * - topology_transactions.ignore_reason
    */
  final case class String256M(str: String)(override val name: Option[String] = None)
      extends AbstractLengthLimitedString {
    override def maxLength: Int = String256M.maxLength
  }
  object String256M extends LengthLimitedStringCompanion[String256M] {
    override def maxLength: Int = 0x10000000

    override protected def factoryMethod(str: String)(name: Option[String]): String256M =
      new String256M(str)(name)
  }

  final case class LengthLimitedStringVar(override val str: String, maxLength: Int)(
      override val name: Option[String] = None
  ) extends LengthLimitedString
  object LengthLimitedStringVar {
    private[this] def apply(str: String): LengthLimitedStringVar =
      throw new UnsupportedOperationException("Use create or tryCreate methods")
  }

  /** Trait that implements method commonly needed in the companion object of an [[AbstractLengthLimitedString]] */
  trait LengthLimitedStringCompanion[A <: AbstractLengthLimitedString] {

    val empty: A = checked(factoryMethod("")(None))

    /** The maximum string length. Should not be overwritten with `val` to avoid initialization issues. */
    def maxLength: Int

    /** Factory method for creating a string.
      * @throws java.lang.IllegalArgumentException if `str` is longer than [[maxLength]]
      */
    protected def factoryMethod(str: String)(name: Option[String]): A

    def create(str: String, name: Option[String] = None): Either[String, A] =
      Either.cond(
        str.length <= maxLength,
        factoryMethod(str)(name),
        LengthLimitedString.errorMsg(str, maxLength, name),
      )

    private[this] def apply(str: String): A =
      throw new UnsupportedOperationException("Use create or tryCreate methods")

    def tryCreate(str: String, name: Option[String] = None): A =
      factoryMethod(str)(name)

    def fromProtoPrimitive(str: String, name: String): ParsingResult[A] =
      create(str, Some(name)).leftMap(e => ProtoInvariantViolation(field = Some(name), error = e))

    implicit val lengthLimitedStringOrder: Order[A] =
      Order.by[A, String](_.str)

    implicit val encodeLengthLimitedString: Encoder[A] =
      Encoder.encodeString.contramap[A](_.unwrap)

    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    implicit val setParameterLengthLimitedString: SetParameter[A] = (v, pp) =>
      pp.setString(v.unwrap)
    implicit val getResultLengthLimitedString: GetResult[A] =
      GetResult(r => tryCreate(r.nextString()))

    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    implicit val setParameterOptLengthLimitedString: SetParameter[Option[A]] = (v, pp) =>
      pp.setStringOption(v.map(_.unwrap))
    implicit val getResultOptLengthLimitedString: GetResult[Option[A]] =
      GetResult(r => r.nextStringOption().map(tryCreate(_)))

    implicit val lengthLimitedStringReader: ConfigReader[A] = {
      ConfigReader.fromString[A] { str =>
        Either.cond(
          str.nonEmpty && str.length <= maxLength,
          factoryMethod(str)(None),
          InvalidLengthString(str),
        )
      }
    }

    implicit val lengthLimitedStringWriter: ConfigWriter[A] = ConfigWriter.toString(_.unwrap)
  }

  /** Trait for case classes that are a wrapper around a [[LengthLimitedString]].
    * @see com.digitalasset.canton.crypto.CertificateId for an example
    */
  trait LengthLimitedStringWrapper {
    protected val str: LengthLimitedString
    def unwrap: String = str.unwrap
    def toProtoPrimitive: String = str.unwrap
    override def toString: String = unwrap
    // overwriting equals here to improve console UX - see e.g. issue i7071 for context
    @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
    def canEqual(a: Any): Boolean =
      a.isInstanceOf[LengthLimitedStringWrapper] || a.isInstanceOf[String]

    override def equals(that: Any): Boolean =
      that match {
        case that: LengthLimitedStringWrapper =>
          that.canEqual(this) && this.getClass == that.getClass && this.str == that.str
        case that: String => that.canEqual(this) && this.unwrap == that
        case _ => false
      }

    override def hashCode(): Int = unwrap.hashCode()
  }

  /** Trait that implements utility methods to avoid boilerplate in the companion object of a case class that wraps a
    * [[LengthLimitedString]] type using [[LengthLimitedStringWrapper]].
    *
    * @see com.digitalasset.canton.crypto.CertificateId for an example
    */
  trait LengthLimitedStringWrapperCompanion[
      A <: LengthLimitedString,
      Wrapper <: LengthLimitedStringWrapper,
  ] {

    def instanceName: String
    protected def companion: LengthLimitedStringCompanion[A]
    protected def factoryMethodWrapper(str: A): Wrapper

    def create(str: String): Either[String, Wrapper] =
      companion.create(str, instanceName.some).map(factoryMethodWrapper)

    def tryCreate(str: String): Wrapper = factoryMethodWrapper(
      companion.tryCreate(str, instanceName.some)
    )

    def fromProtoPrimitive(str: String): ParsingResult[Wrapper] =
      companion.fromProtoPrimitive(str, instanceName).map(factoryMethodWrapper)

    implicit val wrapperOrder: Order[Wrapper] =
      Order.by[Wrapper, String](_.unwrap)

    implicit val encodeWrapper: Encoder[Wrapper] =
      Encoder.encodeString.contramap[Wrapper](_.unwrap)

    // Instances for slick (db) queries
    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    implicit val setParameterWrapper: SetParameter[Wrapper] = (v, pp) =>
      pp.setString(v.toProtoPrimitive)
    implicit val getResultWrapper: GetResult[Wrapper] = GetResult(r =>
      fromProtoPrimitive(r.nextString()).valueOr(err =>
        throw new DbDeserializationException(err.toString)
      )
    )

    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    implicit val setParameterOptionWrapper: SetParameter[Option[Wrapper]] = (v, pp) =>
      pp.setStringOption(v.map(_.toProtoPrimitive))
    implicit val getResultOptionWrapper: GetResult[Option[Wrapper]] = GetResult { r =>
      r.nextStringOption()
        .traverse(fromProtoPrimitive)
        .valueOr(err => throw new DbDeserializationException(err.toString))
    }
  }

  final case class InstanceName private (unwrap: String) extends NoCopy with PrettyPrinting {

    if (!unwrap.matches("^[a-zA-Z0-9_-]*$")) {
      throw InvalidInstanceName(
        show"Node name contains invalid characters (allowed: [a-zA-Z0-9_-]): " +
          show"${unwrap.limit(InstanceName.maxLength).toString.doubleQuoted}"
      )
    }

    if (unwrap.isEmpty) {
      throw InvalidInstanceName(
        "Empty node name."
      )
    }

    if (unwrap.length > InstanceName.maxLength) {
      throw InvalidInstanceName(
        show"Node name is too long. Max length: ${InstanceName.maxLength}. Length: ${unwrap.length}. " +
          show"Name: ${unwrap.limit(InstanceName.maxLength).toString.doubleQuoted}"
      )
    }

    def toProtoPrimitive: String = unwrap

    override def pretty: Pretty[InstanceName] = prettyOfParam(_.unwrap.unquoted)
  }

  object InstanceName {
    val maxLength: Int = 30

    def create(str: String): Either[InvalidInstanceName, InstanceName] = Either
      .catchOnly[InvalidInstanceName](tryCreate(str))

    def tryCreate(str: String): InstanceName = InstanceName(str)

    def tryFromStringMap[A](map: Map[String, A]): Map[InstanceName, A] = map.map { case (n, c) =>
      tryCreate(n) -> c
    }

    final case class InvalidInstanceName(override val description: String)
        extends RuntimeException(description)
        with FailureReason

    implicit val instanceNameReader: ConfigReader[InstanceName] = ConfigReader.fromString(create)
    implicit def instanceNameKeyReader[A: ConfigReader]: ConfigReader[Map[InstanceName, A]] =
      pureconfig.configurable.genericMapReader(create)

    implicit val instanceNameWriter: ConfigWriter[InstanceName] = ConfigWriter.toString(_.unwrap)
    implicit def instanceNameKeyWriter[A: ConfigWriter]: ConfigWriter[Map[InstanceName, A]] =
      pureconfig.configurable.genericMapWriter(_.unwrap)

    implicit val encodeInstanceName: Encoder[InstanceName] =
      Encoder.encodeString.contramap(_.unwrap)
    implicit val encodeKeyInstanceName: KeyEncoder[InstanceName] =
      KeyEncoder.encodeKeyString.contramap(_.unwrap)
  }

}
