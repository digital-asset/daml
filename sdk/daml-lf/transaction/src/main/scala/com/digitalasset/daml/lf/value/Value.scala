// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name, TypeConId}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{Ast, StablePackages}
import data.ScalazEqual._
import scalaz.{Equal, Order}
import scalaz.syntax.order._
import scalaz.syntax.semigroup._

import java.nio.{ByteBuffer, ByteOrder}

sealed abstract class GenValue[+X]
    extends CidContainer[GenValue[X]]
    with Product
    with Serializable {
  def nonVerboseWithoutTrailingNones: GenValue[X]
}

object GenValue {

  sealed trait Extension[+B]

  sealed abstract class CidLessAtom extends GenValue[Nothing] with CidContainer[CidLessAtom] {
    final override def mapCid(f: Value.ContractId => Value.ContractId): this.type = this
  }

  final case class Record[+X](
      tycon: Option[Identifier],
      fields: ImmArray[(Option[Name], GenValue[X])],
  ) extends GenValue[X]
      with CidContainer[Record[X]] {
    // TODO (FM) make this tail recursive
    override def mapCid(f: Value.ContractId => Value.ContractId): Record[X] =
      Record(
        tycon,
        fields.map { case (lbl, value) =>
          (lbl, value.mapCid(f))
        },
      )

    override def nonVerboseWithoutTrailingNones: Record[X] = {
      val n = fields.reverseIterator.dropWhile(_._2 == Value.ValueNone).size
      Record(
        None,
        fields.iterator
          .take(n)
          .map { case (_, v) => (None, v.nonVerboseWithoutTrailingNones) }
          .to(ImmArray),
      )
    }
  }

  final case class Variant[+X](tycon: Option[Identifier], variant: Name, value: GenValue[X])
      extends GenValue[X]
      with CidContainer[Variant[X]] {
    // TODO (FM) make this tail recursive
    override def mapCid(f: Value.ContractId => Value.ContractId): Variant[X] =
      Variant(tycon, variant, value.mapCid(f))

    override def nonVerboseWithoutTrailingNones: Variant[X] =
      Variant(None, variant, value.nonVerboseWithoutTrailingNones)
  }
  final case class Enum(tycon: Option[Identifier], value: Name)
      extends CidLessAtom
      with CidContainer[Enum] {
    override def nonVerboseWithoutTrailingNones: Value =
      Enum(None, value)
  }

  final case class ContractId(value: Value.ContractId)
      extends GenValue[Nothing]
      with CidContainer[ContractId] {
    override def mapCid(f: Value.ContractId => Value.ContractId): ContractId = ContractId(f(value))

    override def nonVerboseWithoutTrailingNones: this.type = this
  }

  /** Daml-LF lists are basically linked lists. However we use FrontQueue since we store list-literals in the Daml-LF
    * packages and FrontQueue lets prepend chunks rather than only one element.
    */
  final case class List[+X](values: FrontStack[GenValue[X]])
      extends GenValue[X]
      with CidContainer[List[X]] {
    // TODO (FM) make this tail recursive
    override def mapCid(f: Value.ContractId => Value.ContractId): List[X] = List(
      values.map(_.mapCid(f))
    )

    override def nonVerboseWithoutTrailingNones: List[X] = List(
      values.map(_.nonVerboseWithoutTrailingNones)
    )
  }

  final case class Int64(value: Long) extends CidLessAtom with CidContainer[Int64] {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }
  final case class Numeric(value: data.Numeric) extends CidLessAtom with CidContainer[Numeric] {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }
  // Note that Text are assume to be UTF8
  final case class Text(value: String) extends CidLessAtom with CidContainer[Text] {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }
  final case class Timestamp(value: Time.Timestamp)
      extends CidLessAtom
      with CidContainer[Timestamp] {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }
  final case class Date(value: Time.Date) extends CidLessAtom with CidContainer[Date] {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }
  final case class Party(value: Ref.Party) extends CidLessAtom with CidContainer[Party] {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }
  final case class Bool(value: Boolean) extends CidLessAtom with CidContainer[Bool] {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }
  object Bool {
    val True = new Bool(true)
    val False = new Bool(false)
    def apply(value: Boolean): Bool =
      if (value) True else False
  }
  case object Unit extends CidLessAtom {
    override def nonVerboseWithoutTrailingNones: this.type = this
  }

  final case class Optional[+X](value: Option[GenValue[X]]) extends GenValue[X] {
    // TODO (FM) make this tail recursive
    override def mapCid(f: Value.ContractId => Value.ContractId): Optional[X] = Optional(
      value.map(_.mapCid(f))
    )

    override def nonVerboseWithoutTrailingNones: Optional[X] = Optional(
      value.map(_.nonVerboseWithoutTrailingNones)
    )
  }
  final case class TextMap[+X](value: SortedLookupList[GenValue[X]]) extends GenValue[X] {
    // TODO (FM) make this tail recursive
    override def mapCid(f: Value.ContractId => Value.ContractId): TextMap[X] = TextMap(
      value.mapValue(_.mapCid(f))
    )

    override def nonVerboseWithoutTrailingNones: TextMap[X] = TextMap(
      value.mapValue(_.nonVerboseWithoutTrailingNones)
    )
  }
  final case class GenMap[+X](entries: ImmArray[(GenValue[X], GenValue[X])])
      extends GenValue[X]
      with CidContainer[GenMap[X]] {
    // TODO (FM) make this tail recursive
    override def mapCid(f: Value.ContractId => Value.ContractId): GenMap[X] = GenMap(entries.map {
      case (k, v) => k.mapCid(f) -> v.mapCid(f)
    })
    override def toString: String = entries.iterator.mkString("GenMap(", ",", ")")

    override def nonVerboseWithoutTrailingNones: GenMap[X] = GenMap(entries.map { case (k, v) =>
      k.nonVerboseWithoutTrailingNones -> v.nonVerboseWithoutTrailingNones
    })
  }

  final case class Blob[Content](value: Content) extends GenValue[Extension[Content]] {
    override def nonVerboseWithoutTrailingNones: Nothing =
      throw new UnsupportedOperationException(
        "Blob values do not support nonVerboseWithoutTrailingNones"
      )

    override def mapCid(f: Value.ContractId => Value.ContractId): Nothing =
      throw new UnsupportedOperationException(
        "Blob values do not support nonVerboseWithoutTrailingNones"
      )
    private[lf] def getContent: Content = value
  }

  final case class Any[Content](ty: Ast.Type, any: GenValue[Extension[Content]])
      extends GenValue[Extension[Content]] {
    override def nonVerboseWithoutTrailingNones: Nothing =
      throw new UnsupportedOperationException(
        "Any values do not support nonVerboseWithoutTrailingNones"
      )

    override def mapCid(f: Value.ContractId => Value.ContractId): Nothing =
      throw new UnsupportedOperationException(
        "Any values do not support nonVerboseWithoutTrailingNones"
      )
  }

  final case class TypeRep[Content](ty: Ast.Type) extends GenValue[Extension[Content]] {
    override def nonVerboseWithoutTrailingNones: Nothing =
      throw new UnsupportedOperationException(
        "TypeRep values do not support nonVerboseWithoutTrailingNones"
      )

    override def mapCid(f: Value.ContractId => Value.ContractId): Nothing =
      throw new UnsupportedOperationException(
        "TypeRep values do not support nonVerboseWithoutTrailingNones"
      )
  }

}

object Value {

  /** the maximum nesting level for Daml-LF serializable values. we put this
    * limitation to be able to reliably implement stack safe programs with it.
    * right now it's 100 to be conservative -- it's in the same order of magnitude
    * as the default maximum nesting value of protobuf.
    *
    * encoders and decoders should check this to make sure values do not exceed
    * this level of nesting.
    */
  val MAXIMUM_NESTING: Int = 100

  type VersionedValue = transaction.Versioned[Value]

  type ValueCidLessAtom = GenValue.CidLessAtom

  type ValueRecord = GenValue.Record[Nothing]
  val ValueRecord: GenValue.Record.type = GenValue.Record

  type ValueVariant = GenValue.Variant[Nothing]
  val ValueVariant: GenValue.Variant.type = GenValue.Variant

  type ValueEnum = GenValue.Enum
  val ValueEnum: GenValue.Enum.type = GenValue.Enum

  type ValueContractId = GenValue.ContractId
  val ValueContractId: GenValue.ContractId.type = GenValue.ContractId

  type ValueList = GenValue.List[Nothing]
  val ValueList: GenValue.List.type = GenValue.List

  type ValueOptional = GenValue.Optional[Nothing]
  val ValueOptional: GenValue.Optional.type = GenValue.Optional

  type ValueTextMap = GenValue.TextMap[Nothing]
  val ValueTextMap: GenValue.TextMap.type = GenValue.TextMap

  type ValueGenMap = GenValue.GenMap[Nothing]
  val ValueGenMap: GenValue.GenMap.type = GenValue.GenMap

  type ValueInt64 = GenValue.Int64
  val ValueInt64: GenValue.Int64.type = GenValue.Int64

  type ValueNumeric = GenValue.Numeric
  val ValueNumeric: GenValue.Numeric.type = GenValue.Numeric

  type ValueText = GenValue.Text
  val ValueText: GenValue.Text.type = GenValue.Text

  type ValueTimestamp = GenValue.Timestamp
  val ValueTimestamp: GenValue.Timestamp.type = GenValue.Timestamp

  type ValueDate = GenValue.Date
  val ValueDate: GenValue.Date.type = GenValue.Date

  type ValueParty = GenValue.Party
  val ValueParty: GenValue.Party.type = GenValue.Party

  type ValueBool = GenValue.Bool
  val ValueBool: GenValue.Bool.type = GenValue.Bool

  type ValueUnit = GenValue.Unit.type
  val ValueUnit: GenValue.Unit.type = GenValue.Unit

  import scalaz.syntax.traverse._
  import scalaz.std.either._
  import scalaz.std.option._

  // Casts from GenValue[Extension[From]] to GenValue[Nothing] i.e. `Value`
  def castExtendedValue[From](
      value: GenValue[GenValue.Extension[From]]
  ): Either[RuntimeException, GenValue[Nothing]] =
    value match {
      case _: GenValue.Blob[From] => Left(new RuntimeException("Illegal Blob in Value"))
      case _: GenValue.Any[From] => Left(new RuntimeException("Illegal Any in Value"))
      case _: GenValue.TypeRep[From] => Left(new RuntimeException("Illegal TypeRep in Value"))
      case ValueRecord(tycon, content) =>
        content
          .traverse { case (label, value) => castExtendedValue(value).map((label, _)) }
          .map(ValueRecord(tycon, _))
      case ValueVariant(tycon, variant, content) =>
        castExtendedValue(content).map(ValueVariant(tycon, variant, _))
      case ValueList(content) =>
        content.traverse(castExtendedValue(_)).map(ValueList(_))
      case ValueOptional(content) =>
        content.traverse(castExtendedValue(_)).map(ValueOptional(_))
      case ValueTextMap(content) =>
        content.traverse(castExtendedValue(_)).map(ValueTextMap(_))
      case ValueGenMap(content) =>
        content
          .traverse { case (key, value) =>
            for {
              pureKey <- castExtendedValue(key)
              pureValue <- castExtendedValue(value)
            } yield (pureKey, pureValue)
          }
          .map(ValueGenMap(_))
      case v: ValueEnum => Right(v)
      case v: ValueContractId => Right(v)
      case v: ValueInt64 => Right(v)
      case v: ValueNumeric => Right(v)
      case v: ValueText => Right(v)
      case v: ValueTimestamp => Right(v)
      case v: ValueDate => Right(v)
      case v: ValueParty => Right(v)
      case v: ValueBool => Right(v)
      case v: ValueUnit => Right(v)
    }

  class ValueArithmeticError(stablePackages: StablePackages) {
    val tyCon: TypeConId = stablePackages.ArithmeticError
    val typ: Ast.Type = Ast.TTyCon(tyCon)
    private val someTyCon = Some(tyCon)
    val fieldName: Ref.Name = Ref.Name.assertFromString("message")
    private val someFieldName = Some(fieldName)
    def apply(message: String): ValueRecord =
      ValueRecord(someTyCon, ImmArray(someFieldName -> ValueText(message)))
    def unapply(excep: ValueRecord): Option[String] =
      excep match {
        case ValueRecord(id, ImmArray((field, ValueText(message))))
            if id.forall(_ == tyCon) && field.forall(_ == fieldName) =>
          Some(message)
        case _ => None
      }
  }

  /** The data constructors of a variant or enum, if defined. */
  type LookupVariantEnum = Identifier => Option[ImmArray[Name]]

  /** A contract instance is a value plus the template that originated it. */
  // Prefer to use transaction.FatContractInstance
  final case class ThinContractInstance(
      packageName: Ref.PackageName,
      template: Identifier,
      arg: Value,
  ) extends CidContainer[ThinContractInstance] {

    def map(f: Value => Value): ThinContractInstance =
      copy(arg = f(arg))

    def mapCid(f: ContractId => ContractId): ThinContractInstance =
      copy(arg = arg.mapCid(f))
  }

  type VersionedThinContractInstance = transaction.Versioned[ThinContractInstance]

  object VersionedContractInstance {
    def apply(
        packageName: Ref.PackageName,
        template: Identifier,
        arg: VersionedValue,
    ): VersionedThinContractInstance =
      arg.map(ThinContractInstance(packageName, template, _))

    @deprecated("use the version with 3 argument", since = "2.9.0")
    def apply(
        version: transaction.SerializationVersion,
        packageName: Ref.PackageName,
        template: Identifier,
        arg: Value,
    ): VersionedThinContractInstance =
      transaction.Versioned(version, ThinContractInstance(packageName, template, arg))
  }

  type NodeIdx = Int

  sealed abstract class ContractId extends Product with Serializable with CidContainer[ContractId] {
    def coid: String
    def toBytes: Bytes
    def version: ContractIdVersion
    def isAbsolute: Boolean
    def isLocal: Boolean

    final override def mapCid(f: ContractId => ContractId): ContractId = f(this)
  }

  object ContractId {
    final case class V1 private (discriminator: crypto.Hash, suffix: Bytes)
        extends ContractId
        with data.NoCopy {
      override lazy val toBytes: Bytes = V1.prefix ++ discriminator.bytes ++ suffix
      lazy val coid: Ref.HexString = toBytes.toHexString
      override def toString: String = s"ContractId($coid)"
      override def version: ContractIdVersion = ContractIdVersion.V1
      override def isAbsolute: Boolean = suffix.nonEmpty
      override def isLocal: Boolean = !isAbsolute
    }

    object V1 {
      // For more details, please refer to  V1 Contract ID allocation scheme
      // daml-lf/spec/contract-id.rst

      private[lf] val MaxSuffixLength = 94

      def apply(discriminator: Hash): V1 = new V1(discriminator, Bytes.Empty)

      def build(discriminator: crypto.Hash, suffix: Bytes): Either[String, V1] =
        Either.cond(
          suffix.length <= MaxSuffixLength,
          new V1(discriminator, suffix),
          s"the suffix is too long, expected at most $MaxSuffixLength bytes, but got ${suffix.length}",
        )

      def assertBuild(discriminator: crypto.Hash, suffix: Bytes): V1 =
        assertRight(build(discriminator, suffix))

      val prefix: Bytes = Bytes.assertFromString("00")

      private val suffixStart: Int = crypto.Hash.underlyingHashLength + prefix.length

      def fromBytes(bytes: Bytes): Either[String, V1] =
        if (bytes.startsWith(prefix) && bytes.length >= suffixStart)
          crypto.Hash
            .fromBytes(bytes.slice(prefix.length, suffixStart))
            .flatMap(
              V1.build(_, bytes.slice(suffixStart, bytes.length))
            )
        else
          Left(s"""cannot parse V1 ContractId "${bytes.toHexString}"""")

      def fromString(s: String): Either[String, V1] =
        Bytes.fromString(s).flatMap(fromBytes)

      def assertFromString(s: String): V1 = assertRight(fromString(s))

      implicit val `V1 Order`: Order[V1] = {
        case (ContractId.V1(hash1, suffix1), ContractId.V1(hash2, suffix2)) =>
          hash1 ?|? hash2 |+| suffix1 ?|? suffix2
      }
    }

    final case class V2 private (local: Bytes, suffix: Bytes) extends ContractId with data.NoCopy {
      override lazy val toBytes: Bytes = V2.prefix ++ local ++ suffix
      lazy val coid: Ref.HexString = toBytes.toHexString
      override def toString: String = s"ContractId($coid)"
      override def version: ContractIdVersion = ContractIdVersion.V2

      def isRelative: Boolean = suffix.nonEmpty && suffix.toByteString.byteAt(0) >= 0
      override def isAbsolute: Boolean = suffix.nonEmpty && suffix.toByteString.byteAt(0) < 0
      override def isLocal: Boolean = suffix.isEmpty
    }

    object V2 {
      private[lf] def MaxSuffixLength: Int = 33

      val prefix: Bytes = Bytes.assertFromString("01")

      private[lf] val timePrefixSize: Int = 5
      private[lf] val discriminatorSize: Int = 7

      val localSize: Int = timePrefixSize + discriminatorSize

      private val suffixStart: Int = prefix.length + localSize

      def build(local: Bytes, suffix: Bytes): Either[String, V2] = {
        for {
          _ <- Either.cond(
            local.length == localSize,
            (),
            s"The local prefix has the wrong size, expected $localSize, but got ${local.length}",
          )
          _ <- Either.cond(
            suffix.length <= MaxSuffixLength,
            (),
            s"The suffix is too long, expected at most $MaxSuffixLength bytes, but got ${suffix.length}",
          )
        } yield new V2(local, suffix)
      }

      def assertBuild(local: Bytes, suffix: Bytes): V2 =
        assertRight(build(local, suffix))

      /** The largest integer `i` so that the number of microseconds between [[Time.Timestamp.MinValue]]
        * and [[Time.Timestamp.MaxValue]] divided by `i` is smaller than `2^40-1` and therefore fits into 5 bytes.
        */
      private[lf] val resolution: Long = 286981L

      private[lf] def timePrefix(time: Time.Timestamp): Bytes = {
        // Even if this overflows, it will not matter as we're using unsigned division below.
        val microsRebased = time.micros - Time.Timestamp.MinValue.micros
        val scaled = java.lang.Long.divideUnsigned(microsRebased, resolution)
        val byteBuffer = ByteBuffer.allocate(timePrefixSize).order(ByteOrder.BIG_ENDIAN)
        // This implementation hard-codes the time prefix size of 5 bytes.
        // It does not seem worth to find an implementation that is generic in the size of the time prefix here.
        discard(byteBuffer.put((scaled >> 32).toByte).putInt(scaled.toInt))
        Bytes.fromByteBuffer(byteBuffer.position(0))
      }

      private[lf] def discriminator(nodeSeed: crypto.Hash): Bytes =
        nodeSeed.bytes.slice(0, discriminatorSize)

      def unsuffixed(time: Time.Timestamp, nodeSeed: crypto.Hash): V2 =
        new V2(timePrefix(time) ++ discriminator(nodeSeed), Bytes.Empty)

      private[lf] def suffixed(
          time: Time.Timestamp,
          nodeSeed: crypto.Hash,
          suffix: Bytes,
      ): Either[String, V2] =
        build(timePrefix(time) ++ discriminator(nodeSeed), suffix)

      private[lf] def assertSuffixed(
          time: Time.Timestamp,
          nodeSeed: crypto.Hash,
          suffix: Bytes,
      ): V2 =
        assertRight(suffixed(time, nodeSeed, suffix))

      def fromBytes(bytes: Bytes): Either[String, V2] =
        if (bytes.startsWith(prefix) && bytes.length >= suffixStart) {
          val local = bytes.slice(prefix.length, suffixStart)
          val suffix = bytes.slice(suffixStart, bytes.length)
          build(local, suffix)
        } else
          Left(s"""cannot parse V2 ContractId "${bytes.toHexString}"""")

      def fromString(s: String): Either[String, V2] =
        Bytes.fromString(s).flatMap(fromBytes)

      def assertFromString(s: String): V2 = assertRight(fromString(s))

      implicit val `V2 Order`: Order[V2] = {
        case (ContractId.V2(local1, suffix1), ContractId.V2(local2, suffix2)) =>
          local1 ?|? local2 |+| suffix1 ?|? suffix2
      }
    }

    def fromString(s: String): Either[String, ContractId] =
      V2.fromString(s)
        .orElse(
          V1.fromString(s)
            .left
            .map(_ => s"""cannot parse ContractId "$s"""")
        )

    def assertFromString(s: String): ContractId =
      assertRight(fromString(s))

    def fromBytes(bytes: Bytes): Either[String, ContractId] =
      if (bytes.startsWith(V2.prefix)) V2.fromBytes(bytes)
      else if (bytes.startsWith(V1.prefix)) V1.fromBytes(bytes)
      else Left(s"cannot parse ContractId: unknown version prefix ${bytes.slice(0, 1).toHexString}")

    implicit val `Cid Order`: Order[ContractId] = new Order[ContractId] {
      override def order(a: ContractId, b: ContractId): scalaz.Ordering =
        (a, b) match {
          case (a: V2, b: V2) => a ?|? b
          case (a: V1, b: V1) => a ?|? b
          case (_: V1, _: V2) => scalaz.Ordering.LT
          case (_: V2, _: V1) => scalaz.Ordering.GT
        }

      override def equal(a: ContractId, b: ContractId): Boolean =
        (a, b).match2 {
          case V1(discA, suffA) => { case V1(discB, suffB) =>
            discA == discB && suffA.toByteString == suffB.toByteString
          }
          case V2(localA, suffA) => { case V2(localB, suffB) =>
            localA == localB && suffA.toByteString == suffB.toByteString
          }
        }(fallback = false)
    }

    implicit val equalInstance: Equal[ContractId] = Equal.equalA
  }

  val ValueTrue: ValueBool = ValueBool.True
  val ValueFalse: ValueBool = ValueBool.False
  val ValueNil: ValueList = ValueList(FrontStack.empty)
  val ValueNone: ValueOptional = ValueOptional(None)
}

sealed trait ContractIdVersion extends Product with Serializable
object ContractIdVersion {
  case object V1 extends ContractIdVersion
  case object V2 extends ContractIdVersion
}
