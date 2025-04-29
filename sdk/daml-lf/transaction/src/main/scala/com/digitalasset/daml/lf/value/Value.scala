// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.daml.scalautil.Statement.discard
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name, TypeConName}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{Ast, StablePackages}
import data.ScalazEqual._
import scalaz.{Equal, Order}
import scalaz.syntax.order._
import scalaz.syntax.semigroup._

import java.nio.{ByteBuffer, ByteOrder}

/** Values */
sealed abstract class Value extends CidContainer[Value] with Product with Serializable {
  import Value._

  final override protected def self: this.type = this

  final override def mapCid(f: ContractId => ContractId): Value = {
    def go(v0: Value): Value =
      // TODO (FM) make this tail recursive
      v0 match {
        case ValueContractId(coid) => ValueContractId(f(coid))
        case ValueRecord(id, fs) =>
          ValueRecord(
            id,
            fs.map({ case (lbl, value) =>
              (lbl, go(value))
            }),
          )
        case ValueVariant(id, variant, value) =>
          ValueVariant(id, variant, go(value))
        case x: ValueCidlessLeaf => x
        case ValueList(vs) =>
          ValueList(vs.map(go))
        case ValueOptional(x) => ValueOptional(x.map(go))
        case ValueTextMap(x) => ValueTextMap(x.mapValue(go))
        case ValueGenMap(entries) =>
          ValueGenMap(entries.map { case (k, v) => go(k) -> go(v) })
      }
    go(this)
  }

  // TODO (FM) make this tail recursive
  private[lf] def foreach1(f: ContractId => Unit): Value => Unit = {
    def go(v0: Value): Unit =
      v0 match {
        case ValueContractId(coid) =>
          f(coid)
        case ValueRecord(id @ _, fs) =>
          fs.foreach { case (_, value) => go(value) }
        case ValueVariant(id @ _, variant @ _, value) =>
          go(value)
        case _: ValueCidlessLeaf =>
        case ValueList(vs) =>
          vs.foreach(go)
        case ValueOptional(x) =>
          x.foreach(go)
        case ValueTextMap(x) =>
          x.foreach { case (_, value) => go(value) }
        case ValueGenMap(entries) =>
          entries.foreach { case (k, v) => go(k); go(v) }
      }

    go
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

  /** The parent of all [[Value]] cases that cannot possibly have a Cid.
    * NB: use only in pattern-matching [[Value]]; the ''type'' of a cid-less
    * Value is `Value[Nothing]`.
    */
  sealed abstract class ValueCidlessLeaf extends Value

  final case class ValueRecord(
      tycon: Option[Identifier],
      fields: ImmArray[(Option[Name], Value)],
  ) extends Value

  class ValueArithmeticError(stablePackages: StablePackages) {
    val tyCon: TypeConName = stablePackages.ArithmeticError
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

  final case class ValueVariant(tycon: Option[Identifier], variant: Name, value: Value)
      extends Value
  final case class ValueEnum(tycon: Option[Identifier], value: Name) extends ValueCidlessLeaf

  final case class ValueContractId(value: ContractId) extends Value

  /** Daml-LF lists are basically linked lists. However we use FrontQueue since we store list-literals in the Daml-LF
    * packages and FrontQueue lets prepend chunks rather than only one element.
    */
  final case class ValueList(values: FrontStack[Value]) extends Value
  final case class ValueInt64(value: Long) extends ValueCidlessLeaf
  final case class ValueNumeric(value: Numeric) extends ValueCidlessLeaf
  // Note that Text are assume to be UTF8
  final case class ValueText(value: String) extends ValueCidlessLeaf
  final case class ValueTimestamp(value: Time.Timestamp) extends ValueCidlessLeaf
  final case class ValueDate(value: Time.Date) extends ValueCidlessLeaf
  final case class ValueParty(value: Ref.Party) extends ValueCidlessLeaf
  final case class ValueBool(value: Boolean) extends ValueCidlessLeaf
  object ValueBool {
    val True = new ValueBool(true)
    val False = new ValueBool(false)
    def apply(value: Boolean): ValueBool =
      if (value) ValueTrue else ValueFalse
  }
  case object ValueUnit extends ValueCidlessLeaf

  final case class ValueOptional(value: Option[Value]) extends Value
  final case class ValueTextMap(value: SortedLookupList[Value]) extends Value
  final case class ValueGenMap(entries: ImmArray[(Value, Value)]) extends Value {
    override def toString: String = entries.iterator.mkString("ValueGenMap(", ",", ")")
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

    override protected def self: this.type = this

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
        version: transaction.TransactionVersion,
        packageName: Ref.PackageName,
        template: Identifier,
        arg: Value,
    ): VersionedThinContractInstance =
      transaction.Versioned(version, ThinContractInstance(packageName, template, arg))
  }

  type NodeIdx = Int

  sealed abstract class ContractId extends Product with Serializable {
    def coid: String
    def toBytes: Bytes
    def version: ContractIdVersion
  }

  object ContractId {
    final case class V1 private (discriminator: crypto.Hash, suffix: Bytes)
        extends ContractId
        with data.NoCopy {
      override lazy val toBytes: Bytes = V1.prefix ++ discriminator.bytes ++ suffix
      lazy val coid: Ref.HexString = toBytes.toHexString
      override def toString: String = s"ContractId($coid)"
      override def version: ContractIdVersion = ContractIdVersion.V1
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
      def isAbsolute: Boolean = suffix.nonEmpty && suffix.toByteString.byteAt(0) < 0
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
