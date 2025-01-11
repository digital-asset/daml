// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.Ref.{Identifier, Name, TypeConName}
import com.digitalasset.daml.lf.data._
import com.digitalasset.daml.lf.language.{Ast, StablePackages}
import data.ScalazEqual._
import scalaz.{Equal, Order}
import scalaz.syntax.order._
import scalaz.syntax.semigroup._

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
  final case class ContractInstance(
      packageName: Ref.PackageName,
      packageVersion: Option[Ref.PackageVersion] = None,
      template: Identifier,
      arg: Value,
  ) extends CidContainer[ContractInstance] {

    override protected def self: this.type = this

    def map(f: Value => Value): ContractInstance =
      copy(arg = f(arg))

    def mapCid(f: ContractId => ContractId): ContractInstance =
      copy(arg = arg.mapCid(f))
  }

  type VersionedContractInstance = transaction.Versioned[ContractInstance]

  object VersionedContractInstance {
    def apply(
        packageName: Ref.PackageName,
        packageVersion: Option[Ref.PackageVersion],
        template: Identifier,
        arg: VersionedValue,
    ): VersionedContractInstance =
      arg.map(ContractInstance(packageName, packageVersion, template, _))

    @deprecated("use the version with 3 argument", since = "2.9.0")
    def apply(
        version: transaction.TransactionVersion,
        packageName: Ref.PackageName,
        packageVersion: Option[Ref.PackageVersion],
        template: Identifier,
        arg: Value,
    ): VersionedContractInstance =
      transaction.Versioned(version, ContractInstance(packageName, packageVersion, template, arg))
  }

  type NodeIdx = Int

  sealed abstract class ContractId extends Product with Serializable {
    def coid: String
    def toBytes: Bytes
  }

  object ContractId {
    final case class V1 private (discriminator: crypto.Hash, suffix: Bytes)
        extends ContractId
        with data.NoCopy {
      override lazy val toBytes: Bytes = V1.prefix ++ discriminator.bytes ++ suffix
      lazy val coid: Ref.HexString = toBytes.toHexString
      override def toString: String = s"ContractId($coid)"
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

    def fromString(s: String): Either[String, ContractId] =
      V1.fromString(s)
        .left
        .map(_ => s"""cannot parse ContractId "$s"""")

    def assertFromString(s: String): ContractId =
      assertRight(fromString(s))

    def fromBytes(bytes: Bytes): Either[String, ContractId] = V1.fromBytes(bytes)

    implicit val `Cid Order`: Order[ContractId] = new Order[ContractId] {
      override def order(a: ContractId, b: ContractId) =
        (a, b) match {
          case (a: V1, b: V1) => a ?|? b
        }

      override def equal(a: ContractId, b: ContractId) =
        (a, b).match2 {
          case V1(discA, suffA) => { case V1(discB, suffB) =>
            discA == discB && suffA.toByteString == suffB.toByteString
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
