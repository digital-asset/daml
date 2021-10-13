// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{Identifier, Name}
import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.transaction.TransactionVersion
import com.daml.scalautil.Statement.discard
import data.ScalazEqual._

import scalaz.{@@, Equal, Order, Tag}
import scalaz.Ordering.EQ
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.order._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._

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

  def cids[Cid2 >: ContractId] = {
    val cids = Set.newBuilder[Cid2]
    foreach1(x => discard(cids += x))(this)
    cids.result()
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

  final case class VersionedValue(version: TransactionVersion, value: Value)
      extends CidContainer[VersionedValue] {

    override protected def self: this.type = this

    final override def mapCid(f: ContractId => ContractId): VersionedValue =
      copy(value = value.mapCid(f))

    def foreach1(f: ContractId => Unit) =
      value.foreach1(f)

    def cids[Cid2 >: ContractId]: Set[Cid2] = value.cids
  }

  object VersionedValue {
    implicit def `VersionedValue Equal instance`: Equal[VersionedValue] =
      ScalazEqual.withNatural(Equal[ContractId].equalIsNatural) { (a, b) =>
        import a._
        val VersionedValue(bVersion, bValue) = b
        version == bVersion && value === bValue
      }
  }

  /** The parent of all [[Value]] cases that cannot possibly have a Cid.
    * NB: use only in pattern-matching [[Value]]; the ''type'' of a cid-less
    * Value is `Value[Nothing]`.
    */
  sealed abstract class ValueCidlessLeaf extends Value

  final case class ValueRecord(
      tycon: Option[Identifier],
      fields: ImmArray[(Option[Name], Value)],
  ) extends Value

  object ValueArithmeticError {
    // The package ID should match the ID of the stable package daml-prim-DA-Exception-ArithmeticError
    // See test compiler/damlc/tests/src/stable-packages.sh
    val tyCon: Ref.TypeConName = Ref.Identifier.assertFromString(
      "cb0552debf219cc909f51cbb5c3b41e9981d39f8f645b1f35e2ef5be2e0b858a:DA.Exception.ArithmeticError:ArithmeticError"
    )
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

  /** This comparison assumes that you are comparing values of matching type,
    * and, like the lf-value-json decoder, all variants and enums contain
    * their identifier.  Moreover, the `Scope` must include all of those
    * identifiers.
    */
  def orderInstance(Scope: LookupVariantEnum): Order[Value @@ Scope.type] =
    Tag.subst(new `Value Order instance`(Scope): Order[Value])

  // Order of GenMap entries is relevant for this equality.
  implicit val `Value Equal instance`: Equal[Value] =
    new `Value Equal instance`

  /** A contract instance is a value plus the template that originated it. */
  final case class ContractInst[+Val](
      template: Identifier,
      arg: Val,
      agreementText: String,
  ) {

    def map[Val2](f: Val => Val2): ContractInst[Val2] =
      copy(arg = f(arg))
  }

  object ContractInst {

    implicit class CidContainerInstance[Val <: value.CidContainer[Val]](inst: ContractInst[Val])
        extends value.CidContainer[ContractInst[Val]] {
      override protected def self = inst
      final override def mapCid(f: ContractId => ContractId): ContractInst[Val] =
        inst.copy(arg = inst.arg.mapCid(f))
    }

    implicit def equalInstance[Val: Equal]: Equal[ContractInst[Val]] =
      ScalazEqual.withNatural(Equal[Val].equalIsNatural) { (a, b) =>
        import a._
        val ContractInst(bTemplate, bArg, bAgreementText) = b
        template == bTemplate && arg === bArg && agreementText == bAgreementText
      }
  }

  final case class VersionedContractInstance(
      version: TransactionVersion,
      template: Identifier,
      arg: Value,
      agreementText: String,
  ) {
    def coinst = ContractInst(template, arg, agreementText)
    def versionedArg = VersionedValue(version, arg)
  }

  object VersionedContractInstance {
    def apply(version: TransactionVersion, coinst: ContractInst[Value]): VersionedContractInstance =
      VersionedContractInstance(version, coinst.template, coinst.arg, coinst.agreementText)
    def apply(
        template: Identifier,
        arg: VersionedValue,
        agreementText: String,
    ): VersionedContractInstance =
      VersionedContractInstance(arg.version, template, arg.value, agreementText)
  }

  type NodeIdx = Int

  sealed abstract class ContractId extends Product with Serializable {
    def coid: String
  }

  object ContractId {
    final case class V0(coid: Ref.ContractIdString) extends ContractId {
      override def toString: String = s"ContractId($coid)"
    }

    object V0 {
      def fromString(s: String): Either[String, V0] =
        Ref.ContractIdString.fromString(s).map(V0(_))

      def assertFromString(s: String): V0 = assertRight(fromString(s))

      implicit val `V0 Order`: Order[V0] = Order.fromScalaOrdering[String] contramap (_.coid)
    }

    final case class V1 private (discriminator: crypto.Hash, suffix: Bytes)
        extends ContractId
        with data.NoCopy {
      lazy val toBytes: Bytes = V1.prefix ++ discriminator.bytes ++ suffix
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

      def fromString(s: String): Either[String, V1] =
        Bytes
          .fromString(s)
          .flatMap(bytes =>
            if (bytes.startsWith(prefix) && bytes.length >= suffixStart)
              crypto.Hash
                .fromBytes(bytes.slice(prefix.length, suffixStart))
                .flatMap(
                  V1.build(_, bytes.slice(suffixStart, bytes.length))
                )
            else
              Left(s"""cannot parse V1 ContractId "$s"""")
          )

      def assertFromString(s: String): V1 = assertRight(fromString(s))

      implicit val `V1 Order`: Order[V1] = {
        case (ContractId.V1(hash1, suffix1), ContractId.V1(hash2, suffix2)) =>
          hash1 ?|? hash2 |+| suffix1 ?|? suffix2
      }

    }

    def fromString(s: String): Either[String, ContractId] =
      V1.fromString(s)
        .left
        .flatMap(_ => V0.fromString(s))
        .left
        .map(_ => s"""cannot parse ContractId "$s"""")

    def assertFromString(s: String): ContractId =
      assertRight(fromString(s))

    implicit val `Cid Order`: Order[ContractId] = new Order[ContractId] {
      import scalaz.Ordering._
      override def order(a: ContractId, b: ContractId) =
        (a, b) match {
          case (a: V0, b: V0) => a ?|? b
          case (a: V1, b: V1) => a ?|? b
          case (_: V0, _: V1) => LT
          case (_: V1, _: V0) => GT
        }

      override def equal(a: ContractId, b: ContractId) =
        (a, b).match2 {
          case a: V0 => { case b: V0 => a === b }
          case V1(discA, suffA) => { case V1(discB, suffB) =>
            discA == discB && suffA.toByteString == suffB.toByteString
          }
        }(fallback = false)
    }

    implicit val equalInstance: Equal[ContractId] = Equal.equalA
  }

  /** * Keys cannot contain contract ids
    */
  type Key = Value

  val ValueTrue: ValueBool = ValueBool.True
  val ValueFalse: ValueBool = ValueBool.False
  val ValueNil: ValueList = ValueList(FrontStack.empty)
  val ValueNone: ValueOptional = ValueOptional(None)
}

/** This Order is not strictly compatible with the Equal instance, so
  * instances of it must always be local or tagged.
  */
private final class `Value Order instance`(Scope: Value.LookupVariantEnum) extends Order[Value] {
  import Value._, Utf8.ImplicitOrder._
  import scalaz.std.anyVal._
  import ScalazEqual._2, _2._

  implicit final def Self: this.type = this

  override final def order(a: Value, b: Value) = {
    val (ixa, cmp) = ixv(a)
    cmp.applyOrElse(
      b,
      { b: Value =>
        val (ixb, _) = ixv(b)
        ixa ?|? ixb
      },
    )
  }

  private[this] def ixv(a: Value) = a match {
    case ValueUnit => (0, k { case ValueUnit => EQ })
    case ValueBool(a) => (10, k { case ValueBool(b) => a ?|? b })
    case ValueInt64(a) => (20, k { case ValueInt64(b) => a ?|? b })
    case ValueText(a) => (30, k { case ValueText(b) => a ?|? b })
    case ValueNumeric(a) => (40, k { case ValueNumeric(b) => a ?|? b })
    case ValueTimestamp(a) => (50, k { case ValueTimestamp(b) => a ?|? b })
    case ValueDate(a) => (60, k { case ValueDate(b) => a ?|? b })
    case ValueParty(a) => (70, k { case ValueParty(b) => a ?|? b })
    case ValueContractId(a) => (80, k { case ValueContractId(b) => a ?|? b })
    case ValueOptional(a) => (90, k { case ValueOptional(b) => a ?|? b })
    case ValueList(a) => (100, k { case ValueList(b) => a ?|? b })
    case ValueTextMap(a) => (110, k { case ValueTextMap(b) => a ?|? b })
    case ValueGenMap(a) => (120, k { case ValueGenMap(b) => a ?|? b })
    case ValueEnum(idA, a) =>
      (
        130,
        k { case ValueEnum(idB, b) =>
          ctorOrder(idA, idB, a, b)
        },
      )
    case ValueRecord(_, a) => (140, k { case ValueRecord(_, b) => _2.T.subst(a) ?|? _2.T.subst(b) })
    case ValueVariant(idA, conA, a) =>
      (
        160,
        k { case ValueVariant(idB, conB, b) =>
          ctorOrder(idA, idB, conA, conB) |+| a ?|? b
        },
      )
  }

  private[this] def ctorOrder(
      idA: Option[Identifier],
      idB: Option[Identifier],
      ctorA: Ref.Name,
      ctorB: Ref.Name,
  ) = {
    val idAB = unifyIds(idA, idB)
    Scope(idAB).cata(
      { ctors =>
        val lookup = ctors.toSeq
        (lookup indexOf ctorA) ?|? (lookup indexOf ctorB)
      },
      noType(idAB),
    )
  }
  @throws[IllegalArgumentException]
  private[this] def noType(id: Identifier): Nothing =
    throw new IllegalArgumentException(s"type $id not found in scope")

  // could possibly return Ordering \/ Id instead of throwing in the Some/Some case,
  // not sure if that could apply to None/None...
  @throws[IllegalArgumentException]
  private[this] def unifyIds[Id <: Identifier](a: Option[Id], b: Option[Id]): Id = (a, b) match {
    case (Some(idA), Some(idB)) =>
      if (idA != idB) throw new IllegalArgumentException(s"types $idA and $idB don't match")
      else idA
    case _ => a orElse b getOrElse (throw new IllegalArgumentException("missing type identifier"))
  }

  private[this] def k[Z](f: Value PartialFunction Z): f.type = f
}

private final class `Value Equal instance` extends Equal[Value] {
  import Value._
  import ScalazEqual._

  override final def equalIsNatural: Boolean = Equal[ContractId].equalIsNatural

  implicit final def Self: this.type = this

  override final def equal(a: Value, b: Value) =
    (a, b).match2 {
      case a @ (_: ValueInt64 | _: ValueNumeric | _: ValueText | _: ValueTimestamp | _: ValueParty |
          _: ValueBool | _: ValueDate | ValueUnit) => { case b => a == b }
      case r: ValueRecord => { case ValueRecord(tycon2, fields2) =>
        import r._
        tycon == tycon2 && fields === fields2
      }
      case v: ValueVariant => { case ValueVariant(tycon2, variant2, value2) =>
        import v._
        tycon == tycon2 && variant == variant2 && value === value2
      }
      case v: ValueEnum => { case ValueEnum(tycon2, value2) =>
        import v._
        tycon == tycon2 && value == value2
      }
      case ValueContractId(value) => { case ValueContractId(value2) =>
        value === value2
      }
      case ValueList(values) => { case ValueList(values2) =>
        values === values2
      }
      case ValueOptional(value) => { case ValueOptional(value2) =>
        value === value2
      }
      case ValueTextMap(map1) => { case ValueTextMap(map2) =>
        map1 === map2
      }
      case genMap: ValueGenMap => { case ValueGenMap(entries2) =>
        genMap.entries === entries2
      }
    }(fallback = false)
}
