// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package value

import com.daml.lf.crypto.Hash
import com.daml.lf.data.Ref.{Identifier, Name}
import com.daml.lf.data._
import data.ScalazEqual._
import com.daml.lf.language.LanguageVersion

import scala.annotation.tailrec
import scalaz.{@@, Equal, Order, Tag}
import scalaz.Ordering.EQ
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.order._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._

/** Values   */
sealed abstract class Value[+Cid] extends CidContainer[Value[Cid]] with Product with Serializable {
  import Value._

  final override protected val self: this.type = this

  final def mapContractId[Cid2](f: Cid => Cid2): Value[Cid2] =
    map1(f)

  private[lf] final def map1[Cid2](f: Cid => Cid2): Value[Cid2] =
    Value.map1(f)(this)

  /** returns a list of validation errors: if the result is non-empty the value is
    * _not_ serializable.
    *
    * note that this does not check the validity of the [[Identifier]]s, it just checks
    * that the shape of the value is serializable.
    */
  def serializable(): ImmArray[String] = {
    @tailrec
    def go(
        exceededNesting: Boolean,
        errs: BackStack[String],
        vs0: FrontStack[(Value[Cid], Int)],
    ): BackStack[String] = vs0 match {
      case FrontStack() => errs

      case FrontStackCons((v, nesting), vs) =>
        // we cannot define helper functions because otherwise go is not tail recursive. fun!
        val exceedsNestingErr = s"exceeds maximum nesting value of $MAXIMUM_NESTING"
        val newNesting = nesting + 1

        v match {
          case tpl: ValueStruct[Cid] =>
            go(exceededNesting, errs :+ s"contains struct $tpl", vs)
          case ValueRecord(_, flds) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, flds.map(v => (v._2, newNesting)) ++: vs)
            }

          case ValueList(values) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, values.toImmArray.map(v => (v, newNesting)) ++: vs)
            }

          case ValueVariant(_, _, value) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, (value, newNesting) +: vs)
            }

          case _: ValueCidlessLeaf | _: ValueContractId[Cid] =>
            go(exceededNesting, errs, vs)
          case ValueOptional(x) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, ImmArray(x.toList.map(v => (v, newNesting))) ++: vs)
            }
          case ValueTextMap(value) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              go(exceededNesting, errs, value.values.map(v => (v, newNesting)) ++: vs)
            }
          case ValueGenMap(entries) =>
            if (newNesting > MAXIMUM_NESTING) {
              if (exceededNesting) {
                // we already exceeded the nesting, do not output again
                go(exceededNesting, errs, vs)
              } else {
                go(true, errs :+ exceedsNestingErr, vs)
              }
            } else {
              val vs1 = entries.foldLeft(vs) {
                case (acc, (k, v)) => (k -> newNesting) +: (v -> newNesting) +: acc
              }
              go(exceededNesting, errs, vs1)
            }
        }
    }

    go(false, BackStack.empty, FrontStack((this, 0))).toImmArray
  }

  def foreach1(f: Cid => Unit) =
    Value.foreach1(f)(self)

  def cids[Cid2 >: Cid] = {
    val cids = Set.newBuilder[Cid2]
    foreach1(cids += _)
    cids.result()
  }

}

object Value extends CidContainer1WithDefaultCidResolver[Value] {

  // TODO (FM) make this tail recursive
  private[lf] override def map1[Cid, Cid2](f: Cid => Cid2): Value[Cid] => Value[Cid2] = {
    def go(v0: Value[Cid]): Value[Cid2] =
      // TODO (FM) make this tail recursive
      v0 match {
        case ValueContractId(coid) => ValueContractId(f(coid))
        case ValueRecord(id, fs) =>
          ValueRecord(id, fs.map({
            case (lbl, value) => (lbl, go(value))
          }))
        case ValueStruct(fs) =>
          ValueStruct(fs.map[(Name, Value[Cid2])] {
            case (lbl, value) => (lbl, go(value))
          })
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

    go
  }

  private[lf] override def foreach1[Cid](f: Cid => Unit): Value[Cid] => Unit = {

    def go(v0: Value[Cid]): Unit =
      v0 match {
        case ValueContractId(coid) =>
          f(coid)
        case ValueRecord(id @ _, fs) =>
          fs.foreach { case (_, value) => go(value) }
        case ValueStruct(fs) =>
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

  /** the maximum nesting level for DAML-LF serializable values. we put this
    * limitation to be able to reliably implement stack safe programs with it.
    * right now it's 100 to be conservative -- it's in the same order of magnitude
    * as the default maximum nesting value of protobuf.
    *
    * encoders and decoders should check this to make sure values do not exceed
    * this level of nesting.
    */
  val MAXIMUM_NESTING: Int = 100

  final case class VersionedValue[+Cid](version: ValueVersion, value: Value[Cid])
      extends CidContainer[VersionedValue[Cid]] {

    override protected val self: this.type = this

    @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCidd", since = "0.13.52")
    def mapContractId[Cid2](f: Cid => Cid2): VersionedValue[Cid2] =
      map1(f)

    private[lf] def map1[Cid2](f: Cid => Cid2): VersionedValue[Cid2] =
      VersionedValue.map1(f)(this)

    /** Increase the `version` if appropriate for `languageVersions`. */
    def typedBy(languageVersions: LanguageVersion*): VersionedValue[Cid] = {
      import com.daml.lf.transaction.VersionTimeline, VersionTimeline._, Implicits._
      copy(version =
        latestWhenAllPresent(version, languageVersions map (a => a: SpecifiedVersion): _*))
    }

    def foreach1(f: Cid => Unit) =
      VersionedValue.foreach1(f)(self)

    def cids[Cid2 >: Cid]: Set[Cid2] = value.cids
  }

  object VersionedValue extends CidContainer1[VersionedValue] {
    implicit def `VersionedValue Equal instance`[Cid: Equal]: Equal[VersionedValue[Cid]] =
      ScalazEqual.withNatural(Equal[Cid].equalIsNatural) { (a, b) =>
        import a._
        val VersionedValue(bVersion, bValue) = b
        version == bVersion && value === bValue
      }

    override private[lf] def map1[A, B](f: A => B): VersionedValue[A] => VersionedValue[B] =
      x => x.copy(value = Value.map1(f)(x.value))

    final implicit def cidResolverInstance[A1, A2](
        implicit mapper1: CidMapper.RelCidResolver[A1, A2],
    ): CidMapper.RelCidResolver[VersionedValue[A1], VersionedValue[A2]] =
      cidMapperInstance

    override private[lf] def foreach1[A](f: A => Unit): VersionedValue[A] => Unit =
      x => Value.foreach1(f)(x.value)
  }

  /** The parent of all [[Value]] cases that cannot possibly have a Cid.
    * NB: use only in pattern-matching [[Value]]; the ''type'' of a cid-less
    * Value is `Value[Nothing]`.
    */
  sealed abstract class ValueCidlessLeaf extends Value[Nothing]

  final case class ValueRecord[+Cid](
      tycon: Option[Identifier],
      fields: ImmArray[(Option[Name], Value[Cid])],
  ) extends Value[Cid]
  final case class ValueVariant[+Cid](tycon: Option[Identifier], variant: Name, value: Value[Cid])
      extends Value[Cid]
  final case class ValueEnum(tycon: Option[Identifier], value: Name) extends ValueCidlessLeaf

  final case class ValueContractId[+Cid](value: Cid) extends Value[Cid]

  /**
    * DAML-LF lists are basically linked lists. However we use FrontQueue since we store list-literals in the DAML-LF
    * packages and FrontQueue lets prepend chunks rather than only one element.
    */
  final case class ValueList[+Cid](values: FrontStack[Value[Cid]]) extends Value[Cid]
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
  final case class ValueOptional[+Cid](value: Option[Value[Cid]]) extends Value[Cid]
  final case class ValueTextMap[+Cid](value: SortedLookupList[Value[Cid]]) extends Value[Cid]
  final case class ValueGenMap[+Cid](entries: ImmArray[(Value[Cid], Value[Cid])]) extends Value[Cid]
  // this is present here just because we need it in some internal code --
  // specifically the scenario interpreter converts committed values to values and
  // currently those can be structs, although we should probably ban that.
  final case class ValueStruct[+Cid](fields: ImmArray[(Name, Value[Cid])]) extends Value[Cid]

  /** The data constructors of a variant or enum, if defined. */
  type LookupVariantEnum = Identifier => Option[ImmArray[Name]]

  /** This comparison assumes that you are comparing values of matching type,
    * and, like the lf-value-json decoder, all variants and enums contain
    * their identifier.  Moreover, the `Scope` must include all of those
    * identifiers.
    */
  def orderInstance[Cid: Order](Scope: LookupVariantEnum): Order[Value[Cid] @@ Scope.type] =
    Tag.subst(new `Value Order instance`(Scope): Order[Value[Cid]])

  // Order of GenMap entries is relevant for this equality.
  implicit def `Value Equal instance`[Cid: Equal]: Equal[Value[Cid]] =
    new `Value Equal instance`

  /** A contract instance is a value plus the template that originated it. */
  final case class ContractInst[+Val](template: Identifier, arg: Val, agreementText: String)
      extends value.CidContainer[ContractInst[Val]] {

    override protected val self: ContractInst[Val] = this

    @deprecated("use resolveRelCid/ensureNoCid/ensureNoRelCid", since = "0.13.52")
    def mapValue[Val2](f: Val => Val2): ContractInst[Val2] =
      ContractInst.map1(f)(this)

  }

  object ContractInst extends CidContainer1WithDefaultCidResolver[ContractInst] {
    implicit def equalInstance[Val: Equal]: Equal[ContractInst[Val]] =
      ScalazEqual.withNatural(Equal[Val].equalIsNatural) { (a, b) =>
        import a._
        val ContractInst(bTemplate, bArg, bAgreementText) = b
        template == bTemplate && arg === bArg && agreementText == bAgreementText
      }

    override private[lf] def map1[A, B](f: A => B): ContractInst[A] => ContractInst[B] =
      x => x.copy(arg = f(x.arg))

    override private[lf] def foreach1[A](f: A => Unit): ContractInst[A] => Unit =
      x => f(x.arg)
  }

  type NodeIdx = Int

  /** Possibly relative contract identifiers.
    *
    * The contract identifiers can be either absolute, referring to a
    * specific instance in the contract store, or relative, referring
    * to a contract created in the same transaction and hence not yet
    * having been assigned an absolute identifier.
    *
    * Note that relative contract ids are useful only before commit, in
    * the context of a transaction. After committing we should never
    * mention them.
    *
    * Why put it here and not just in Transaction.scala? Because we want
    * to be able to use AbsoluteContractId elsewhere, so that we can
    * automatically upcast to ContractId by subtyping.
    */
  sealed abstract class ContractId extends Product with Serializable

  sealed abstract class AbsoluteContractId extends ContractId {
    def coid: String
  }

  object AbsoluteContractId {
    final case class V0(coid: Ref.ContractIdString) extends AbsoluteContractId {
      override def toString: String = s"AbsoluteContractId($coid)"
    }

    object V0 {
      def fromString(s: String): Either[String, V0] =
        Ref.ContractIdString.fromString(s).map(V0(_))

      def assertFromString(s: String): V0 = assertRight(fromString(s))

      implicit val `V0 Order`: Order[V0] = Order.fromScalaOrdering[String] contramap (_.coid)
    }

    final case class V1 private (discriminator: crypto.Hash, suffix: Bytes)
        extends AbsoluteContractId
        with data.NoCopy {
      lazy val toBytes: Bytes = V1.prefix ++ discriminator.bytes ++ suffix
      lazy val coid: Ref.HexString = toBytes.toHexString
      override def toString: String = s"AbsoluteContractId($coid)"
    }

    object V1 {
      val maxSuffixLength = 94

      def apply(discriminator: Hash): V1 = new V1(discriminator, Bytes.Empty)

      def build(discriminator: crypto.Hash, suffix: Bytes): Either[String, V1] =
        Either.cond(
          suffix.length <= maxSuffixLength,
          new V1(discriminator, suffix),
          s"the suffix is too long, expected at most ${maxSuffixLength} bytes, but got ${suffix.length}"
        )

      def assertBuild(discriminator: crypto.Hash, suffix: Bytes): V1 =
        assertRight(build(discriminator, suffix))

      val prefix: Bytes = Bytes.assertFromString("00")

      private val suffixStart: Int = crypto.Hash.underlyingHashLength + prefix.length

      def fromString(s: String): Either[String, V1] =
        Bytes
          .fromString(s)
          .flatMap(
            bytes =>
              if (bytes.startsWith(prefix) && bytes.length >= suffixStart)
                crypto.Hash
                  .fromBytes(bytes.slice(prefix.length, suffixStart))
                  .flatMap(
                    V1.build(_, bytes.slice(suffixStart, bytes.length))
                  )
              else
                Left(s"""cannot parse V1 ContractId "$s""""))

      def assertFromString(s: String): V1 = assertRight(fromString(s))

      implicit val `V1 Order`: Order[V1] = {
        case (AbsoluteContractId.V1(hash1, suffix1), AbsoluteContractId.V1(hash2, suffix2)) =>
          hash1 ?|? hash2 |+| suffix1 ?|? suffix2
      }

    }

    def fromString(s: String): Either[String, AbsoluteContractId] =
      V1.fromString(s)
        .left
        .flatMap(_ => V0.fromString(s))
        .left
        .map(_ => s"""cannot parse ContractId "$s"""")

    def assertFromString(s: String): AbsoluteContractId =
      assertRight(fromString(s))

    implicit val `AbsCid Order`: Order[AbsoluteContractId] = new Order[AbsoluteContractId] {
      import scalaz.Ordering._
      override def order(a: AbsoluteContractId, b: AbsoluteContractId) =
        (a, b) match {
          case (a: V0, b: V0) => a ?|? b
          case (a: V1, b: V1) => a ?|? b
          case (_: V0, _: V1) => LT
          case (_: V1, _: V0) => GT
        }

      override def equal(a: AbsoluteContractId, b: AbsoluteContractId) =
        (a, b).match2 {
          case a: V0 => { case b: V0 => a === b }
          case V1(discA, suffA) => {
            case V1(discB, suffB) => discA == discB && suffA.toByteString == suffB.toByteString
          }
        }(fallback = false)
    }
  }

  final case class RelativeContractId(txnid: NodeId) extends ContractId

  object ContractId {
    implicit val equalInstance: Equal[ContractId] = Equal.equalA

    implicit val noCidMapper: CidMapper.NoCidChecker[ContractId, Nothing] =
      CidMapper.basicMapperInstance[ContractId, Nothing]
    implicit val noRelCidMapper: CidMapper.NoRelCidChecker[ContractId, AbsoluteContractId] =
      CidMapper.basicMapperInstance[ContractId, AbsoluteContractId]
    implicit val relCidResolver: CidMapper.RelCidResolver[ContractId, AbsoluteContractId] =
      CidMapper.basicCidResolverInstance
    implicit val cidSuffixer: CidMapper.CidSuffixer[ContractId, AbsoluteContractId.V1] =
      CidMapper.basicMapperInstance[ContractId, AbsoluteContractId.V1]
  }

  /** The constructor is private so that we make sure that only this object constructs
    * node ids -- we don't want external code to manipulate them.
    */
  final case class NodeId(index: Int)

  object NodeId {
    implicit def cidMapperInstance[In, Out]: CidMapper[NodeId, NodeId, In, Out] =
      CidMapper.trivialMapper
  }

  /*** Keys cannot contain contract ids */
  type Key = Value[Nothing]

  val ValueTrue: ValueBool = ValueBool.True
  val ValueFalse: ValueBool = ValueBool.False
  val ValueNil: ValueList[Nothing] = ValueList(FrontStack.empty)
  val ValueNone: ValueOptional[Nothing] = ValueOptional(None)
}

/** This Order is not strictly compatible with the Equal instance, so
  * instances of it must always be local or tagged.
  */
private final class `Value Order instance`[Cid: Order](Scope: Value.LookupVariantEnum)
    extends Order[Value[Cid]] {
  import Value._, Utf8.ImplicitOrder._
  import scalaz.std.anyVal._
  import ScalazEqual._2, _2._

  implicit final def Self: this.type = this

  override final def order(a: Value[Cid], b: Value[Cid]) = {
    val (ixa, cmp) = ixv(a)
    cmp.applyOrElse(b, { b: Value[Cid] =>
      val (ixb, _) = ixv(b)
      ixa ?|? ixb
    })
  }

  private[this] def ixv(a: Value[Cid]) = a match {
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
      (130, k {
        case ValueEnum(idB, b) =>
          ctorOrder(idA, idB, a, b)
      })
    case ValueRecord(_, a) => (140, k { case ValueRecord(_, b) => _2.T.subst(a) ?|? _2.T.subst(b) })
    case ValueStruct(a) => (150, k { case ValueStruct(b) => a ?|? b })
    case ValueVariant(idA, conA, a) =>
      (160, k {
        case ValueVariant(idB, conB, b) =>
          ctorOrder(idA, idB, conA, conB) |+| a ?|? b
      })
  }

  private[this] def ctorOrder(
      idA: Option[Identifier],
      idB: Option[Identifier],
      ctorA: Ref.Name,
      ctorB: Ref.Name) = {
    val idAB = unifyIds(idA, idB)
    Scope(idAB) cata ({ ctors =>
      val lookup = ctors.toSeq
      (lookup indexOf ctorA) ?|? (lookup indexOf ctorB)
    },
    noType(idAB))
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

  private[this] def k[Z](f: Value[Cid] PartialFunction Z): f.type = f
}

private final class `Value Equal instance`[Cid: Equal] extends Equal[Value[Cid]] {
  import Value._
  import ScalazEqual._

  override final def equalIsNatural: Boolean = Equal[Cid].equalIsNatural

  implicit final def Self: this.type = this

  override final def equal(a: Value[Cid], b: Value[Cid]) =
    (a, b).match2 {
      case a @ (_: ValueInt64 | _: ValueNumeric | _: ValueText | _: ValueTimestamp | _: ValueParty |
          _: ValueBool | _: ValueDate | ValueUnit) => { case b => a == b }
      case r: ValueRecord[Cid] => {
        case ValueRecord(tycon2, fields2) =>
          import r._
          tycon == tycon2 && fields === fields2
      }
      case v: ValueVariant[Cid] => {
        case ValueVariant(tycon2, variant2, value2) =>
          import v._
          tycon == tycon2 && variant == variant2 && value === value2
      }
      case v: ValueEnum => {
        case ValueEnum(tycon2, value2) =>
          import v._
          tycon == tycon2 && value == value2
      }
      case ValueContractId(value) => {
        case ValueContractId(value2) =>
          value === value2
      }
      case ValueList(values) => {
        case ValueList(values2) =>
          values === values2
      }
      case ValueOptional(value) => {
        case ValueOptional(value2) =>
          value === value2
      }
      case ValueStruct(fields) => {
        case ValueStruct(fields2) =>
          fields === fields2
      }
      case ValueTextMap(map1) => {
        case ValueTextMap(map2) =>
          map1 === map2
      }
      case genMap: ValueGenMap[Cid] => {
        case ValueGenMap(entries2) =>
          genMap.entries === entries2
      }
    }(fallback = false)
}
