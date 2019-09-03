// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import scala.language.higherKinds
import data.{FrontStack, Numeric, Ref, SortedLookupList, Time}
import data.ImmArray.ImmArraySeq
import data.DataArbitrary._
import iface.{Type, TypeNumeric, TypePrim, PrimType => PT}
import scalaz.Id.Id
import scalaz.syntax.traverse._
import scalaz.std.option._
import org.scalacheck.{Arbitrary, Gen, Shrink}

/** [[ValueGenerators]] produce untyped values; for example, if you use the list gen,
  * you get a heterogeneous list.  The generation target here, on the other hand, is
  * ''a pair of a type and a generator of values of that type''.  For example, you might
  * generate the type `[Map Decimal]`, which would be accompanied by a `Gen` that produced
  * [[Value]]s each guaranteed to be a list of maps, whose values are all guaranteed to
  * be Decimals.
  *
  * As a user, you will probably be most interested in one of the generators derived
  * from `genAddend`; if you need a generator in this theme not already supported by
  * one such generator, you can probably derive a new one from `genAddend` yourself.
  */
object TypedValueGenerators {
  sealed abstract class ValueAddend {
    type Inj[Cid]
    def t: Type
    def inj[Cid]: Inj[Cid] => Value[Cid]
    def prj[Cid]: Value[Cid] => Option[Inj[Cid]]
    implicit def injarb[Cid: Arbitrary]: Arbitrary[Inj[Cid]]
    implicit def injshrink[Cid: Shrink]: Shrink[Inj[Cid]]
    final override def toString = s"${classOf[ValueAddend].getSimpleName}{t = ${t.toString}}"
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  object ValueAddend extends PrimInstances[Lambda[a => ValueAddend { type Inj[_] = a }]] {
    type Aux[Inj0[_]] = ValueAddend {
      type Inj[Cid] = Inj0[Cid]
    }
    type NoCid[Inj0] = ValueAddend {
      type Inj[_] = Inj0
    }

    def noCid[Inj0](pt: PT, inj0: Inj0 => Value[Nothing])(prj0: Value[Any] PartialFunction Inj0)(
        implicit arb: Arbitrary[Inj0],
        shr: Shrink[Inj0]): NoCid[Inj0] = new ValueAddend {
      type Inj[Cid] = Inj0
      override val t = TypePrim(pt, ImmArraySeq.empty)
      override def inj[Cid] = inj0
      override def prj[Cid] = prj0.lift
      override def injarb[Cid: Arbitrary] = arb
      override def injshrink[Cid: Shrink] = shr
    }

    import Value._, ValueGenerators.Implicits._
    val text = noCid(PT.Text, ValueText) { case ValueText(t) => t }
    val int64 = noCid(PT.Int64, ValueInt64) { case ValueInt64(i) => i }
    val unit = noCid(PT.Unit, (_: Unit) => ValueUnit) { case ValueUnit => () }
    val date = noCid(PT.Date, ValueDate) { case ValueDate(d) => d }
    val timestamp = noCid(PT.Timestamp, ValueTimestamp) { case ValueTimestamp(t) => t }
    val bool = noCid(PT.Bool, ValueBool) { case ValueBool(b) => b }
    val party = noCid(PT.Party, ValueParty) { case ValueParty(p) => p }

    def numeric(scale: Int): NoCid[Numeric] = new ValueAddend {
      type Inj[Cid] = Numeric

      override def t: Type = TypeNumeric(scale)

      override def inj[Cid]: Numeric => Value[Cid] = ValueNumeric

      override def prj[Cid]: Value[Cid] => Option[Numeric] = {
        case ValueNumeric(x) if x.scale <= scale => Some(x)
        case _ => None
      }

      override def injarb[Cid: Arbitrary]: Arbitrary[Numeric] =
        Arbitrary(ValueGenerators.numGen(scale))

      override def injshrink[Cid: Shrink]: Shrink[Numeric] =
        implicitly

    }

    val contractId: Aux[Id] = new ValueAddend {
      type Inj[Cid] = Cid
      // TODO SC it probably doesn't make much difference for our initial use case,
      // but the proper arg should probably end up here, not Unit
      override val t = TypePrim(PT.ContractId, ImmArraySeq(TypePrim(PT.Unit, ImmArraySeq.empty)))
      override def inj[Cid] = ValueContractId(_)
      override def prj[Cid] = {
        case ValueContractId(cid) => Some(cid)
        case _ => None
      }
      override def injarb[Cid](implicit cid: Arbitrary[Cid]) = cid
      override def injshrink[Cid](implicit shr: Shrink[Cid]) = shr
    }

    type Compose[F[_], G[_], A] = F[G[A]]
    def list(elt: ValueAddend): Aux[Compose[Vector, elt.Inj, ?]] = new ValueAddend {
      import scalaz.std.vector._
      type Inj[Cid] = Vector[elt.Inj[Cid]]
      override val t = TypePrim(PT.List, ImmArraySeq(elt.t))
      override def inj[Cid] = elts => ValueList(elts.map(elt.inj).to[FrontStack])
      override def prj[Cid] = {
        case ValueList(v) => v.toImmArray.toSeq.to[Vector] traverse elt.prj
        case _ => None
      }
      override def injarb[Cid: Arbitrary] = {
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        implicitly[Arbitrary[Vector[elt.Inj[Cid]]]]
      }
      override def injshrink[Cid: Shrink] = {
        import elt.injshrink
        implicitly[Shrink[Vector[elt.Inj[Cid]]]]
      }
    }

    def optional(elt: ValueAddend): Aux[Compose[Option, elt.Inj, ?]] = new ValueAddend {
      type Inj[Cid] = Option[elt.Inj[Cid]]
      override val t = TypePrim(PT.Optional, ImmArraySeq(elt.t))
      override def inj[Cid] = oe => ValueOptional(oe map elt.inj)
      override def prj[Cid] = {
        case ValueOptional(v) => v traverse elt.prj
        case _ => None
      }
      override def injarb[Cid: Arbitrary] = {
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        implicitly[Arbitrary[Option[elt.Inj[Cid]]]]
      }
      override def injshrink[Cid: Shrink] = {
        import elt.injshrink
        implicitly[Shrink[Option[elt.Inj[Cid]]]]
      }
    }

    def map(elt: ValueAddend): Aux[Compose[SortedLookupList, elt.Inj, ?]] = new ValueAddend {
      type Inj[Cid] = SortedLookupList[elt.Inj[Cid]]
      override val t = TypePrim(PT.Map, ImmArraySeq(elt.t))
      override def inj[Cid] =
        (sll: SortedLookupList[elt.Inj[Cid]]) => ValueMap(sll map elt.inj)
      override def prj[Cid] = {
        case ValueMap(sll) => sll traverse elt.prj
        case _ => None
      }
      override def injarb[Cid: Arbitrary] = {
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        implicitly[Arbitrary[SortedLookupList[elt.Inj[Cid]]]]
      }
      override def injshrink[Cid: Shrink] = Shrink.shrinkAny // XXX descend
    }
  }

  trait PrimInstances[F[_]] {
    def text: F[String]
    def int64: F[Long]
    def unit: F[Unit]
    def date: F[Time.Date]
    def timestamp: F[Time.Timestamp]
    def bool: F[Boolean]
    def party: F[Ref.Party]
    def leafInstances: Seq[F[_]] = Seq(text, int64, unit, date, timestamp, bool, party)
  }

  /** This is the key member of interest, supporting many patterns:
    *
    *  1. generating a type and value
    *  2. generating a type and many values
    *  3. generating well-typed values of different types
    *
    * All of which are derivable from what [[ValueAddend]] is, ''a type, a
    * prism into [[Value]], a [[Type]] describing that type, and
    * Scalacheck support surrounding that type.''
    */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  val genAddend: Gen[ValueAddend] = Gen.sized { sz =>
    val self = Gen.resize(sz / 2, Gen.lzy(genAddend))
    val nestSize = sz / 6
    Gen.frequency(
      ((sz max 1) * ValueAddend.leafInstances.length, Gen.oneOf(ValueAddend.leafInstances)),
      (sz max 1, Gen.const(ValueAddend.contractId)),
      (sz max 1, Gen.choose(0, Numeric.maxPrecision).map(ValueAddend.numeric)),
      (nestSize, self.map(ValueAddend.list(_))),
      (nestSize, self.map(ValueAddend.optional(_))),
      (nestSize, self.map(ValueAddend.map(_))),
    )
  }

  /** Generate a type and value guaranteed to conform to that type. */
  def genTypeAndValue[Cid](cid: Gen[Cid]): Gen[(Type, Value[Cid])] =
    for {
      addend <- genAddend
      value <- addend.injarb(Arbitrary(cid)).arbitrary
    } yield (addend.t, addend.inj(value))
}
