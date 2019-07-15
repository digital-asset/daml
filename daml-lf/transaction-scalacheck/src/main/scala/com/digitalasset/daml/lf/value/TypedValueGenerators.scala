// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import scala.language.higherKinds

import data.{Decimal, FrontStack, Ref, Time}
import data.ImmArray.ImmArraySeq
import iface.{PrimType => PT, Type, TypePrim}

import org.scalacheck.{Arbitrary, Shrink}

/** [[ValueGenerators]] produce untyped values; for example, if you use the list gen,
  * you get a heterogeneous list.  The generation target here, on the other hand, is
  * ''a pair of a type and a generator of values of that type''.  For example, you might
  * generate the type `[Map Decimal]`, which would be accompanied by a `Gen` that produced
  * [[Value]]s each guaranteed to be a list of maps, whose values are all guaranteed to
  * be Decimals.
  */
object TypedValueGenerators {
  sealed abstract class ValueAddend {
    type Inj[Cid]
    def t: Type
    def inj[Cid]: Inj[Cid] => Value[Cid]
    def prj[Cid]: Value[Cid] PartialFunction Inj[Cid]
    implicit def injarb[Cid: Arbitrary]: Arbitrary[Inj[Cid]]
    implicit def injshrink[Cid: Shrink]: Shrink[Inj[Cid]]
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
      override def prj[Cid] = prj0
      override def injarb[Cid: Arbitrary] = arb
      override def injshrink[Cid: Shrink] = shr
    }

    import Value._, ValueGenerators.Implicits._
    val text = noCid(PT.Text, ValueText) { case ValueText(t) => t }
    val int64 = noCid(PT.Int64, ValueInt64) { case ValueInt64(i) => i }
    val decimal = noCid(PT.Decimal, ValueDecimal) { case ValueDecimal(d) => d }
    val unit = noCid(PT.Unit, (_: Unit) => ValueUnit) { case ValueUnit => () }
    val date = noCid(PT.Date, ValueDate) { case ValueDate(d) => d }
    val timestamp = noCid(PT.Timestamp, ValueTimestamp) { case ValueTimestamp(t) => t }
    val bool = noCid(PT.Bool, ValueBool) { case ValueBool(b) => b }
    val party = noCid(PT.Party, ValueParty) { case ValueParty(p) => p }

    type Compose[F[_], G[_], A] = F[G[A]]
    def list(elt: ValueAddend): Aux[Compose[FrontStack, elt.Inj, ?]] = new ValueAddend {
      type Inj[Cid] = FrontStack[elt.Inj[Cid]]
      override val t = TypePrim(PT.List, ImmArraySeq(elt.t))
      override def inj[Cid] = elts => ValueList(elts map elt.inj)
      override def prj[Cid] = { case ValueList(v) => v map elt.prj }
      override def injarb[Cid: Arbitrary] = ???
      override def injshrink[Cid: Shrink] = ???
    }
  }

  trait PrimInstances[F[_]] {
    def text: F[String]
    def int64: F[Long]
    def decimal: F[Decimal]
    def unit: F[Unit]
    def date: F[Time.Date]
    def timestamp: F[Time.Timestamp]
    def bool: F[Boolean]
    def party: F[Ref.Party]
    def leafInstances: Seq[F[_]] = Seq(text, int64, decimal, unit, date, timestamp, bool, party)
  }
}
