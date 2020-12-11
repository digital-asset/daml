// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package value
package test

import scala.language.higherKinds
import data.{FrontStack, ImmArray, ImmArrayCons, Numeric, Ref, SortedLookupList, Time}
import ImmArray.ImmArraySeq
import data.DataArbitrary._
import iface.{
  DefDataType,
  Enum,
  Record,
  Type,
  TypeCon,
  TypeConName,
  TypeNumeric,
  TypePrim,
  Variant,
  PrimType => PT
}
import scalaz.{@@, Order, Ordering, Tag, ~>}
import scalaz.Id.Id
import scalaz.syntax.bitraverse._
import scalaz.syntax.traverse._
import scalaz.std.option._
import scalaz.std.tuple._
import org.scalacheck.{Arbitrary, Gen, Shrink}
import Arbitrary.arbitrary

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
    type IntroCtx[Cid] = Order[Cid]
    def t: Type
    def inj[Cid: IntroCtx](v: Inj[Cid]): Value[Cid]
    def prj[Cid]: Value[Cid] => Option[Inj[Cid]]
    implicit def injord[Cid: Order]: Order[Inj[Cid]]
    implicit def injarb[Cid: Arbitrary: IntroCtx]: Arbitrary[Inj[Cid]]
    implicit def injshrink[Cid: Shrink]: Shrink[Inj[Cid]]
    final override def toString = s"${classOf[ValueAddend].getSimpleName}{t = ${t.toString}}"
  }

  object ValueAddend extends PrimInstances[Lambda[a => ValueAddend { type Inj[_] = a }]] {
    type Aux[Inj0[_]] = ValueAddend {
      type Inj[Cid] = Inj0[Cid]
    }
    type NoCid[Inj0] = ValueAddend {
      type Inj[_] = Inj0
    }

    private sealed abstract class NoCid0[Inj0](
        implicit ord: Order[Inj0],
        arb: Arbitrary[Inj0],
        shr: Shrink[Inj0])
        extends ValueAddend {
      type Inj[Cid] = Inj0
      override final def injord[Cid: Order] = ord
      override final def injarb[Cid: Arbitrary: IntroCtx] = arb
      override final def injshrink[Cid: Shrink] = shr
    }

    def noCid[Inj0: Order: Arbitrary: Shrink](pt: PT, inj0: Inj0 => Value[Nothing])(
        prj0: Value[Any] PartialFunction Inj0): NoCid[Inj0] = new NoCid0[Inj0] {
      override val t = TypePrim(pt, ImmArraySeq.empty)
      override def inj[Cid: IntroCtx](v: Inj0) = inj0(v)
      override def prj[Cid] = prj0.lift
    }

    import Value._, ValueGenerators.Implicits._, data.Utf8.ImplicitOrder._
    import scalaz.std.anyVal._
    val text = noCid(PT.Text, ValueText) { case ValueText(t) => t }
    val int64 = noCid(PT.Int64, ValueInt64) { case ValueInt64(i) => i }
    val unit = noCid(PT.Unit, (_: Unit) => ValueUnit) { case ValueUnit => () }
    val date = noCid(PT.Date, ValueDate) { case ValueDate(d) => d }
    val timestamp = noCid(PT.Timestamp, ValueTimestamp) { case ValueTimestamp(t) => t }
    val bool = noCid(PT.Bool, ValueBool(_)) { case ValueBool(b) => b }
    val party = noCid(PT.Party, ValueParty) { case ValueParty(p) => p }

    def numeric(scale: Numeric.Scale): NoCid[Numeric] = {
      implicit val arb: Arbitrary[Numeric] = Arbitrary(ValueGenerators.numGen(scale))
      new NoCid0[Numeric] {
        override def t: Type = TypeNumeric(scale)

        override def inj[Cid: IntroCtx](x: Numeric): Value[Cid] =
          ValueNumeric(Numeric.assertFromBigDecimal(scale, x))

        override def prj[Cid]: Value[Cid] => Option[Numeric] = {
          case ValueNumeric(x) => Numeric.fromBigDecimal(scale, x).toOption
          case _ => None
        }
      }
    }

    val contractId: Aux[Id] = new ValueAddend {
      type Inj[Cid] = Cid
      // TODO SC it probably doesn't make much difference for our initial use case,
      // but the proper arg should probably end up here, not Unit
      override val t = TypePrim(PT.ContractId, ImmArraySeq(TypePrim(PT.Unit, ImmArraySeq.empty)))
      override def inj[Cid: IntroCtx](v: Cid) = ValueContractId(v)
      override def prj[Cid] = {
        case ValueContractId(cid) => Some(cid)
        case _ => None
      }
      override def injord[Cid](implicit ord: Order[Cid]) = ord
      override def injarb[Cid](implicit cid: Arbitrary[Cid], _ign: Order[Cid]) = cid
      override def injshrink[Cid](implicit shr: Shrink[Cid]) = shr
    }

    type Compose[F[_], G[_], A] = F[G[A]]
    def list(elt: ValueAddend): Aux[Compose[Vector, elt.Inj, *]] = new ValueAddend {
      type Inj[Cid] = Vector[elt.Inj[Cid]]
      override val t = TypePrim(PT.List, ImmArraySeq(elt.t))
      override def inj[Cid: IntroCtx](elts: Inj[Cid]) =
        ValueList(elts.map(elt.inj(_)).to[FrontStack])
      override def prj[Cid] = {
        case ValueList(v) =>
          import scalaz.std.vector._
          v.toImmArray.toSeq.to[Vector] traverse elt.prj
        case _ => None
      }
      override def injord[Cid: Order] = {
        import scalaz.std.iterable._ // compatible with SValue ordering
        implicit val e: Order[elt.Inj[Cid]] = elt.injord
        Order[Iterable[elt.Inj[Cid]]] contramap identity
      }
      override def injarb[Cid: Arbitrary: IntroCtx] = {
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        Tag unsubst implicitly[Arbitrary[Vector[elt.Inj[Cid]] @@ Div3]]
      }
      override def injshrink[Cid: Shrink] = {
        import elt.injshrink
        implicitly[Shrink[Vector[elt.Inj[Cid]]]]
      }
    }

    def optional(elt: ValueAddend): Aux[Compose[Option, elt.Inj, *]] = new ValueAddend {
      type Inj[Cid] = Option[elt.Inj[Cid]]
      override val t = TypePrim(PT.Optional, ImmArraySeq(elt.t))
      override def inj[Cid: IntroCtx](oe: Inj[Cid]) = ValueOptional(oe map (elt.inj(_)))
      override def prj[Cid] = {
        case ValueOptional(v) => v traverse elt.prj
        case _ => None
      }
      override def injord[Cid: Order] = {
        implicit val e: Order[elt.Inj[Cid]] = elt.injord
        Order[Option[elt.Inj[Cid]]]
      }
      override def injarb[Cid: Arbitrary: IntroCtx] = {
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        implicitly[Arbitrary[Option[elt.Inj[Cid]]]]
      }
      override def injshrink[Cid: Shrink] = {
        import elt.injshrink
        implicitly[Shrink[Option[elt.Inj[Cid]]]]
      }
    }

    def map(elt: ValueAddend): Aux[Compose[SortedLookupList, elt.Inj, *]] = new ValueAddend {
      type Inj[Cid] = SortedLookupList[elt.Inj[Cid]]
      override val t = TypePrim(PT.TextMap, ImmArraySeq(elt.t))
      override def inj[Cid: IntroCtx](sll: SortedLookupList[elt.Inj[Cid]]) =
        ValueTextMap(sll map (elt.inj(_)))
      override def prj[Cid] = {
        case ValueTextMap(sll) => sll traverse elt.prj
        case _ => None
      }
      override def injord[Cid: Order] = {
        implicit val e: Order[elt.Inj[Cid]] = elt.injord[Cid]
        Order[SortedLookupList[elt.Inj[Cid]]]
      }
      override def injarb[Cid: Arbitrary: IntroCtx] = {
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        Tag unsubst implicitly[Arbitrary[SortedLookupList[elt.Inj[Cid]] @@ Div3]]
      }
      override def injshrink[Cid: Shrink] = Shrink.shrinkAny // XXX descend
    }

    def genMap(key: ValueAddend, elt: ValueAddend): ValueAddend {
      type Inj[Cid] = key.Inj[Cid] Map elt.Inj[Cid]
    } = new ValueAddend {
      type Inj[Cid] = key.Inj[Cid] Map elt.Inj[Cid]
      override val t = TypePrim(PT.GenMap, ImmArraySeq(key.t, elt.t))
      override def inj[Cid: IntroCtx](m: key.Inj[Cid] Map elt.Inj[Cid]) =
        ValueGenMap {
          import key.{injord => keyorder}
          implicit val skeyord: math.Ordering[key.Inj[Cid]] = Order[key.Inj[Cid]].toScalaOrdering
          m.to[ImmArraySeq]
            .sortBy(_._1)
            .map { case (k, v) => (key.inj(k), elt.inj(v)) }
            .toImmArray
        }
      override def prj[Cid] = {
        case ValueGenMap(kvs) =>
          kvs traverse (_ bitraverse (key.prj[Cid], elt.prj[Cid])) map (_.toSeq.toMap)
        case _ => None
      }
      override def injord[Cid: Order] = {
        implicit val k: Order[key.Inj[Cid]] = key.injord
        implicit val ki: math.Ordering[key.Inj[Cid]] = k.toScalaOrdering
        implicit val e: Order[elt.Inj[Cid]] = elt.injord
        // for compatibility with SValue ordering
        Order[ImmArray[(key.Inj[Cid], elt.Inj[Cid])]] contramap { m =>
          m.to[ImmArraySeq].sortBy(_._1).toImmArray
        }
      }
      override def injarb[Cid: Arbitrary: IntroCtx] = {
        implicit val k: Arbitrary[key.Inj[Cid]] = key.injarb
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        Tag unsubst implicitly[Arbitrary[key.Inj[Cid] Map elt.Inj[Cid] @@ Div3]]
      }
      override def injshrink[Cid: Shrink] = {
        import key.{injshrink => keyshrink}, elt.{injshrink => eltshrink}
        implicitly[Shrink[key.Inj[Cid] Map elt.Inj[Cid]]]
      }
    }

    /** See [[RecVarSpec]] companion for usage examples. */
    def record(name: Ref.Identifier, spec: RecVarSpec): (DefDataType.FWT, Aux[spec.HRec]) =
      (DefDataType(ImmArraySeq.empty, Record(spec.t.to[ImmArraySeq])), new ValueAddend {
        private[this] val lfvFieldNames = spec.t map { case (n, _) => Some(n) }
        type Inj[Cid] = spec.HRec[Cid]
        override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
        override def inj[Cid: IntroCtx](hl: Inj[Cid]) =
          ValueRecord(Some(name), (lfvFieldNames zip spec.injRec(hl)).to[ImmArray])
        override def prj[Cid] = {
          case ValueRecord(_, fields) if fields.length == spec.t.length =>
            spec.prjRec(fields)
          case _ => None
        }
        override def injord[Cid: Order] = spec.record[Cid]
        override def injarb[Cid: Arbitrary: IntroCtx] = spec.recarb[Cid]
        override def injshrink[Cid: Shrink] = spec.recshrink
      })

    /** See [[RecVarSpec]] companion for usage examples. */
    def variant(name: Ref.Identifier, spec: RecVarSpec): (DefDataType.FWT, Aux[spec.HVar]) =
      (DefDataType(ImmArraySeq.empty, Variant(spec.t.to[ImmArraySeq])), new ValueAddend {
        type Inj[Cid] = spec.HVar[Cid]
        override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
        override def inj[Cid: IntroCtx](cp: Inj[Cid]) = {
          val (ctor, v) = spec.injVar(cp)
          ValueVariant(Some(name), ctor, v)
        }
        override def prj[Cid] = {
          case ValueVariant(_, name, vv) =>
            spec.prjVar get name flatMap (_(vv))
          case _ => None
        }
        override def injord[Cid: Order] = spec.varord[Cid]
        override def injarb[Cid: Arbitrary: IntroCtx] =
          Arbitrary(Gen.oneOf(spec.vararb[Cid].toSeq).flatMap(_._2))
        override def injshrink[Cid: Shrink] = spec.varshrink
      })

    def enum(
        name: Ref.Identifier,
        members: Seq[Ref.Name]): (DefDataType.FWT, EnumAddend[members.type]) =
      (DefDataType(ImmArraySeq.empty, Enum(members.to[ImmArraySeq])), new EnumAddend[members.type] {
        type Member = Ref.Name
        override val values = members
        override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
        override def inj[Cid: IntroCtx](v: Inj[Cid]) = ValueEnum(Some(name), v)
        override def prj[Cid] = {
          case ValueEnum(_, dc) => get(dc)
          case _ => None
        }
        override def injord[Cid: Order] = Order.orderBy(values.indexOf)
        override def injarb[Cid: Arbitrary: IntroCtx] = Arbitrary(Gen.oneOf(values))
        override def injshrink[Cid: Shrink] =
          Shrink { ev =>
            if (!(values.headOption contains ev)) values.headOption.toStream
            else Stream.empty
          }
      })

    sealed abstract class EnumAddend[+Values <: Seq[Ref.Name]] extends ValueAddend {
      type Inj[Cid] = Member
      type Member <: Ref.Name
      val values: Values with Seq[Member]
      def get(m: Ref.Name): Option[Member] = values collectFirst { case v if m == v => v }
    }
  }

  sealed abstract class RecVarSpec { self =>
    import shapeless.{::, :+:, Coproduct, HList, Inl, Inr, Witness}
    import shapeless.labelled.{field, FieldType => :->>:}

    type HRec[Cid] <: HList
    type HVar[Cid] <: Coproduct
    type IntroCtx[Cid] = Order[Cid]
    def ::[K <: Symbol](h: K :->>: ValueAddend)(implicit ev: Witness.Aux[K]): RecVarSpec {
      type HRec[Cid] = (K :->>: h.Inj[Cid]) :: self.HRec[Cid]
      type HVar[Cid] = (K :->>: h.Inj[Cid]) :+: self.HVar[Cid]
    } =
      new RecVarSpec {
        private[this] val fname = Ref.Name assertFromString ev.value.name
        type HRec[Cid] = (K :->>: h.Inj[Cid]) :: self.HRec[Cid]
        type HVar[Cid] = (K :->>: h.Inj[Cid]) :+: self.HVar[Cid]
        override val t = (fname, h.t) :: self.t
        override def injRec[Cid: IntroCtx](v: HRec[Cid]) =
          h.inj(v.head) :: self.injRec(v.tail)
        override def prjRec[Cid](v: ImmArray[(_, Value[Cid])]) = v match {
          case ImmArrayCons(vh, vt) =>
            for {
              pvh <- h.prj(vh._2)
              pvt <- self.prjRec(vt)
            } yield field[K](pvh) :: pvt
          case _ => None
        }

        override def record[Cid: Order] = {
          import h.{injord => hord}, self.{record => tailord}
          Order.orderBy { case ah :: at => (ah: h.Inj[Cid], at) }
        }

        override def recarb[Cid: Arbitrary: IntroCtx] = {
          import self.{recarb => tailarb}, h.{injarb => headarb}
          Arbitrary(arbitrary[(h.Inj[Cid], self.HRec[Cid])] map {
            case (vh, vt) =>
              field[K](vh) :: vt
          })
        }

        override def recshrink[Cid: Shrink]: Shrink[HRec[Cid]] = {
          import h.{injshrink => hshrink}, self.{recshrink => tshrink}
          Shrink {
            case vh :: vt =>
              (Shrink.shrink(vh: h.Inj[Cid]) zip Shrink.shrink(vt)) map {
                case (nh, nt) => field[K](nh) :: nt
              }
          }
        }

        override def injVar[Cid: IntroCtx](v: HVar[Cid]) = v match {
          case Inl(hv) => (fname, h.inj(hv))
          case Inr(tl) => self.injVar(tl)
        }

        override val prjVar = self.prjVar transform { (_, tf) =>
          Lambda[Value ~> PrjResult](tv => tf(tv) map (Inr(_)))
        } updated (fname, Lambda[Value ~> PrjResult](hv => h.prj(hv) map (pv => Inl(field[K](pv)))))

        override def varord[Cid: Order] =
          (a, b) =>
            (a, b) match {
              case (Inr(at), Inr(bt)) => self.varord[Cid].order(at, bt)
              case (Inl(_), Inr(_)) => Ordering.LT
              case (Inr(_), Inl(_)) => Ordering.GT
              case (Inl(ah), Inl(bh)) => h.injord[Cid].order(ah, bh)
          }

        override def vararb[Cid: Arbitrary: IntroCtx] =
          self.vararb[Cid] transform { (_, ta) =>
            ta map (Inr(_))
          } updated (fname, {
            import h.{injarb => harb}
            arbitrary[h.Inj[Cid]] map (hv => Inl(field[K](hv)))
          })

        override def varshrink[Cid: Shrink] = {
          val lshr: Shrink[h.Inj[Cid]] = h.injshrink
          val rshr: Shrink[self.HVar[Cid]] = self.varshrink
          Shrink {
            case Inl(hv) => lshr shrink hv map (shv => Inl(field[K](shv)))
            case Inr(tl) => rshr shrink tl map (Inr(_))
          }
        }
      }

    private[TypedValueGenerators] val t: List[(Ref.Name, Type)]
    private[TypedValueGenerators] def injRec[Cid: IntroCtx](v: HRec[Cid]): List[Value[Cid]]
    private[TypedValueGenerators] def prjRec[Cid](v: ImmArray[(_, Value[Cid])]): Option[HRec[Cid]]
    private[TypedValueGenerators] implicit def record[Cid: Order]: Order[HRec[Cid]]
    private[TypedValueGenerators] implicit def recarb[Cid: Arbitrary: IntroCtx]
      : Arbitrary[HRec[Cid]]
    private[TypedValueGenerators] implicit def recshrink[Cid: Shrink]: Shrink[HRec[Cid]]

    private[TypedValueGenerators] def injVar[Cid: IntroCtx](v: HVar[Cid]): (Ref.Name, Value[Cid])
    private[TypedValueGenerators] type PrjResult[Cid] = Option[HVar[Cid]]
    // could be made more efficient by replacing ~> with a Nat GADT,
    // but the :+: case is tricky enough as it is
    private[TypedValueGenerators] val prjVar: Map[Ref.Name, Value ~> PrjResult]
    private[TypedValueGenerators] implicit def varord[Cid: Order]: Order[HVar[Cid]]
    private[TypedValueGenerators] implicit def vararb[Cid: Arbitrary: IntroCtx]
      : Map[Ref.Name, Gen[HVar[Cid]]]
    private[TypedValueGenerators] implicit def varshrink[Cid: Shrink]: Shrink[HVar[Cid]]
  }

  case object RNil extends RecVarSpec {
    import shapeless.{HNil, CNil}
    type HRec[Cid] = HNil
    type HVar[Cid] = CNil
    private[TypedValueGenerators] override val t = List.empty
    private[TypedValueGenerators] override def injRec[Cid: IntroCtx](v: HNil) = List.empty
    private[TypedValueGenerators] override def prjRec[Cid](v: ImmArray[(_, Value[Cid])]) =
      Some(HNil)
    private[TypedValueGenerators] override def record[Cid: Order] = (_, _) => Ordering.EQ
    private[TypedValueGenerators] override def recarb[Cid: Arbitrary: IntroCtx] =
      Arbitrary(Gen const HNil)
    private[TypedValueGenerators] override def recshrink[Cid: Shrink] =
      Shrink.shrinkAny

    private[TypedValueGenerators] override def injVar[Cid: IntroCtx](v: CNil) = v.impossible
    private[TypedValueGenerators] override val prjVar = Map.empty
    private[TypedValueGenerators] override def varord[Cid: Order] = (v, _) => v.impossible
    private[TypedValueGenerators] override def vararb[Cid: Arbitrary: IntroCtx] = Map.empty
    private[TypedValueGenerators] override def varshrink[Cid: Shrink] = Shrink.shrinkAny
  }

  private[value] object RecVarSpec {
    // specifying records and variants works the same way: a
    // record written with ->> and ::, terminated with RNil (*not* HNil)
    val sample = {
      import shapeless.syntax.singleton._
      'foo ->> ValueAddend.int64 :: 'bar ->> ValueAddend.text :: RNil
    }

    // a RecVarSpec can be turned into a ValueAddend for records
    val (sampleRecordDDT, sampleAsRecord) =
      ValueAddend.record(
        Ref.Identifier(
          Ref.PackageId assertFromString "hash",
          Ref.QualifiedName assertFromString "Foo.SomeRecord"),
        sample)
    import shapeless.record.Record
    // supposing Cid = String, you can ascribe a matching value
    // using either the spec,
    val sampleData: sample.HRec[String] =
      Record(foo = 42L, bar = "hi")
    // or the record VA
    val sampleDataAgain: sampleAsRecord.Inj[String] = sampleData
    // ascription is not necessary; a correct `Record` expression already
    // has the correct type, as implicit conversion is not used at all

    // unlike most `name = value` calls in Scala, `Record` is order-sensitive,
    // just as DAML-LF record values are order-sensitive. Most tests should preserve
    // this behavior, but if you want to automatically reorder the keys (for a runtime cost!)
    // you can do so with `align`. The resulting error messages are far worse, so
    // I recommend just writing them in the correct order
    shapeless.test
      .illTyped("""Record(bar = "bye", foo = 84L): sample.HRec[String]""", "type mismatch.*")
    val backwardsSampleData: sample.HRec[String] =
      Record(bar = "bye", foo = 84L).align[sample.HRec[String]]

    // a RecVarSpec can be turned into a ValueAddend for variants
    val (sampleVariantDDT, sampleAsVariant) =
      ValueAddend.variant(
        Ref.Identifier(
          Ref.PackageId assertFromString "hash",
          Ref.QualifiedName assertFromString "Foo.SomeVariant"),
        sample)
    // supposing Cid = String, you can create a matching value with Coproduct
    import shapeless.Coproduct, shapeless.syntax.singleton._
    val sampleVt =
      Coproduct[sample.HVar[String]]('foo ->> 42L)
    val anotherSampleVt =
      Coproduct[sample.HVar[String]]('bar ->> "hi")
    // and the `variant` function produces Inj as a synonym for HVar
    // just as `record` makes it a synonym for HRec
    val samples: List[sampleAsVariant.Inj[String]] = List(sampleVt, anotherSampleVt)
    // Coproduct can be factored out, but the implicit resolution means you cannot
    // turn this into the obvious `map` call
    val sampleCp = Coproduct[sample.HVar[String]]
    val moreSamples = List(sampleCp('foo ->> 84L), sampleCp('bar ->> "bye"))
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

  /** Given a function that produces one-level-nested cases, produce a
    * ValueAddend gen.
    */
  def indGenAddend(
      f: (Gen[ValueAddend], Gen[ValueAddend]) => Seq[Gen[ValueAddend]]): Gen[ValueAddend] = {
    object Knot {
      val tie: Gen[ValueAddend] = Gen.sized { sz =>
        val keySelf = Gen.resize(sz / 10, tie)
        val self = Gen.resize(sz / 2, tie)
        val nestSize = sz / 3
        Gen.frequency(
          Seq(
            ((sz max 1) * ValueAddend.leafInstances.length, Gen.oneOf(ValueAddend.leafInstances)),
            (sz max 1, Gen.const(ValueAddend.contractId)),
            (sz max 1, Gen.oneOf(Numeric.Scale.values).map(ValueAddend.numeric)),
            (nestSize, self.map(ValueAddend.optional(_)))
          ) ++
            f(keySelf, self).map((nestSize, _)): _*
        )
      }
    }
    Knot.tie
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
  val genAddend: Gen[ValueAddend] =
    indGenAddend { (keySelf, self) =>
      Seq(
        self.map(ValueAddend.list(_)),
        self.map(ValueAddend.map(_)),
        Gen.zip(keySelf, self).map { case (k, v) => ValueAddend.genMap(k, v) },
      )
    }

  val genAddendNoListMap: Gen[ValueAddend] = indGenAddend((_, _) => Seq.empty)

  /** Generate a type and value guaranteed to conform to that type. */
  def genTypeAndValue[Cid: Order](cid: Gen[Cid]): Gen[(Type, Value[Cid])] =
    for {
      addend <- genAddend
      value <- addend.injarb(Arbitrary(cid), Order[Cid]).arbitrary
    } yield (addend.t, addend.inj(value))
}
