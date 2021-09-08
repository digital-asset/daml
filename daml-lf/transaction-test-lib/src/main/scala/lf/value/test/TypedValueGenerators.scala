// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package value
package test

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
  PrimType => PT,
}
import scala.collection.compat._
import scalaz.{@@, Order, Ordering, Tag}
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
    type Inj
    def t: Type
    def inj(v: Inj): Value
    def prj: Value => Option[Inj]
    implicit def injord: Order[Inj]
    // We parametrize by the Arbitrary instance so we can limit
    // contract id generation to comparable cids in some places.
    implicit def injarb(implicit cid: Arbitrary[Value.ContractId]): Arbitrary[Inj]
    implicit def injshrink(implicit shr: Shrink[Value.ContractId]): Shrink[Inj]
    final override def toString = s"${classOf[ValueAddend].getSimpleName}{t = ${t.toString}}"
  }

  object ValueAddend extends PrimInstances[Lambda[a => ValueAddend { type Inj = a }]] {
    type Aux[Inj0] = ValueAddend {
      type Inj = Inj0
    }
    type NoCid[Inj0] = ValueAddend {
      type Inj = Inj0
    }

    private sealed abstract class NoCid0[Inj0](implicit
        ord: Order[Inj0],
        arb: Arbitrary[Inj0],
        shr: Shrink[Inj0],
    ) extends ValueAddend {
      type Inj = Inj0
      override final def injord = ord
      override final def injarb(implicit cid: Arbitrary[Value.ContractId]) = arb
      override final def injshrink(implicit noshr: Shrink[Value.ContractId]) = shr
    }

    def noCid[Inj0: Order: Arbitrary: Shrink](pt: PT, inj0: Inj0 => Value)(
        prj0: Value PartialFunction Inj0
    ): NoCid[Inj0] = new NoCid0[Inj0] {
      override val t = TypePrim(pt, ImmArraySeq.empty)
      override def inj(v: Inj0) = inj0(v)
      override def prj = prj0.lift
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

        override def inj(x: Numeric): Value =
          ValueNumeric(Numeric.assertFromBigDecimal(scale, x))

        override def prj: Value => Option[Numeric] = {
          case ValueNumeric(x) => Numeric.fromBigDecimal(scale, x).toOption
          case _ => None
        }
      }
    }

    val contractId: Aux[ContractId] = new ValueAddend {
      type Inj = ContractId
      // TODO SC it probably doesn't make much difference for our initial use case,
      // but the proper arg should probably end up here, not Unit
      override val t = TypePrim(PT.ContractId, ImmArraySeq(TypePrim(PT.Unit, ImmArraySeq.empty)))
      override def inj(v: ContractId) = ValueContractId(v)
      override def prj = {
        case ValueContractId(cid) => Some(cid)
        case _ => None
      }
      override def injord = implicitly[Order[ContractId]]
      override def injarb(implicit cid: Arbitrary[Value.ContractId]) = Arbitrary(
        ValueGenerators.coidGen
      )
      override def injshrink(implicit shr: Shrink[Value.ContractId]) =
        implicitly[Shrink[ContractId]]
    }

    def list(elt: ValueAddend): Aux[Vector[elt.Inj]] = new ValueAddend {
      type Inj = Vector[elt.Inj]
      override val t = TypePrim(PT.List, ImmArraySeq(elt.t))
      override def inj(elts: Inj) =
        ValueList(elts.map(elt.inj(_)).to(FrontStack))
      override def prj = {
        case ValueList(v) =>
          import scalaz.std.vector._
          v.toImmArray.toSeq.to(Vector) traverse elt.prj
        case _ => None
      }
      override def injord = {
        import scalaz.std.iterable._ // compatible with SValue ordering
        implicit val e: Order[elt.Inj] = elt.injord
        Order[Iterable[elt.Inj]] contramap identity
      }
      override def injarb(implicit cid: Arbitrary[Value.ContractId]) = {
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        Tag unsubst implicitly[Arbitrary[Vector[elt.Inj] @@ Div3]]
      }
      override def injshrink(implicit shr: Shrink[Value.ContractId]) = {
        import elt.injshrink
        implicitly[Shrink[Vector[elt.Inj]]]
      }
    }

    def optional(elt: ValueAddend): Aux[Option[elt.Inj]] = new ValueAddend {
      type Inj = Option[elt.Inj]
      override val t = TypePrim(PT.Optional, ImmArraySeq(elt.t))
      override def inj(oe: Inj) = ValueOptional(oe map (elt.inj(_)))
      override def prj = {
        case ValueOptional(v) => v traverse elt.prj
        case _ => None
      }
      override def injord = {
        implicit val e: Order[elt.Inj] = elt.injord
        Order[Option[elt.Inj]]
      }
      override def injarb(implicit cid: Arbitrary[Value.ContractId]) = {
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        implicitly[Arbitrary[Option[elt.Inj]]]
      }
      override def injshrink(implicit cid: Shrink[Value.ContractId]) = {
        import elt.injshrink
        implicitly[Shrink[Option[elt.Inj]]]
      }
    }

    def map(elt: ValueAddend): Aux[SortedLookupList[elt.Inj]] = new ValueAddend {
      type Inj = SortedLookupList[elt.Inj]
      override val t = TypePrim(PT.TextMap, ImmArraySeq(elt.t))
      override def inj(sll: SortedLookupList[elt.Inj]) =
        ValueTextMap(sll map (elt.inj(_)))
      override def prj = {
        case ValueTextMap(sll) => sll traverse elt.prj
        case _ => None
      }
      override def injord = {
        implicit val e: Order[elt.Inj] = elt.injord
        Order[SortedLookupList[elt.Inj]]
      }
      override def injarb(implicit cid: Arbitrary[Value.ContractId]) = {
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        Tag unsubst implicitly[Arbitrary[SortedLookupList[elt.Inj] @@ Div3]]
      }
      override def injshrink(implicit shr: Shrink[Value.ContractId]) =
        Shrink.shrinkAny // XXX descend
    }

    def genMap(key: ValueAddend, elt: ValueAddend): ValueAddend {
      type Inj = key.Inj Map elt.Inj
    } = new ValueAddend {
      type Inj = key.Inj Map elt.Inj
      override val t = TypePrim(PT.GenMap, ImmArraySeq(key.t, elt.t))
      override def inj(m: key.Inj Map elt.Inj) =
        ValueGenMap {
          import key.{injord => keyorder}
          implicit val skeyord: math.Ordering[key.Inj] = Order[key.Inj].toScalaOrdering
          m.to(ImmArraySeq)
            .sortBy(_._1)
            .map { case (k, v) => (key.inj(k), elt.inj(v)) }
            .toImmArray
        }
      override def prj = {
        case ValueGenMap(kvs) =>
          kvs traverse (_.bitraverse(key.prj, elt.prj)) map (_.toSeq.toMap)
        case _ => None
      }
      override def injord = {
        implicit val k: Order[key.Inj] = key.injord
        implicit val ki: math.Ordering[key.Inj] = k.toScalaOrdering
        implicit val e: Order[elt.Inj] = elt.injord
        // for compatibility with SValue ordering
        Order[ImmArray[(key.Inj, elt.Inj)]] contramap { m =>
          m.to(ImmArraySeq).sortBy(_._1).toImmArray
        }
      }
      override def injarb(implicit arb: Arbitrary[Value.ContractId]) = {
        implicit val k: Arbitrary[key.Inj] = key.injarb
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        Tag unsubst implicitly[Arbitrary[key.Inj Map elt.Inj @@ Div3]]
      }
      override def injshrink(implicit shr: Shrink[Value.ContractId]) = {
        import key.{injshrink => keyshrink}, elt.{injshrink => eltshrink}
        implicitly[Shrink[key.Inj Map elt.Inj]]
      }
    }

    /** See [[RecVarSpec]] companion for usage examples. */
    def record(name: Ref.Identifier, spec: RecVarSpec): (DefDataType.FWT, Aux[spec.HRec]) =
      (
        DefDataType(ImmArraySeq.empty, Record(spec.t.to(ImmArraySeq))),
        new ValueAddend {
          private[this] val lfvFieldNames = spec.t map { case (n, _) => Some(n) }
          type Inj = spec.HRec
          override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
          override def inj(hl: Inj) =
            ValueRecord(
              Some(name),
              implicitly[
                Factory[(Some[Ref.Name], Value), ImmArray[(Some[Ref.Name], Value)]]
              ]
                .fromSpecific(lfvFieldNames zip spec.injRec(hl)),
            )
          override def prj = {
            case ValueRecord(_, fields) if fields.length == spec.t.length =>
              spec.prjRec(fields)
            case _ => None
          }
          override def injord = spec.record
          override def injarb(implicit cid: Arbitrary[Value.ContractId]) = spec.recarb
          override def injshrink(implicit shr: Shrink[Value.ContractId]) = spec.recshrink
        },
      )

    /** See [[RecVarSpec]] companion for usage examples. */
    def variant(name: Ref.Identifier, spec: RecVarSpec): (DefDataType.FWT, Aux[spec.HVar]) =
      (
        DefDataType(ImmArraySeq.empty, Variant(spec.t.to(ImmArraySeq))),
        new ValueAddend {
          type Inj = spec.HVar
          override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
          override def inj(cp: Inj) = {
            val (ctor, v) = spec.injVar(cp)
            ValueVariant(Some(name), ctor, v)
          }
          override def prj = {
            case ValueVariant(_, name, vv) =>
              spec.prjVar get name flatMap (_(vv))
            case _ => None
          }
          override def injord = spec.varord
          override def injarb(implicit cid: Arbitrary[Value.ContractId]) =
            Arbitrary(Gen.oneOf(spec.vararb.toSeq).flatMap(_._2))
          override def injshrink(implicit shr: Shrink[Value.ContractId]) = spec.varshrink
        },
      )

    def enum(
        name: Ref.Identifier,
        members: Seq[Ref.Name],
    ): (DefDataType.FWT, EnumAddend[members.type]) =
      (
        DefDataType(ImmArraySeq.empty, Enum(members.to(ImmArraySeq))),
        new EnumAddend[members.type] {
          type Member = Ref.Name
          override val values = members
          override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
          override def inj(v: Inj) = ValueEnum(Some(name), v)
          override def prj = {
            case ValueEnum(_, dc) => get(dc)
            case _ => None
          }
          override def injord = Order.orderBy(values.indexOf)
          override def injarb(implicit cid: Arbitrary[Value.ContractId]) = Arbitrary(
            Gen.oneOf(values)
          )
          override def injshrink(implicit shr: Shrink[Value.ContractId]) =
            Shrink { ev =>
              if (!(values.headOption contains ev)) values.headOption.toStream
              else Stream.empty
            }
        },
      )

    sealed abstract class EnumAddend[+Values <: Seq[Ref.Name]] extends ValueAddend {
      type Inj = Member
      type Member <: Ref.Name
      val values: Values with Seq[Member]
      def get(m: Ref.Name): Option[Member] = values collectFirst { case v if m == v => v }
    }
  }

  sealed abstract class RecVarSpec { self =>
    import shapeless.{::, :+:, Coproduct, HList, Inl, Inr, Witness}
    import shapeless.labelled.{field, FieldType => :->>:}

    type HRec <: HList
    type HVar <: Coproduct
    def ::[K <: Symbol](h: K :->>: ValueAddend)(implicit ev: Witness.Aux[K]): RecVarSpec {
      type HRec = (K :->>: h.Inj) :: self.HRec
      type HVar = (K :->>: h.Inj) :+: self.HVar
    } =
      new RecVarSpec {
        private[this] val fname = Ref.Name assertFromString ev.value.name
        type HRec = (K :->>: h.Inj) :: self.HRec
        type HVar = (K :->>: h.Inj) :+: self.HVar
        override val t = (fname, h.t) :: self.t
        override def injRec(v: HRec) =
          h.inj(v.head) :: self.injRec(v.tail)
        override def prjRec(v: ImmArray[(_, Value)]) = v match {
          case ImmArrayCons(vh, vt) =>
            for {
              pvh <- h.prj(vh._2)
              pvt <- self.prjRec(vt)
            } yield field[K](pvh) :: pvt
          case _ => None
        }

        override def record = {
          import h.{injord => hord}, self.{record => tailord}
          Order.orderBy { case ah :: at => (ah: h.Inj, at) }
        }

        override def recarb(implicit cid: Arbitrary[Value.ContractId]) = {
          import self.{recarb => tailarb}, h.{injarb => headarb}
          Arbitrary(arbitrary[(h.Inj, self.HRec)] map { case (vh, vt) =>
            field[K](vh) :: vt
          })
        }

        override def recshrink(implicit shr: Shrink[Value.ContractId]): Shrink[HRec] = {
          import h.{injshrink => hshrink}, self.{recshrink => tshrink}
          Shrink { case vh :: vt =>
            (Shrink.shrink(vh: h.Inj) zip Shrink.shrink(vt)) map { case (nh, nt) =>
              field[K](nh) :: nt
            }
          }
        }

        override def injVar(v: HVar) = v match {
          case Inl(hv) => (fname, h.inj(hv))
          case Inr(tl) => self.injVar(tl)
        }

        override val prjVar = {
          val r = self.prjVar transform { (_, tf) => tv: Value => tf(tv) map (Inr(_)) }
          r.updated(
            fname,
            (hv: Value) => h.prj(hv) map (pv => Inl(field[K](pv))),
          )
        }

        override def varord =
          (a, b) =>
            (a, b) match {
              case (Inr(at), Inr(bt)) => self.varord.order(at, bt)
              case (Inl(_), Inr(_)) => Ordering.LT
              case (Inr(_), Inl(_)) => Ordering.GT
              case (Inl(ah), Inl(bh)) => h.injord.order(ah, bh)
            }

        override def vararb(implicit cid: Arbitrary[Value.ContractId]) = {
          val r =
            self.vararb transform { (_, ta) =>
              ta map (Inr(_))
            }
          r.updated(
            fname, {
              import h.{injarb => harb}
              arbitrary[h.Inj] map (hv => Inl(field[K](hv)))
            },
          )
        }

        override def varshrink = {
          val lshr: Shrink[h.Inj] = h.injshrink
          val rshr: Shrink[self.HVar] = self.varshrink
          Shrink {
            case Inl(hv) => lshr shrink hv map (shv => Inl(field[K](shv)))
            case Inr(tl) => rshr shrink tl map (Inr(_))
          }
        }
      }

    private[TypedValueGenerators] val t: List[(Ref.Name, Type)]
    private[TypedValueGenerators] def injRec(v: HRec): List[Value]
    private[TypedValueGenerators] def prjRec(v: ImmArray[(_, Value)]): Option[HRec]
    private[TypedValueGenerators] implicit def record: Order[HRec]
    private[TypedValueGenerators] implicit def recarb(implicit
        cid: Arbitrary[Value.ContractId]
    ): Arbitrary[HRec]
    private[TypedValueGenerators] implicit def recshrink(implicit
        shr: Shrink[Value.ContractId]
    ): Shrink[HRec]

    private[TypedValueGenerators] def injVar(v: HVar): (Ref.Name, Value)
    private[TypedValueGenerators] type PrjResult = Option[HVar]
    private[TypedValueGenerators] val prjVar: Map[Ref.Name, Value => PrjResult]
    private[TypedValueGenerators] implicit def varord: Order[HVar]
    private[TypedValueGenerators] implicit def vararb(implicit
        cid: Arbitrary[Value.ContractId]
    ): Map[Ref.Name, Gen[HVar]]
    private[TypedValueGenerators] implicit def varshrink: Shrink[HVar]
  }

  case object RNil extends RecVarSpec {
    import shapeless.{HNil, CNil}
    type HRec = HNil
    type HVar = CNil
    private[TypedValueGenerators] override val t = List.empty
    private[TypedValueGenerators] override def injRec(v: HNil) = List.empty
    private[TypedValueGenerators] override def prjRec(v: ImmArray[(_, Value)]) =
      Some(HNil)
    private[TypedValueGenerators] override def record = (_, _) => Ordering.EQ
    private[TypedValueGenerators] override def recarb(implicit cid: Arbitrary[Value.ContractId]) =
      Arbitrary(Gen const HNil)
    private[TypedValueGenerators] override def recshrink(implicit shr: Shrink[Value.ContractId]) =
      Shrink.shrinkAny

    private[TypedValueGenerators] override def injVar(v: CNil) = v.impossible
    private[TypedValueGenerators] override val prjVar = Map.empty
    private[TypedValueGenerators] override def varord = (v, _) => v.impossible
    private[TypedValueGenerators] override def vararb(implicit cid: Arbitrary[Value.ContractId]) =
      Map.empty
    private[TypedValueGenerators] override def varshrink = Shrink.shrinkAny
  }

  private[value] object RecVarSpec {
    // specifying records and variants works the same way: a
    // record written with ->> and ::, terminated with RNil (*not* HNil)
    val sample = {
      import shapeless.syntax.singleton._
      Symbol("foo") ->> ValueAddend.int64 :: Symbol("bar") ->> ValueAddend.text :: RNil
    }

    // a RecVarSpec can be turned into a ValueAddend for records
    val (sampleRecordDDT, sampleAsRecord) =
      ValueAddend.record(
        Ref.Identifier(
          Ref.PackageId assertFromString "hash",
          Ref.QualifiedName assertFromString "Foo.SomeRecord",
        ),
        sample,
      )
    import shapeless.record.Record
    // You can ascribe a matching value
    // using either the spec,
    val sampleData: sample.HRec =
      Record(foo = 42L, bar = "hi")
    // or the record VA
    val sampleDataAgain: sampleAsRecord.Inj = sampleData
    // ascription is not necessary; a correct `Record` expression already
    // has the correct type, as implicit conversion is not used at all

    // unlike most `name = value` calls in Scala, `Record` is order-sensitive,
    // just as Daml-LF record values are order-sensitive. Most tests should preserve
    // this behavior, but if you want to automatically reorder the keys (for a runtime cost!)
    // you can do so with `align`. The resulting error messages are far worse, so
    // I recommend just writing them in the correct order
    shapeless.test
      .illTyped("""Record(bar = "bye", foo = 84L): sample.HRec""", "type mismatch.*")
    val backwardsSampleData: sample.HRec =
      Record(bar = "bye", foo = 84L).align[sample.HRec]

    // a RecVarSpec can be turned into a ValueAddend for variants
    val (sampleVariantDDT, sampleAsVariant) =
      ValueAddend.variant(
        Ref.Identifier(
          Ref.PackageId assertFromString "hash",
          Ref.QualifiedName assertFromString "Foo.SomeVariant",
        ),
        sample,
      )
    // You can create a matching value with Coproduct
    import shapeless.Coproduct, shapeless.syntax.singleton._
    val sampleVt =
      Coproduct[sample.HVar](Symbol("foo") ->> 42L)
    val anotherSampleVt =
      Coproduct[sample.HVar](Symbol("bar") ->> "hi")
    // and the `variant` function produces Inj as a synonym for HVar
    // just as `record` makes it a synonym for HRec
    val samples: List[sampleAsVariant.Inj] = List(sampleVt, anotherSampleVt)
    // Coproduct can be factored out, but the implicit resolution means you cannot
    // turn this into the obvious `map` call
    val sampleCp = Coproduct[sample.HVar]
    val moreSamples = List(sampleCp(Symbol("foo") ->> 84L), sampleCp(Symbol("bar") ->> "bye"))
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
      f: (Gen[ValueAddend], Gen[ValueAddend]) => Seq[Gen[ValueAddend]]
  ): Gen[ValueAddend] = {
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
            (nestSize, self.map(ValueAddend.optional(_))),
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
  def genTypeAndValue(cid: Gen[Value.ContractId]): Gen[(Type, Value)] =
    for {
      addend <- genAddend
      value <- addend.injarb(Arbitrary(cid)).arbitrary
    } yield (addend.t, addend.inj(value))
}
