// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml
package lf
package value
package test

import data.{FrontStack, ImmArray, ImmArrayCons, Numeric, Ref, SortedLookupList, Time}
import ImmArray.ImmArraySeq
import data.DataArbitrary._
import typesig.{
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
    implicit def injarb: Arbitrary[Inj]
    implicit def injshrink: Shrink[Inj]
    final override def toString = s"${classOf[ValueAddend].getSimpleName}{t = ${t.toString}}"

    final def xmap[B](f: Inj => B)(g: B => Inj): ValueAddend.Aux[B] =
      new ValueAddend.XMapped[Inj, B](this, f, g)
  }

  object ValueAddend extends PrimInstances[Lambda[a => ValueAddend { type Inj = a }]] {
    import shapeless.HList

    type Aux[Inj0] = ValueAddend {
      type Inj = Inj0
    }
    @deprecated("use Aux instead", since = "2.2.0")
    type NoCid[Inj0] = Aux[Inj0]

    private sealed abstract class Leaf0[Inj0](implicit
        ord: Order[Inj0],
        arb: Arbitrary[Inj0],
        shr: Shrink[Inj0],
    ) extends ValueAddend {
      type Inj = Inj0
      override final def injord = ord
      override final def injarb = arb
      override final def injshrink = shr
    }

    private def leaf[Inj0: Order: Arbitrary: Shrink](pt: PT, inj0: Inj0 => Value)(
        prj0: Value PartialFunction Inj0
    ): Aux[Inj0] = new Leaf0[Inj0] {
      override val t = TypePrim(pt, ImmArraySeq.empty)
      override def inj(v: Inj0) = inj0(v)
      override def prj = prj0.lift
    }

    import Value._, ValueGenerators.Implicits._, data.Utf8.ImplicitOrder._
    import scalaz.std.anyVal._

    val text = leaf(PT.Text, ValueText) { case ValueText(t) => t }
    val int64 = leaf(PT.Int64, ValueInt64) { case ValueInt64(i) => i }
    val unit = leaf(PT.Unit, (_: Unit) => ValueUnit) { case ValueUnit => () }
    val date = leaf(PT.Date, ValueDate) { case ValueDate(d) => d }
    val timestamp = leaf(PT.Timestamp, ValueTimestamp) { case ValueTimestamp(t) => t }
    val bool = leaf(PT.Bool, ValueBool(_)) { case ValueBool(b) => b }
    val party = leaf(PT.Party, ValueParty) { case ValueParty(p) => p }

    def numeric(scale: Numeric.Scale): Aux[Numeric] = {
      implicit val arb: Arbitrary[Numeric] = Arbitrary(ValueGenerators.numGen(scale))
      new Leaf0[Numeric] {
        override def t: Type = TypeNumeric(scale)

        override def inj(x: Numeric): Value =
          ValueNumeric(Numeric.assertFromBigDecimal(scale, x))

        override def prj: Value => Option[Numeric] = {
          case ValueNumeric(x) => Numeric.fromBigDecimal(scale, x).toOption
          case _ => None
        }
      }
    }

    // we limit contract id generation to comparable cids in some places
    def contractId(implicit cid: Arbitrary[Value.ContractId]): Aux[ContractId] =
      new Leaf0[ContractId] {
        // TODO SC it probably doesn't make much difference for our initial use case,
        // but the proper arg should probably end up here, not Unit
        override val t = TypePrim(PT.ContractId, ImmArraySeq(TypePrim(PT.Unit, ImmArraySeq.empty)))
        override def inj(v: ContractId) = ValueContractId(v)
        override def prj = {
          case ValueContractId(cid) => Some(cid)
          case _ => None
        }
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
      override def injarb = {
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        Tag unsubst implicitly[Arbitrary[Vector[elt.Inj] @@ Div3]]
      }
      override def injshrink = implicitly[Shrink[Vector[elt.Inj]]]
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
      override def injarb = {
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        implicitly[Arbitrary[Option[elt.Inj]]]
      }
      override def injshrink = implicitly[Shrink[Option[elt.Inj]]]
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
      override def injarb = {
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        Tag unsubst implicitly[Arbitrary[SortedLookupList[elt.Inj] @@ Div3]]
      }
      override def injshrink =
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
      override def injarb = {
        implicit val k: Arbitrary[key.Inj] = key.injarb
        implicit val e: Arbitrary[elt.Inj] = elt.injarb
        Tag unsubst implicitly[Arbitrary[key.Inj Map elt.Inj @@ Div3]]
      }
      override def injshrink = {
        import key.{injshrink => keyshrink}, elt.{injshrink => eltshrink}
        implicitly[Shrink[key.Inj Map elt.Inj]]
      }
    }

    /** See [[RecVarExample]] for usage examples. */
    def record[Spec <: HList](name: Ref.Identifier, fields: Spec)(implicit
        rvs: RecVarSpec[Spec]
    ): (DefDataType.FWT, Aux[rvs.HRec]) = {
      val spec = rvs configure fields
      (
        DefDataType(ImmArraySeq.empty, Record(spec.t.to(ImmArraySeq))),
        new ValueAddend {
          private[this] val lfvFieldNames = spec.t map { case (n, _) => Some(n) }
          type Inj = rvs.HRec
          override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
          override def inj(hl: Inj) =
            ValueRecord(
              Some(name),
              (lfvFieldNames zip spec.injRec(hl)).to(ImmArray),
            )
          override def prj = {
            case ValueRecord(_, fields) if fields.length == spec.t.length =>
              spec.prjRec(fields)
            case _ => None
          }
          override def injord = spec.record
          override def injarb = spec.recarb
          override def injshrink = spec.recshrink
        },
      )
    }

    /** See [[RecVarExample]] companion for usage examples. */
    def variant[Spec <: HList](name: Ref.Identifier, constructors: Spec)(implicit
        rvs: RecVarSpec[Spec]
    ): (DefDataType.FWT, Aux[rvs.HVar]) = {
      val spec = rvs configure constructors
      (
        DefDataType(ImmArraySeq.empty, Variant(spec.t.to(ImmArraySeq))),
        new ValueAddend {
          type Inj = rvs.HVar
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
          override def injarb =
            Arbitrary(Gen.oneOf(spec.vararb.toSeq).flatMap(_._2))
          override def injshrink = spec.varshrink
        },
      )
    }

    def enumeration(
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
          override def injarb = Arbitrary(
            Gen.oneOf(values)
          )
          override def injshrink =
            Shrink { ev =>
              if (!(values.headOption contains ev)) values.headOption.toStream
              else Stream.empty: @annotation.nowarn("cat=deprecation")
            }
        },
      )

    sealed abstract class EnumAddend[+Values <: Seq[Ref.Name]] extends ValueAddend {
      type Inj = Member
      type Member <: Ref.Name
      val values: Values with Seq[Member]
      def get(m: Ref.Name): Option[Member] = values collectFirst { case v if m == v => v }
    }

    private final class XMapped[Under, Inj0](under: Aux[Under], f: Under => Inj0, g: Inj0 => Under)
        extends ValueAddend {
      type Inj = Inj0
      override def t = under.t
      override def inj(v: Inj) = under.inj(g(v))
      override def prj = under.prj andThen (_ map f)

      override def injord = under.injord contramap g
      override def injarb =
        Arbitrary(under.injarb.arbitrary map f)
      override def injshrink =
        Shrink.xmap(f, g)(under.injshrink)
    }
  }

  @deprecated("use shapeless.HNil instead", since = "2.4.0")
  private[daml] val RNil: shapeless.HNil.type = shapeless.HNil

  sealed abstract class RecVarSpec[-In] { self =>
    import shapeless.{Coproduct, HList}

    type HRec <: HList
    type HVar <: Coproduct

    private[TypedValueGenerators] def configure(in: In): Rules
    private[TypedValueGenerators] sealed abstract class Rules {
      val t: List[(Ref.Name, Type)]

      def injRec(v: HRec): List[Value]
      def prjRec(v: ImmArray[(_, Value)]): Option[HRec]
      implicit def record: Order[HRec]
      implicit def recarb: Arbitrary[HRec]
      implicit def recshrink: Shrink[HRec]

      def injVar(v: HVar): (Ref.Name, Value)
      type PrjResult = Option[HVar]
      val prjVar: Map[Ref.Name, Value => PrjResult]
      implicit def varord: Order[HVar]
      implicit def vararb: Map[Ref.Name, Gen[HVar]]
      implicit def varshrink: Shrink[HVar]
    }
  }

  object RecVarSpec {
    import shapeless.{::, :+:, CNil, Coproduct, HList, HNil, Inl, Inr, Witness}
    import shapeless.labelled.{field, FieldType => :->>:}

    type Aux[In, HRec0 <: HList, HVar0 <: Coproduct] = RecVarSpec[In] {
      type HRec = HRec0
      type HVar = HVar0
    }

    implicit val rvsHnil: Aux[HNil, HNil, CNil] = new RecVarSpec[HNil] {
      type HRec = HNil
      type HVar = CNil
      override def configure(in: HNil): Rules = new Rules {
        override val t = List.empty

        override def injRec(v: HNil) = List.empty
        override def prjRec(v: ImmArray[(_, Value)]) =
          Some(HNil)
        override def record = (_, _) => Ordering.EQ
        override def recarb =
          Arbitrary(Gen const HNil)
        override def recshrink =
          Shrink.shrinkAny

        override def injVar(v: CNil) = v.impossible
        override val prjVar = Map.empty
        override def varord = (v, _) => v.impossible
        override def vararb = Map.empty
        override def varshrink = Shrink.shrinkAny
      }
    }

    implicit def rvsHcons[KS <: Symbol, KT, Tl <: HList](implicit
        ev: Witness.Aux[KS],
        TL: RecVarSpec[Tl],
    ): Aux[(KS :->>: ValueAddend.Aux[
      KT
    ]) :: Tl, (KS :->>: KT) :: TL.HRec, (KS :->>: KT) :+: TL.HVar] =
      new RecVarSpec[(KS :->>: ValueAddend.Aux[KT]) :: Tl] {
        type HRec = (KS :->>: KT) :: TL.HRec
        type HVar = (KS :->>: KT) :+: TL.HVar
        private[this] val fname = Ref.Name assertFromString ev.value.name
        override def configure(in: (KS :->>: ValueAddend.Aux[KT]) :: Tl): Rules = new Rules {
          private[this] val hVA :: tlVAs = in
          private[this] val tlRules = TL configure tlVAs
          type K = KS

          override val t = (fname, hVA.t) :: tlRules.t

          override def injRec(v: HRec) =
            hVA.inj(v.head) :: tlRules.injRec(v.tail)

          override def prjRec(v: ImmArray[(_, Value)]) = v match {
            case ImmArrayCons(vh, vt) =>
              for {
                pvh <- hVA.prj(vh._2)
                pvt <- tlRules.prjRec(vt)
              } yield field[K](pvh) :: pvt
            case _ => None
          }

          override def record = {
            import hVA.{injord => hord}, tlRules.{record => tailord}
            Order.orderBy { case ah :: at => (ah: hVA.Inj, at) }
          }

          override def recarb = {
            import tlRules.{recarb => tailarb}, hVA.{injarb => headarb}
            Arbitrary(arbitrary[(hVA.Inj, TL.HRec)] map { case (vh, vt) =>
              field[K](vh) :: vt
            })
          }

          override def recshrink: Shrink[HRec] = {
            import hVA.{injshrink => hshrink}, tlRules.{recshrink => tshrink}
            Shrink { case vh :: vt =>
              (Shrink.shrink(vh: hVA.Inj) zip Shrink.shrink(vt)) map { case (nh, nt) =>
                field[K](nh) :: nt
              }
            }
          }

          override def injVar(v: HVar) = v match {
            case Inl(hv) => (fname, hVA.inj(hv))
            case Inr(tl) => tlRules.injVar(tl)
          }

          override val prjVar: Map[Ref.Name, Value => PrjResult] = {
            val r = tlRules.prjVar transform { (_, tf) => tv: Value => tf(tv) map (Inr(_)) }
            r.updated(
              fname,
              (hv: Value) => hVA.prj(hv) map (pv => Inl(field[K](pv))),
            )
          }

          override def varord =
            (a, b) =>
              (a, b) match {
                case (Inr(at), Inr(bt)) => tlRules.varord.order(at, bt)
                case (Inl(_), Inr(_)) => Ordering.LT
                case (Inr(_), Inl(_)) => Ordering.GT
                case (Inl(ah), Inl(bh)) => hVA.injord.order(ah, bh)
              }

          override def vararb: Map[Ref.Name, Gen[HVar]] = {
            val r =
              tlRules.vararb transform { (_, ta) =>
                ta map (Inr(_))
              }
            r.updated(
              fname, {
                import hVA.{injarb => harb}
                arbitrary[hVA.Inj] map (hv => Inl(field[K](hv)))
              },
            )
          }

          override def varshrink = {
            val lshr: Shrink[hVA.Inj] = hVA.injshrink
            val rshr: Shrink[TL.HVar] = tlRules.varshrink
            Shrink {
              case Inl(hv) => lshr shrink hv map (shv => Inl(field[K](shv)))
              case Inr(tl) => rshr shrink tl map (Inr(_))
            }
          }
        }
      }
  }

  private[value] object RecVarExample {
    // specifying records and variants works the same way: a
    // record written with the Record macro
    val sample = {
      import shapeless.record.{Record => ShRecord}
      ShRecord(foo = ValueAddend.int64, bar = ValueAddend.text)
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
    // using the record ValueAddend
    val sampleData: sampleAsRecord.Inj =
      Record(foo = 42L, bar = "hi")
    // ascription is not necessary; a correct `Record` expression already
    // has the correct type, as implicit conversion is not used at all

    // unlike most `name = value` calls in Scala, `Record` is order-sensitive,
    // just as Daml-LF record values are order-sensitive. Most tests should preserve
    // this behavior, but if you want to automatically reorder the keys (for a runtime cost!)
    // you can do so with `align`. The resulting error messages are far worse, so
    // I recommend just writing them in the correct order
    shapeless.test
      .illTyped("""Record(bar = "bye", foo = 84L): sampleAsRecord.Inj""", "type mismatch.*")
    val backwardsSampleData: sampleAsRecord.Inj =
      Record(bar = "bye", foo = 84L).align[sampleAsRecord.Inj]

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
      Coproduct[sampleAsVariant.Inj](Symbol("foo") ->> 42L)
    val anotherSampleVt =
      Coproduct[sampleAsVariant.Inj](Symbol("bar") ->> "hi")
    // and the `variant` function produces Inj as a synonym for HVar
    // just as `record` makes it a synonym for HRec
    val samples: List[sampleAsVariant.Inj] = List(sampleVt, anotherSampleVt)
    // Coproduct can be factored out, but the implicit resolution means you cannot
    // turn this into the obvious `map` call
    val sampleCp = Coproduct[sampleAsVariant.Inj]
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
  )(implicit cid: Arbitrary[Value.ContractId]): Gen[ValueAddend] = {
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
  def genAddend(implicit cid: Arbitrary[Value.ContractId]): Gen[ValueAddend] =
    indGenAddend { (keySelf, self) =>
      Seq(
        self.map(ValueAddend.list(_)),
        self.map(ValueAddend.map(_)),
        Gen.zip(keySelf, self).map { case (k, v) => ValueAddend.genMap(k, v) },
      )
    }

  def genAddendNoListMap(implicit cid: Arbitrary[Value.ContractId]): Gen[ValueAddend] =
    indGenAddend((_, _) => Seq.empty)

  /** Generate a type and value guaranteed to conform to that type. */
  def genTypeAndValue(cid: Gen[Value.ContractId]): Gen[(Type, Value)] =
    for {
      addend <- genAddend(Arbitrary(cid))
      value <- addend.injarb.arbitrary
    } yield (addend.t, addend.inj(value))
}
