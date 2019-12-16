// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package value

import scala.language.higherKinds
import data.{FrontStack, ImmArray, ImmArrayCons, Numeric, Ref, SortedLookupList, Time}
import ImmArray.ImmArraySeq
import data.DataArbitrary._
import iface.{
  DefDataType,
  Record,
  Type,
  TypeCon,
  TypeConName,
  TypeNumeric,
  TypePrim,
  PrimType => PT,
  Variant,
}

import scalaz.~>
import scalaz.Id.Id
import scalaz.syntax.traverse._
import scalaz.std.option._
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
    val bool = noCid(PT.Bool, ValueBool(_)) { case ValueBool(b) => b }
    val party = noCid(PT.Party, ValueParty) { case ValueParty(p) => p }

    def numeric(scale: Numeric.Scale): NoCid[Numeric] = new ValueAddend {
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
      override val t = TypePrim(PT.TextMap, ImmArraySeq(elt.t))
      override def inj[Cid] =
        (sll: SortedLookupList[elt.Inj[Cid]]) => ValueTextMap(sll map elt.inj)
      override def prj[Cid] = {
        case ValueTextMap(sll) => sll traverse elt.prj
        case _ => None
      }
      override def injarb[Cid: Arbitrary] = {
        implicit val e: Arbitrary[elt.Inj[Cid]] = elt.injarb
        implicitly[Arbitrary[SortedLookupList[elt.Inj[Cid]]]]
      }
      override def injshrink[Cid: Shrink] = Shrink.shrinkAny // XXX descend
    }

    /** See [[RecordVa]] companion for usage examples. */
    def record(name: Ref.Identifier, rec: RecordVa): (DefDataType.FWT, Aux[rec.HRec]) =
      (DefDataType(ImmArraySeq.empty, Record(rec.t.to[ImmArraySeq])), new ValueAddend {
        private[this] val lfvFieldNames = rec.t map { case (n, _) => Some(n) }
        type Inj[Cid] = rec.HRec[Cid]
        override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
        override def inj[Cid] =
          hl => ValueRecord(Some(name), (lfvFieldNames zip rec.inj(hl)).to[ImmArray])
        override def prj[Cid] = {
          case ValueRecord(_, fields) if fields.length == rec.t.length =>
            rec.prj(fields)
          case _ => None
        }
        override def injarb[Cid: Arbitrary] = rec.injarb[Cid]
        override def injshrink[Cid: Shrink] = rec.injshrink
      })

    /** See [[RecordVa]] companion for usage examples. */
    def variant(name: Ref.Identifier, spec: RecordVa): (DefDataType.FWT, Aux[spec.HVar]) =
      (DefDataType(ImmArraySeq.empty, Variant(spec.t.to[ImmArraySeq])), new ValueAddend {
        type Inj[Cid] = spec.HVar[Cid]
        override val t = TypeCon(TypeConName(name), ImmArraySeq.empty)
        override def inj[Cid] = { cp =>
          val (ctor, v) = spec.injVar(cp)
          ValueVariant(Some(name), ctor, v)
        }
        override def prj[Cid] = {
          case ValueVariant(_, name, vv) =>
            spec.prjVar get name flatMap (_(vv))
          case _ => None
        }
        override def injarb[Cid: Arbitrary] =
          Arbitrary(Gen.oneOf(spec.vararb[Cid].toSeq).flatMap(_._2))
        override def injshrink[Cid: Shrink] = spec.varshrink
      })
  }

  sealed abstract class RecordVa { self =>
    import shapeless.{::, :+:, Coproduct, HList, Inl, Inr, Witness}
    import shapeless.labelled.{field, FieldType => :->>:}

    type HRec[Cid] <: HList
    type HVar[Cid] <: Coproduct
    def ::[K <: Symbol](h: K :->>: ValueAddend)(implicit ev: Witness.Aux[K]): RecordVa {
      type HRec[Cid] = (K :->>: h.Inj[Cid]) :: self.HRec[Cid]
      type HVar[Cid] = (K :->>: h.Inj[Cid]) :+: self.HVar[Cid]
    } =
      new RecordVa {
        private[this] val fname = Ref.Name assertFromString ev.value.name
        type HRec[Cid] = (K :->>: h.Inj[Cid]) :: self.HRec[Cid]
        type HVar[Cid] = (K :->>: h.Inj[Cid]) :+: self.HVar[Cid]
        override val t = (fname, h.t) :: self.t
        override def inj[Cid](v: HRec[Cid]) =
          h.inj(v.head) :: self.inj(v.tail)
        override def prj[Cid](v: ImmArray[(_, Value[Cid])]) = v match {
          case ImmArrayCons(vh, vt) =>
            for {
              pvh <- h.prj(vh._2)
              pvt <- self.prj(vt)
            } yield field[K](pvh) :: pvt
          case _ => None
        }
        override def injarb[Cid: Arbitrary] = {
          import self.{injarb => tailarb}, h.{injarb => headarb}
          Arbitrary(arbitrary[(h.Inj[Cid], self.HRec[Cid])] map {
            case (vh, vt) =>
              field[K](vh) :: vt
          })
        }

        override def injshrink[Cid: Shrink]: Shrink[HRec[Cid]] = {
          import h.{injshrink => hshrink}, self.{injshrink => tshrink}
          Shrink {
            case vh :: vt =>
              (Shrink.shrink(vh: h.Inj[Cid]) zip Shrink.shrink(vt)) map {
                case (nh, nt) => field[K](nh) :: nt
              }
          }
        }

        override def injVar[Cid](v: HVar[Cid]) = v match {
          case Inl(hv) => (fname, h.inj(hv))
          case Inr(tl) => self.injVar(tl)
        }

        @SuppressWarnings(Array("org.wartremover.warts.Any"))
        override val prjVar = self.prjVar transform { (_, tf) =>
          Lambda[Value ~> PrjResult](tv => tf(tv) map (Inr(_)))
        } updated (fname, Lambda[Value ~> PrjResult](hv => h.prj(hv) map (pv => Inl(field[K](pv)))))

        override def vararb[Cid: Arbitrary] =
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
    private[TypedValueGenerators] def inj[Cid](v: HRec[Cid]): List[Value[Cid]]
    private[TypedValueGenerators] def prj[Cid](v: ImmArray[(_, Value[Cid])]): Option[HRec[Cid]]
    private[TypedValueGenerators] implicit def injarb[Cid: Arbitrary]: Arbitrary[HRec[Cid]]
    private[TypedValueGenerators] implicit def injshrink[Cid: Shrink]: Shrink[HRec[Cid]]

    private[TypedValueGenerators] def injVar[Cid](v: HVar[Cid]): (Ref.Name, Value[Cid])
    private[TypedValueGenerators] type PrjResult[Cid] = Option[HVar[Cid]]
    // could be made more efficient by replacing ~> with a Nat GADT,
    // but the :+: case is tricky enough as it is
    private[TypedValueGenerators] val prjVar: Map[Ref.Name, Value ~> PrjResult]
    private[TypedValueGenerators] implicit def vararb[Cid: Arbitrary]: Map[Ref.Name, Gen[HVar[Cid]]]
    private[TypedValueGenerators] implicit def varshrink[Cid: Shrink]: Shrink[HVar[Cid]]
  }

  case object RNil extends RecordVa {
    import shapeless.{HNil, CNil}
    type HRec[Cid] = HNil
    type HVar[Cid] = CNil
    private[TypedValueGenerators] override val t = List.empty
    private[TypedValueGenerators] override def inj[Cid](v: HNil) = List.empty
    private[TypedValueGenerators] override def prj[Cid](v: ImmArray[(_, Value[Cid])]) =
      Some(HNil)
    private[TypedValueGenerators] override def injarb[Cid: Arbitrary] =
      Arbitrary(Gen const HNil)
    private[TypedValueGenerators] override def injshrink[Cid: Shrink] =
      Shrink.shrinkAny

    private[TypedValueGenerators] override def injVar[Cid](v: CNil) = v.impossible
    private[TypedValueGenerators] override val prjVar = Map.empty
    private[TypedValueGenerators] override def vararb[Cid: Arbitrary] = Map.empty
    private[TypedValueGenerators] override def varshrink[Cid: Shrink] = Shrink.shrinkAny
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private[value] object RecordVa {
    // specifying records and variants works the same way: a
    // record written with ->> and ::, terminated with RNil (*not* HNil)
    val sample = {
      import shapeless.syntax.singleton._
      'foo ->> ValueAddend.int64 :: 'bar ->> ValueAddend.text :: RNil
    }

    // a RecordVa can be turned into a ValueAddend for records
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

    // a RecordVa can be turned into a ValueAddend for variants
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
    val nestSize = sz / 3
    Gen.frequency(
      ((sz max 1) * ValueAddend.leafInstances.length, Gen.oneOf(ValueAddend.leafInstances)),
      (sz max 1, Gen.const(ValueAddend.contractId)),
      (sz max 1, Gen.oneOf(Numeric.Scale.values).map(ValueAddend.numeric)),
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
