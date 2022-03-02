// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client
package binding
package encoding

import scala.collection.immutable.Map

import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scalaz.{Apply, OneAnd, ~>}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.std.vector._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._

import com.daml.ledger.api.v1.{value => rpcvalue}
import rpcvalue.Value.{Sum => VSum}
import binding.{Primitive => P}

class LfTypeEncodingSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import LfTypeEncodingSpec._

  /** The sanity check for [[LfTypeEncoding]]: can we produce something that works
    * just like our bespoke codegenned [[Value]]s, but in a purely tagless-final
    * way?
    */
  private def deriveSameValueAbstractlyAsConcretely[A](
      abstractly: Value[A],
      concretely: Value[A],
  )(implicit arbA: Arbitrary[A], shrA: Shrink[A]) =
    forAll { a: A =>
      val ca = concretely.write(a)
      concretely.read(ca) shouldBe Some(a)
      val aa = abstractly.write(a)
      ca shouldBe aa
      abstractly.read(aa) shouldBe Some(a)
    }

  "TrialVariant" should {
    "derive same Value abstractly as concretely" in deriveSameValueAbstractlyAsConcretely(
      abstractly = LfEncodable.encoding[TrialVariant[P.Int64, P.Text]](ValueLfTypeEncoding),
      concretely = TrialVariant.`TrialVariant value`[P.Int64, P.Text],
    )
  }

  "TrialSubRec" should {
    "derive same Value abstractly as concretely" in deriveSameValueAbstractlyAsConcretely(
      abstractly = LfEncodable.encoding[TrialSubRec[P.Int64]](ValueLfTypeEncoding),
      concretely = TrialSubRec.`TrialSubRec value`[P.Int64],
    )
  }

  "TrialEmptyRec" should {
    "derive same Value abstractly as concretely" in deriveSameValueAbstractlyAsConcretely(
      abstractly = LfEncodable.encoding[TrialEmptyRec](ValueLfTypeEncoding),
      concretely = TrialEmptyRec.`TrialEmptyRec value`,
    )
  }

  "CallablePayout" should {
    "derive same Value abstractly as concretely" in deriveSameValueAbstractlyAsConcretely(
      abstractly = LfEncodable.encoding[CallablePayout](ValueLfTypeEncoding),
      concretely = CallablePayout.`the template Value`,
    )
  }
}

object LfTypeEncodingSpec {
  import Arbitrary.arbitrary

  sealed abstract class TrialVariant[A, B] extends ValueRef

  object TrialVariant extends ValueRefCompanion {
    final case class TLeft[A, B](body: A) extends TrialVariant[A, B]
    final case class TRight[A, B](one: B, two: B) extends TrialVariant[A, B]

    override protected val ` dataTypeId` = rpcvalue.Identifier("hello", "Trial", "Variant")

    implicit def `TrialVariant arb`[A: Arbitrary, B: Arbitrary]: Arbitrary[TrialVariant[A, B]] =
      Arbitrary(
        Gen.oneOf(
          arbitrary[A] map (TLeft[A, B](_)),
          Gen.zip(arbitrary[B], arbitrary[B]) map ((TRight[A, B] _).tupled),
        )
      )

    implicit def `TrialVariant shrink`[A: Shrink, B: Shrink]: Shrink[TrialVariant[A, B]] =
      Shrink.xmap(
        (_: A Either (B, B)).fold(TLeft(_), { case (o, t) => TRight(o, t) }),
        {
          case TLeft(body) => Left(body)
          case TRight(one, two) => Right((one, two))
        },
      )

    implicit def `TrialVariant value`[A: Value, B: Value]: Value[TrialVariant[A, B]] =
      new `Value ValueRef`[TrialVariant[A, B]] {
        override def write(value: TrialVariant[A, B]) = value match {
          case TLeft(body) => ` variant`("TLeft", Value.encode(body))
          case TRight(one, two) =>
            ` createVariantOfSynthRecord`(
              "TRight",
              ("one", Value.encode(one)),
              ("two", Value.encode(two)),
            )
        }

        override def read(av: VSum) = av.variant flatMap { obj =>
          obj.constructor match {
            case "TLeft" => obj.value flatMap (Value.decode[A](_)) map (TLeft(_))
            case "TRight" =>
              obj.value flatMap (_.sum.record) flatMap { o2 =>
                o2.fields match {
                  case Seq(
                        rpcvalue.RecordField("" | "one", Some(one)),
                        rpcvalue.RecordField("" | "two", Some(two)),
                      ) =>
                    Apply[Option].apply2(Value.decode[B](one), Value.decode[B](two))(TRight(_, _))
                }
              }
            case _ => None
          }
        }
      }

    /** Example encoding of a variant type.  A variant has a significantly different
      * language for dealing with its "cases", as compared to records' "fields",
      * because the type alignment [and therefore the type alignment of backends
      * producing encodings for them] is very different.
      *
      * But the core idea is the same: feed information about the variant
      * structure to `lte`, building it up with combinators also supplied by
      * `lte`.
      */
    implicit def `TrialVariant LfEncodable`[A: LfEncodable, B: LfEncodable]
        : LfEncodable[TrialVariant[A, B]] =
      new LfEncodable[TrialVariant[A, B]] {
        override def encoding(lte: LfTypeEncoding): lte.Out[TrialVariant[A, B]] = {
          import lte.{field, fields}
          val tright: lte.RecordFields[TRight[A, B]] =
            lte.RecordFields.xmapN(
              fields(field("one", LfEncodable.encoding[B](lte))),
              fields(field("two", LfEncodable.encoding[B](lte))),
            )(TRight[A, B](_, _)) { case TRight(one, two) =>
              (one, two)
            }
          lte.variantAll(
            ` dataTypeId`,
            lte
              .variantCase[A, TrialVariant[A, B]]("TLeft", LfEncodable.encoding[A](lte))(TLeft(_)) {
                case TLeft(body) => body
              },
            lte.variantRecordCase[TRight[A, B], TrialVariant[A, B]](
              "TRight",
              ` dataTypeId`,
              tright,
            ) { case rec @ TRight(_, _) =>
              rec
            },
          )
        }
      }
  }

  final case class TrialSubRec[A](num: P.Int64, a: A) extends ValueRef

  object TrialSubRec extends ValueRefCompanion {
    override protected val ` dataTypeId` = rpcvalue.Identifier("hello", "Trial", "SubRec")

    implicit def `TrialSubRec arb`[A: Arbitrary]: Arbitrary[TrialSubRec[A]] =
      Arbitrary(arbitrary[(P.Int64, A)] map (TrialSubRec[A] _).tupled)

    implicit def `TrialSubRec value`[A: Value]: Value[TrialSubRec[A]] =
      new `Value ValueRef`[TrialSubRec[A]] {
        override def read(av: VSum) =
          av.record flatMap { r =>
            r.fields match {
              case Seq(
                    rpcvalue.RecordField("" | "num", Some(num)),
                    rpcvalue.RecordField("" | "a", Some(a)),
                  ) =>
                Apply[Option].apply2(Value.decode[P.Int64](num), Value.decode[A](a))(
                  TrialSubRec(_, _)
                )
              case _ => None
            }
          }

        override def write(value: TrialSubRec[A]) =
          ` record`(("num", Value.encode(value.num)), ("a", Value.encode(value.a)))
      }

    /** Example encoding of a record type.  Mostly like the template example below;
      * see that one first.  The two important differences are:
      *
      *  1. Evidence from running `lte` on the ''used'' type parameters must be
      *     supplied as extra arguments.
      *
      *  2. We don't bother producing a `View` with the fields.
      */
    implicit def `TrialSubRec LfEncodable`[A: LfEncodable]: LfEncodable[TrialSubRec[A]] =
      new LfEncodable[TrialSubRec[A]] {
        def encoding(lte: LfTypeEncoding): lte.Out[TrialSubRec[A]] = {
          import lte.{RecordFields, field, fields}
          val num = field("num", LfEncodable.encoding[P.Int64](lte))
          val a = field("a", LfEncodable.encoding[A](lte))
          lte.record(
            ` dataTypeId`,
            RecordFields.xmapN(fields(num), fields(a))(TrialSubRec(_, _)) {
              case TrialSubRec(num, a) => (num, a)
            },
          )
        }
      }
  }

  final case class TrialEmptyRec() extends ValueRef

  object TrialEmptyRec extends ValueRefCompanion {
    override protected val ` dataTypeId` = rpcvalue.Identifier("hello", "Trial", "SubRec")

    implicit val `TrialEmptyRec arb`: Arbitrary[TrialEmptyRec] =
      Arbitrary(Gen const TrialEmptyRec())

    implicit val `TrialEmptyRec value`: Value[TrialEmptyRec] =
      new `Value ValueRef`[TrialEmptyRec] {
        override def write(obj: TrialEmptyRec): VSum = ` record`()
        override def read(argumentValue: VSum): Option[TrialEmptyRec] =
          argumentValue.record map (_ => TrialEmptyRec())
      }

    implicit val `TrialEmptyRec LfEncodable`: LfEncodable[TrialEmptyRec] =
      new LfEncodable[TrialEmptyRec] {
        override def encoding(lte: LfTypeEncoding): lte.Out[TrialEmptyRec] =
          lte.emptyRecord(` dataTypeId`, () => TrialEmptyRec())
      }
  }

  final case class CallablePayout(
      receiver: P.Party,
      subr: TrialSubRec[P.Int64],
      lst: P.List[P.Int64],
      emptyRec: TrialEmptyRec,
      variant: TrialVariant[P.Text, P.ContractId[CallablePayout]],
  ) extends Template[CallablePayout] {
    protected[this] override def templateCompanion(implicit d: DummyImplicit) = CallablePayout
  }

  object CallablePayout extends TemplateCompanion[CallablePayout] {
    trait view[C[_]] extends RecordView[C, view] { vself =>
      val receiver: C[P.Party]
      val subr: C[TrialSubRec[P.Int64]]
      val lst: C[P.List[P.Int64]]
      val emptyRec: C[TrialEmptyRec]
      val variant: C[TrialVariant[P.Text, P.ContractId[CallablePayout]]]

      override final def hoist[D[_]](f: C ~> D): view[D] = new view[D] {
        override val receiver = f(vself.receiver)
        override val subr = f(vself.subr)
        override val lst = f(vself.lst)
        override val emptyRec = f(vself.emptyRec)
        override val variant = f(vself.variant)
      }
    }

    implicit val `CallablePayout arb`: Arbitrary[CallablePayout] = Arbitrary {
      implicit val PA: Arbitrary[P.Party] = Arbitrary(GenEncoding.primitive.valueParty)
      implicit val CA: Arbitrary[P.ContractId[CallablePayout]] =
        Arbitrary(GenEncoding.primitive.valueContractId)
      arbitrary[
        (
            P.Party,
            TrialSubRec[P.Int64],
            P.List[P.Int64],
            TrialEmptyRec,
            TrialVariant[P.Text, P.ContractId[CallablePayout]],
        )
      ] map (CallablePayout.apply _).tupled
    }

    override val id = ` templateId`("hello", "MyMain", "CallablePayout")

    override val consumingChoices = Set()

    override def toNamedArguments(cp: CallablePayout): rpcvalue.Record =
      ` arguments`(
        ("receiver", Value.encode(cp.receiver)),
        ("subr", Value.encode(cp.subr)),
        ("lst", Value.encode(cp.lst)),
        ("emptyRec", Value.encode(cp.emptyRec)),
        ("variant", Value.encode(cp.variant)),
      )

    override def fromNamedArguments(r: rpcvalue.Record): Option[CallablePayout] =
      r.fields match {
        case Seq(
              rpcvalue.RecordField("" | "receiver", Some(receiver)),
              rpcvalue.RecordField("" | "subr", Some(subr)),
              rpcvalue.RecordField("" | "lst", Some(lst)),
              rpcvalue.RecordField("" | "emptyRec", Some(emptyRec)),
              rpcvalue.RecordField("" | "variant", Some(variant)),
            ) =>
          Apply[Option].apply5(
            Value.decode[P.Party](receiver),
            Value.decode[TrialSubRec[P.Int64]](subr),
            Value.decode[P.List[P.Int64]](lst),
            Value.decode[TrialEmptyRec](emptyRec),
            Value.decode[TrialVariant[P.Text, P.ContractId[CallablePayout]]](variant),
          )(CallablePayout.apply)
        case _ => None
      }

    /** Example encoding of a template.  The job of this function is to feed all the
      * information about an ADT's specific structure to `lte` in a well-typed
      * way; `lte` is responsible for all the specifics of what is done at
      * runtime with that information.
      *
      * The first result supports special cases like Slick's query EDSL that
      * demand more well-typed views of the component parts of the encoding, but
      * can be ignored if it is not needed.  The second result is `lte`'s
      * encoding of this template, and might be [[Value]] for working with grpc,
      * or `AbstractTable` for working with Slick.
      *
      * This example shows what codegen might output for ''this exact''
      * template.  However, because the functions of `lte` are ordinary
      * functions, this can be implemented by interpreting LF structures
      * directly, instead.
      */
    def fieldEncoding(lte: LfTypeEncoding): view[lte.Field] = {
      import lte.{Field, field}
      new view[Field] {
        override val receiver = field("receiver", LfEncodable.encoding[P.Party](lte))
        override val subr =
          field("subr", LfEncodable.encoding[TrialSubRec[P.Int64]](lte))
        override val lst = field("lst", LfEncodable.encoding[P.List[P.Int64]](lte))
        override val emptyRec = field("emptyRec", LfEncodable.encoding[TrialEmptyRec](lte))
        override val variant =
          field(
            "variant",
            LfEncodable.encoding[TrialVariant[P.Text, P.ContractId[CallablePayout]]](lte),
          )
      }
    }

    def encoding(lte: LfTypeEncoding)(view: view[lte.Field]): lte.Out[CallablePayout] = {
      import lte.{RecordFields, fields}
      // it's not necessary for 4 arguments to use tuple nesting, but that
      // allows us to verify that we will treat larger record types properly
      // (i.e. associatively)
      val out: lte.Out[CallablePayout] =
        lte.record(
          ` dataTypeId`,
          RecordFields.xmapN(
            fields(view.receiver),
            fields(view.subr),
            fields(view.lst),
            RecordFields.tupleN(fields(view.emptyRec), fields(view.variant)),
          ) { case (r, s, l, (e, v)) =>
            CallablePayout(r, s, l, e, v)
          } { case CallablePayout(r, s, l, e, v) => (r, s, l, (e, v)) },
        )
      out
    }
  }

  /** An example encoding backend.  All the `encoding` functions above feed their
    * [abstract] [[LfTypeEncoding]] argument all the information about the
    * datatypes' structures; a backend such as this must collect that
    * information to produce a valid `Out` for each type.
    */
  private object ValueLfTypeEncoding extends LfTypeEncoding {
    type Out[A] = Value[A]

    type Field[A] = (String, Value[A])

    final case class RecordFields[A](
        fieldNames: Vector[String],
        writers: A => Vector[rpcvalue.Value],
        reader: Seq[rpcvalue.RecordField] => (Option[A], Seq[rpcvalue.RecordField]),
    )

    final case class VariantCases[A](
        readers: Map[String, VSum => Option[A]],
        writers: Vector[(String, A => Option[VSum])],
    )

    override def record[A](recordId: rpcvalue.Identifier, fi: RecordFields[A]): Out[A] =
      new Value.InternalImpl[A] {
        override def write(a: A): VSum =
          VSum.Record(Primitive.arguments(recordId, fi.fieldNames zip fi.writers(a)))

        override def read(av: VSum): Option[A] =
          av.record flatMap { r =>
            if (
              fi.fieldNames.size == r.fields.size ||
              (fi.fieldNames zip r.fields forall { case (fn, rpcvalue.RecordField(rfn, _)) =>
                rfn == "" || rfn == fn
              })
            ) {
              val (oa, s) = fi.reader(r.fields)
              if (s.isEmpty) oa else None
            } else None
          }
      }

    override def emptyRecord[A](recordId: rpcvalue.Identifier, element: () => A): Out[A] =
      new Value.InternalImpl[A] {
        override def write(obj: A): VSum = VSum.Record(Primitive.arguments(recordId, Seq.empty))
        override def read(argumentValue: VSum): Option[A] = Some(element())
      }

    override def field[A](fieldName: String, o: Out[A]) = (fieldName, o)

    override def fields[A](fi: Field[A]) = {
      val (fn, v) = fi
      RecordFields(
        Vector(fn),
        a => Vector(rpcvalue.Value(v.write(a))),
        {
          case rpcvalue.RecordField(_, sv) +: tail =>
            (sv flatMap (Value.decode(_)(v)), tail)
          case all => (None, all)
        },
      )
    }

    override def enumAll[A](
        enumId: rpcvalue.Identifier,
        index: A => Int,
        cases: OneAnd[Vector, (String, A)],
    ): Out[A] =
      new Value.InternalImpl[A] {
        private val readerMap =
          cases.toVector.toMap

        override def read(av: VSum): Option[A] =
          av.enum.flatMap(e => readerMap.get(e.constructor))

        private val writerArray =
          cases.toVector.map(x => VSum.Enum(rpcvalue.Enum(Some(enumId), x._1)))

        override def write(a: A): VSum = writerArray(index(a))
      }

    def variant[A](variantId: rpcvalue.Identifier, cases: VariantCases[A]): Out[A] =
      new Value.InternalImpl[A] {
        override def write(a: A): VSum =
          cases.writers
            .collectFirst(Function unlift (_ traverse (_(a))))
            .cata(
              { case (cName, v) =>
                VSum.Variant(
                  rpcvalue
                    .Variant(
                      variantId = Some(variantId),
                      constructor = cName,
                      Some(rpcvalue.Value(v)),
                    )
                )
              },
              sys.error(s"Missing case for $variantId: ${cases.readers.keySet} is not exhaustive"),
            )

        override def read(av: VSum): Option[A] =
          av.variant flatMap { v =>
            cases.readers get v.constructor flatMap (f => v.value flatMap (dc => f(dc.sum)))
          }
      }

    def variantCase[B, A](caseName: String, o: Out[B])(
        inject: B => A
    )(select: A PartialFunction B): VariantCases[A] =
      VariantCases(
        readers = Map((caseName, (o.read _) andThen (_ map inject))),
        writers = Vector((caseName, select.lift andThen (_ map o.write))),
      )

    object RecordFields extends InvariantApply[RecordFields] {
      override def xmap[A, Z](fa: RecordFields[A], f: A => Z, g: Z => A): RecordFields[Z] =
        fa.copy(writers = g andThen fa.writers, reader = fa.reader andThen (_ leftMap (_ map f)))

      override def xmapN[A, B, Z](fa: RecordFields[A], fb: RecordFields[B])(f: (A, B) => Z)(
          g: Z => (A, B)
      ): RecordFields[Z] =
        RecordFields(
          fa.fieldNames |+| fb.fieldNames,
          writers = { z =>
            val (a, b) = g(z)
            fa.writers(a) |+| fb.writers(b)
          },
          reader = { s0 =>
            val (oa, s1) = fa.reader(s0)
            val (ob, s2) = fb.reader(s1)
            (^(oa, ob)(f), s2)
          },
        )
    }

    object VariantCases extends scalaz.Plus[VariantCases] {
      override def plus[A](a: VariantCases[A], b: => VariantCases[A]): VariantCases[A] =
        VariantCases(readers = a.readers ++ b.readers, writers = a.writers |+| b.writers)
    }

    override val primitive = DamlCodecs
  }
}
