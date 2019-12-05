// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.ledger.client.binding

import encoding.{ValuePrimitiveEncoding, GenEncoding}
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.language.higherKinds

class ValueSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {
  import ValueSpec._

  "Value" should {
    "read(write(x)) yields x" in forAll { wv: Exists[WithValue] =>
      val (vc, t) = wv.run
      import vc._
      (vc.tName, Value.Decoder[wv.T].read(Value.Encoder[wv.T].write(t))) shouldBe (
        (
          vc.tName,
          Some(t)))
    }

    "be found for lists" in {
      import com.digitalasset.ledger.client.binding.{Primitive => P}
      Value[P.List[P.Int64]]
      Value.Decoder[P.List[P.Int64]]
      Value.Encoder[P.List[P.Int64]]
    }
  }
}

object ValueSpec {

  private sealed abstract class Exists[F[_]] {
    type T
    val run: F[T]
  }

  private def Exists[T0, F[_]](_run: F[T0]): Exists[F] = {
    final case class ExistsImpl(run: F[T0]) extends Exists[F] {
      type T = T0
      override def productPrefix = "Exists"
    }
    ExistsImpl(_run)
  }

  private final case class ValueCheck[T](tName: String)(
      implicit val TA: Arbitrary[T],
      val TS: Shrink[T],
      val TV: Value[T])

  import com.digitalasset.ledger.client.binding.{Primitive => P}

  private[binding] implicit val dateArb: Arbitrary[P.Date] =
    Arbitrary(GenEncoding.primitive.valueDate)

  private[binding] implicit val timestampArb: Arbitrary[P.Timestamp] =
    Arbitrary(GenEncoding.primitive.valueTimestamp)

  private[this] object TautologicalValueChecks extends ValuePrimitiveEncoding[ValueCheck] {
    override val valueInt64 = ValueCheck[P.Int64]("Int64")
    override val valueNumeric = ValueCheck[P.Numeric]("Numeric")
    override val valueParty = {
      implicit val PA: Arbitrary[P.Party] = Arbitrary(GenEncoding.primitive.valueParty)
      ValueCheck[P.Party]("Party")
    }
    override val valueText = ValueCheck[P.Text]("Text")
    override val valueDate = ValueCheck[P.Date]("Date")
    override val valueTimestamp = ValueCheck[P.Timestamp]("Timestamp")
    override val valueUnit = ValueCheck[P.Unit]("Unit")
    override val valueBool = ValueCheck[P.Bool]("Bool")
    override def valueContractId[A] = {
      implicit val CA: Arbitrary[P.ContractId[A]] =
        Arbitrary(GenEncoding.primitive.valueContractId)
      ValueCheck[P.ContractId[A]]("ContractId")
    }

    override def valueList[A](implicit vc: ValueCheck[A]) = {
      import vc._
      ValueCheck[P.List[A]](s"List[$tName]")
    }

    override def valueOptional[A](implicit vc: ValueCheck[A]) = {
      import vc._
      ValueCheck[P.Optional[A]](s"Option[$tName]")
    }

    override def valueTextMap[A](implicit vc: ValueCheck[A]) = {
      import vc._
      implicit val arbTM: Arbitrary[P.TextMap[A]] = Arbitrary(GenEncoding.primitive.valueTextMap(TA.arbitrary))
      ValueCheck[P.TextMap[A]](s"Map[$tName]")
    }

    override def valueGenMap[K, V](implicit vcK: ValueCheck[K], vcV: ValueCheck[V]) = {
      import vcK.{TA => KA, TS => KS, TV => KV}
      import vcV.{TA => VA, TS => VS, TV => VV}

      ValueCheck[P.GenMap[K, V]](s"GenMap[${vcK.tName}, ${vcV.tName}]")
    }
  }

  private val tautologicalValueChecks: Seq[Exists[ValueCheck]] =
    ValuePrimitiveEncoding.roots(TautologicalValueChecks).map(Exists(_))

  private val valueChecks: Gen[Exists[ValueCheck]] =
    Gen.frequency(
      (tautologicalValueChecks.size, Gen.oneOf(tautologicalValueChecks)),
      (1, Gen.lzy {
        valueChecks.map { vc =>
          Exists(TautologicalValueChecks.valueList(vc.run))
        }
      }),
      (1, Gen.lzy {
        valueChecks.map { vc =>
          Exists(TautologicalValueChecks.valueOptional(vc.run))
        }
      }),
      (1, Gen.lzy {
        valueChecks.map { vc =>
          Exists(TautologicalValueChecks.valueTextMap(vc.run))
        }
      }),
      (1, Gen.lzy {
        valueChecks.map { vc =>
          Exists(TautologicalValueChecks.valueGenMap(vc.run, vc.run))
        }
      })
    )

  private type WithValue[T] = (ValueCheck[T], T)

  private implicit val withValues: Arbitrary[Exists[WithValue]] =
    Arbitrary(valueChecks.flatMap { evc =>
      val vc = evc.run
      vc.TA.arbitrary map (t => Exists[evc.T, WithValue]((vc, t)))
    })

  private implicit val withValueShrink: Shrink[Exists[WithValue]] =
    Shrink { ewv =>
      val (vc, t) = ewv.run
      vc.TS shrink t map (t2 => Exists[ewv.T, WithValue]((vc, t2)))
    }
}
