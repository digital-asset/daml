// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.value

import com.daml.lf.{crypto, data}
import data.{Bytes, ImmArray, Ref}

import Value._
import Ref.{Identifier, Name}
import test.ValueGenerators.{suffixedV1CidGen, coidGen, idGen, nameGen}
import test.TypedValueGenerators.{genAddend, ValueAddend => VA}
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.freespec.AnyFreeSpec
import org.scalatestplus.scalacheck.{Checkers, ScalaCheckPropertyChecks}
import scalaz.{Order, Tag}
import scalaz.std.anyVal._
import scalaz.syntax.functor._
import scalaz.syntax.order._
import scalaz.scalacheck.{ScalazProperties => SzP}
import scalaz.scalacheck.ScalaCheckBinding._
import shapeless.record.{Record => HRecord}
import shapeless.syntax.singleton._

class ValueSpec
    extends AnyFreeSpec
    with Matchers
    with Inside
    with Checkers
    with ScalaCheckPropertyChecks {
  import ValueSpec._

  "ContractID.V1.build" - {

    "rejects too long suffix" in {

      def suffix(size: Int) =
        Bytes.fromByteArray(Array.iterate(0.toByte, size)(b => (b + 1).toByte))

      val hash = crypto.Hash.hashPrivateKey("some hash")
      import ContractId.V1.build
      build(hash, suffix(0)) shouldBe Symbol("right")
      build(hash, suffix(94)) shouldBe Symbol("right")
      build(hash, suffix(95)) shouldBe Symbol("left")
      build(hash, suffix(96)) shouldBe Symbol("left")
      build(hash, suffix(127)) shouldBe Symbol("left")

    }

    "finds cid in contract id value" in {
      val hash = crypto.Hash.hashPrivateKey("some other hash")
      val cid = ContractId.V1(hash)
      val value = Value.ValueContractId(cid)

      value.cids shouldBe Set(cid)
    }

  }

  "ContractId" - {
    type T = ContractId
    implicit val arbT: Arbitrary[T] = Arbitrary(coidGen)
    "Order" - {
      "obeys Order laws" in checkLaws(SzP.order.laws[T])
    }
  }

  // XXX can factor like FlatSpecCheckLaws
  private def checkLaws(props: org.scalacheck.Properties) =
    forEvery(Table(("law", "property"), props.properties.toSeq: _*)) { (_, p) =>
      check(p, minSuccessful(20))
    }

  private def checkOrderPreserved(
      va: VA,
      scope: Value.LookupVariantEnum,
  ) = {
    import va.{injord, injarb, injshrink}
    implicit val targetOrd: Order[Value] = Tag unsubst Value.orderInstance(scope)
    forAll(minSuccessful(20)) { (a: va.Inj, b: va.Inj) =>
      (a ?|? b) should ===(va.inj(a) ?|? va.inj(b))
    }
  }

  "Order" - {

    implicit val cidArb: Arbitrary[Value.ContractId] = Arbitrary(suffixedV1CidGen)

    val FooScope: Value.LookupVariantEnum =
      Map(fooVariantId -> ImmArray("quux", "baz"), fooEnumId -> ImmArray("quux", "baz"))
        .transform((_, ns) => ns map Ref.Name.assertFromString)
        .lift

    "for primitive, matching types" - {
      val EmptyScope: Value.LookupVariantEnum = _ => None
      implicit val ord: Order[Value] = Tag unsubst Value.orderInstance(EmptyScope)

      "obeys order laws" in forAll(genAddend, minSuccessful(100)) { va =>
        implicit val arb: Arbitrary[Value] = va.injarb map (va.inj(_))
        checkLaws(SzP.order.laws[Value])
      }
    }

    "for record and variant types" - {
      implicit val ord: Order[Value] = Tag unsubst Value.orderInstance(FooScope)
      "obeys order laws" in forEvery(Table[VA]("va", fooRecord, fooVariant)) { va =>
        implicit val arb: Arbitrary[Value] = va.injarb map (va.inj(_))
        checkLaws(SzP.order.laws[Value])
      }

      "matches constructor rank" in {
        val fooCp = shapeless.Coproduct[fooVariant.Inj]
        val quux = fooCp(Symbol("quux") ->> 42L)
        val baz = fooCp(Symbol("baz") ->> 42L)
        (fooVariant.inj(quux) ?|? fooVariant.inj(baz)) shouldBe scalaz.Ordering.LT
      }

      "preserves base order" in forEvery(Table[VA]("va", fooRecord, fooVariant)) { va =>
        checkOrderPreserved(va, FooScope)
      }
    }

    "for enum types" - {
      "obeys order laws" in forAll(enumDetailsAndScopeGen, minSuccessful(20)) {
        case (details, scope) =>
          implicit val ord: Order[Value] = Tag unsubst Value.orderInstance(scope)
          forEvery(Table[VA]("va", details.values.toSeq: _*)) { ea =>
            implicit val arb: Arbitrary[Value] = ea.injarb map (ea.inj(_))
            checkLaws(SzP.order.laws[Value])
          }
      }

      "matches constructor rank" in forAll(enumDetailsAndScopeGen, minSuccessful(20)) {
        case (details, scope) =>
          implicit val ord: Order[Value] = Tag unsubst Value.orderInstance(scope)
          forEvery(Table("va", details.values.toSeq: _*)) { ea =>
            implicit val arb: Arbitrary[Value] = ea.injarb map (ea.inj(_))
            forAll(minSuccessful(20)) { (a: Value, b: Value) =>
              inside((a, b)) { case (ValueEnum(_, ac), ValueEnum(_, bc)) =>
                (a ?|? b) should ===((ea.values indexOf ac) ?|? (ea.values indexOf bc))
              }
            }
          }
      }

      "preserves base order" in forAll(enumDetailsAndScopeGen, minSuccessful(20)) {
        case (details, scope) =>
          forEvery(Table[VA]("va", details.values.toSeq: _*)) { ea =>
            checkOrderPreserved(ea, scope)
          }
      }
    }
  }

}

object ValueSpec {
  private val fooSpec = HRecord(quux = VA.int64, baz = VA.int64)
  private val (_, fooRecord) = VA.record(Identifier assertFromString "abc:Foo:FooRec", fooSpec)
  private val fooVariantId = Identifier assertFromString "abc:Foo:FooVar"
  private val (_, fooVariant) = VA.variant(fooVariantId, fooSpec)
  private val fooEnumId = Identifier assertFromString "abc:Foo:FooEnum"

  private[this] val scopeOfEnumsGen: Gen[Map[Identifier, Seq[Name]]] =
    Gen.mapOf(Gen.zip(idGen, Gen.nonEmptyContainerOf[Set, Name](nameGen) map (_.toSeq)))

  private val enumDetailsAndScopeGen
      : Gen[(Map[Identifier, VA.EnumAddend[Seq[Name]]], Value.LookupVariantEnum)] =
    scopeOfEnumsGen flatMap { details =>
      (
        details transform ((name, members) => VA.enumeration(name, members)._2),
        details
          .transform((_, members) => members.to(ImmArray))
          .lift,
      )
    }
  /*
  private val genFoos: Gen[(ImmArray[Name], ValueAddend)] =
    Gen.nonEmptyContainerOf[ImmArray, Name](ValueGenerators.nameGen)
    Gen.oneOf(
    fooRecord,
    fooVariant,
     map ()
  )
   */
}
