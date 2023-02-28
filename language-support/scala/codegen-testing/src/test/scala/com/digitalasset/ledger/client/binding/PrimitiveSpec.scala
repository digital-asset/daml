// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.time.{Instant, LocalDate}

import com.daml.ledger.client.binding.{Primitive => P}
import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import shapeless.test.illTyped

import scala.collection.immutable.Map

class PrimitiveSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import PrimitiveSpec._

  "Primitive types" when {
    "defined concretely" should {
      "have nice companion aliases" in {
        P.List: test.CollectionCompat.IterableFactory[P.List]
      }
    }
    "defined abstractly" should {
      "carry their phantoms" in {
        def check[A, B]() = {
          illTyped(
            "implicitly[P.ContractId[A] =:= P.ContractId[B]]",
            "Cannot prove that .*ContractId\\[A\\] =:= .*ContractId\\[B\\].",
          )
          illTyped(
            "implicitly[P.TemplateId[A] =:= P.TemplateId[B]]",
            "Cannot prove that .*TemplateId\\[A\\] =:= .*TemplateId\\[B\\].",
          )
          illTyped(
            "implicitly[P.Update[A] =:= P.Update[B]]",
            "Cannot prove that .*Update\\[A\\] =:= .*Update\\[B\\].",
          )
        }
        check[Unit, Unit]()
      }
    }
  }

  "Date.fromLocalDate" should {
    import ValueGen.dateArb

    "pass through existing dates" in forAll { d: P.Date =>
      P.Date.fromLocalDate(d: LocalDate) shouldBe Some(d)
    }

    "be idempotent" in forAll(anyLocalDateGen) { d =>
      val od2 = P.Date.fromLocalDate(d)
      od2 flatMap (P.Date.fromLocalDate(_: LocalDate)) shouldBe od2
    }

    "prove MIN, MAX are valid" in {
      import P.Date.{MAX, MIN}
      P.Date.fromLocalDate(MIN: LocalDate) shouldBe Some(MIN)
      P.Date.fromLocalDate(MAX: LocalDate) shouldBe Some(MAX)
    }
  }

  "Timestamp.discardNanos" should {
    import ValueGen.timestampArb

    "pass through existing times" in forAll { t: P.Timestamp =>
      P.Timestamp.discardNanos(t: Instant) shouldBe Some(t)
    }

    "be idempotent" in forAll(anyInstantGen) { i =>
      val oi2 = P.Timestamp.discardNanos(i)
      oi2 flatMap (P.Timestamp.discardNanos(_: Instant)) shouldBe oi2
    }

    "prove MIN, MAX are valid" in {
      import P.Timestamp.{MAX, MIN}
      P.Timestamp.discardNanos(MIN: Instant) shouldBe Some(MIN)
      P.Timestamp.discardNanos(MAX: Instant) shouldBe Some(MAX)
    }

    "preapprove values for TimestampConversion.instantToMicros" in forAll(anyInstantGen) { i =>
      P.Timestamp.discardNanos(i) foreach { t =>
        noException should be thrownBy com.daml.api.util.TimestampConversion
          .instantToMicros(t)
      }
    }
  }

  "GenMap" when {
    import Primitive.{GenMap, TextMap}
    val mii: Map[Int, Int] = Map(1 -> 2)
    val tmi: TextMap[Int] = TextMap("one" -> 2)

    "implicitly converting" should {
      "preserve Map identity" in {
        val gm: GenMap[Int, Int] = mii
        gm should be theSameInstanceAs mii
      }

      "preserve TextMap identity" in {
        val gm: GenMap[String, Int] = tmi
        gm should be theSameInstanceAs tmi
      }
    }

    "part of a contract" should {
      "construct with Map" in {
        import Primitive.Int64
        final case class FakeContract(m: GenMap[Int64, Int64])
        val withGm = FakeContract(GenMap(1L -> 2L))
        val withM = FakeContract(Map(1L -> 2L))
        withM should ===(withGm)
      }
    }

    "converting with from" should {
      "preserve identity" in {
        val gm = GenMap from mii
        isExactly(gm, ofType[GenMap[Int, Int]])
        gm should ===(mii)
        (gm eq mii) should ===(Map.from(mii) eq mii)
      }
    }

    ".map" should {
      import org.scalatest.matchers.dsl.ResultOfATypeInvocation

      import scala.collection.immutable.{HashMap, ListMap}
      Seq[(ResultOfATypeInvocation[_ <: Map[_, _]], GenMap[Int, Int])](
        a[HashMap[_, _]] -> HashMap(1 -> 2),
        a[ListMap[_, _]] -> ListMap(1 -> 2),
      ).foreach { case (mapClass, classedMap) =>
        s"preserve class ${mapClass.clazz.getSimpleName}" in {
          val Clazz = mapClass.clazzTag
          (classedMap: Map[Int, Int]).map(identity) match {
            case Clazz(_) => classedMap.map(identity) shouldBe mapClass
            case _ => succeed
          }
        }
      }
    }
  }
}

object PrimitiveSpec {
  private val anyLocalDateGen: Gen[LocalDate] =
    Gen.choose(LocalDate.MIN.toEpochDay, LocalDate.MAX.toEpochDay) map LocalDate.ofEpochDay

  private val anyInstantGen: Gen[Instant] =
    Gen
      .zip(
        Gen.choose(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond),
        Gen.choose(0L, 999999999),
      )
      .map { case (s, n) => Instant.ofEpochSecond(s, n) }

  private final class Proxy[A](val ignore: Unit) extends AnyVal
  // test conformance while disabling implicit conversion
  private def ofType[T]: Proxy[T] = new Proxy(())
  // as a rule, the *singleton* type ac.type will not be ~ Ex; we are interested
  // in what expression `ac` infers to *absent context*.
  private def isExactly[Ac, Ex](ac: Ac, ex: Proxy[Ex])(implicit ev: Ac =:= Ex): Unit = ()
}
