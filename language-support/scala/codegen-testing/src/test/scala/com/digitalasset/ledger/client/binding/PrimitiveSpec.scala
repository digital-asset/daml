// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client.binding

import java.time.{Instant, LocalDate}

import org.scalacheck.Gen
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import shapeless.test.illTyped

import com.daml.ledger.client.binding.{Primitive => P}

class PrimitiveSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import PrimitiveSpec._

  "Primitive types" when {
    "defined concretely" should {
      "have nice companion aliases" in {
        P.List: collection.generic.TraversableFactory[P.List]
      }
    }
    "defined abstractly" should {
      "carry their phantoms" in {
        def check[A, B]() = {
          illTyped(
            "implicitly[P.ContractId[A] =:= P.ContractId[B]]",
            "Cannot prove that .*ContractId\\[A\\] =:= .*ContractId\\[B\\].")
          illTyped(
            "implicitly[P.TemplateId[A] =:= P.TemplateId[B]]",
            "Cannot prove that .*TemplateId\\[A\\] =:= .*TemplateId\\[B\\].")
          illTyped(
            "implicitly[P.Update[A] =:= P.Update[B]]",
            "Cannot prove that .*Update\\[A\\] =:= .*Update\\[B\\].")
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
      import P.Date.{MIN, MAX}
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
      import P.Timestamp.{MIN, MAX}
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
}

object PrimitiveSpec {
  private val anyLocalDateGen: Gen[LocalDate] =
    Gen.choose(LocalDate.MIN.toEpochDay, LocalDate.MAX.toEpochDay) map LocalDate.ofEpochDay

  private val anyInstantGen: Gen[Instant] =
    Gen
      .zip(
        Gen.choose(Instant.MIN.getEpochSecond, Instant.MAX.getEpochSecond),
        Gen.choose(0L, 999999999))
      .map { case (s, n) => Instant.ofEpochSecond(s, n) }
}
