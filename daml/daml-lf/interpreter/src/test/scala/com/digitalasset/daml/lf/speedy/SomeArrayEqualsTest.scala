// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.speedy

import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class SomeArrayEqualsTest extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
  import com.daml.lf.speedy.{SomeArrayEquals => SAE}
  import SomeArrayEqualsTest._

  "equals" should {
    "distinguish same-arity but different classes" in {
      case class A() extends SAE
      case class B() extends SAE
      case class C(i: Int) extends SAE
      case class D(i: Int) extends SAE
      val a = A()
      a shouldBe a
      a shouldBe A()
      a should not be B()
      C(42) shouldBe C(42)
      C(42) should not be D(42)
    }

    "distinguish different arity" in {
      case class A() extends SAE
      case class B(i: Int) extends SAE
      A() should not be B(0)
    }

    "case different element data types properly" in forAll { tb: TestBlob =>
      import java.util.Arrays.copyOf
      tb shouldBe tb
      tb.copy() shouldBe tb
      if (tb.ai ne null)
        tb.copy(ai = copyOf(tb.ai, tb.ai.length)) shouldBe tb
      if (tb.as ne null)
        tb.copy(as = copyOf(tb.as, tb.as.length)) shouldBe tb
      if (tb.s ne null)
        tb.copy(s = new String(tb.s)) shouldBe tb
    }

    "detect varying Ints" in forAll { (tb: TestBlob, i: Int) =>
      whenever(i != tb.i) {
        tb.copy(i = i) should not be tb
      }
    }

    "detect varying Strings" in forAll { (tb: TestBlob, s: Option[String]) =>
      whenever(s != Option(tb.s)) {
        tb.copy(s = s.orNull) should not be tb
      }
    }

    "detect varying Int arrays" in forAll { (tb: TestBlob, oai: Option[Array[Int]]) =>
      whenever(oai.fold(tb.ai ne null)(ai => (tb.ai eq null) || !(ai sameElements tb.ai))) {
        tb.copy(ai = oai.orNull) should not be tb
      }
    }

    "detect varying String arrays" in forAll { (tb: TestBlob, oas: Option[Array[String]]) =>
      whenever(oas.fold(tb.as ne null)(as => (tb.as eq null) || !(as sameElements tb.as))) {
        tb.copy(as = oas.orNull) should not be tb
      }
    }
  }
}

object SomeArrayEqualsTest {
  import org.scalacheck.{Gen, Arbitrary}
  import Arbitrary.{arbitrary => arb}

  final case class TestBlob(i: Int, ai: Array[Int], as: Array[String], s: String)
      extends SomeArrayEquals

  val testBlobGen: Gen[TestBlob] =
    arb[(Int, Option[Array[Int]], Option[Array[String]], Option[String])]
      .map { case (i, ai, as, s) => TestBlob(i, ai.orNull, as.orNull, s.orNull) }

  implicit val testBlobArb: Arbitrary[TestBlob] = Arbitrary(testBlobGen)
}
