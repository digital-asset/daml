// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.value

import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref, Unnatural}
import com.digitalasset.daml.lf.value.Value._

import org.scalatest.prop.{Checkers, GeneratorDrivenPropertyChecks}
import org.scalatest.{FreeSpec, Matchers}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ValueSpec extends FreeSpec with Matchers with Checkers with GeneratorDrivenPropertyChecks {
  "serialize" - {
    val emptyStroct = ValueStroct(ImmArray.empty)
    val emptyStroctError = "contains stroct ValueStroct(ImmArray())"
    val exceedsNesting = (1 to MAXIMUM_NESTING + 1).foldRight[Value[Nothing]](ValueInt64(42)) {
      case (_, v) => ValueVariant(None, Ref.Name.assertFromString("foo"), v)
    }
    val exceedsNestingError = s"exceeds maximum nesting value of $MAXIMUM_NESTING"
    val matchesNesting = (1 to MAXIMUM_NESTING).foldRight[Value[Nothing]](ValueInt64(42)) {
      case (_, v) => ValueVariant(None, Ref.Name.assertFromString("foo"), v)
    }

    "rejects stroct" in {
      emptyStroct.serializable shouldBe ImmArray(emptyStroctError)
    }

    "rejects nested stroct" in {
      ValueList(FrontStack(emptyStroct)).serializable shouldBe ImmArray(emptyStroctError)
    }

    "rejects excessive nesting" in {
      exceedsNesting.serializable shouldBe ImmArray(exceedsNestingError)
    }

    "accepts just right nesting" in {
      matchesNesting.serializable shouldBe ImmArray.empty
    }

    "outputs both error messages, without duplication" in {
      ValueList(FrontStack(exceedsNesting, ValueStroct(ImmArray.empty), exceedsNesting)).serializable shouldBe
        ImmArray(exceedsNestingError, emptyStroctError)
    }
  }

  "Equal" - {
    import com.digitalasset.daml.lf.value.ValueGenerators._
    import org.scalacheck.Arbitrary
    type T = VersionedValue[Unnatural[ContractId]]
    implicit val arbT: Arbitrary[T] =
      Arbitrary(versionedValueGen map (_ mapContractId (Unnatural(_))))

    scalaz.scalacheck.ScalazProperties.equal.laws[T].properties foreach {
      case (s, p) => s in check(p)
    }

    "results preserve natural == results" in forAll { (a: T, b: T) =>
      scalaz.Equal[T].equal(a, b) shouldBe (a == b)
    }
  }
}
