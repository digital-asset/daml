// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.Ref
import com.daml.lf.value.Value
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class SValueTest extends AnyWordSpec with Matchers {

  "SValue#toValue" should {

    val Nat = Ref.Identifier.assertFromString("-pkgId:Mod:Nat")
    val Z = Ref.Name.assertFromString("Z")
    val S = Ref.Name.assertFromString("S")

    @tailrec
    def toNat(i: Int, acc: SValue = SValue.SVariant(Nat, Z, 0, SValue.SUnit)): SValue =
      if (i <= 0) acc
      else toNat(i - 1, SValue.SVariant(Nat, S, 1, acc))

    "rejects excessive nesting" in {
      import interpretation.{Error => IError}

      // Because Z has a nesting of 1, toNat(Value.MAXIMUM_NESTING) has a nesting of
      // Value.MAXIMUM_NESTING + 1
      val x = Try(toNat(Value.MAXIMUM_NESTING).toUnnormalizedValue)
      x shouldBe
        Failure(
          SError.SErrorDamlException(IError.Limit(IError.Limit.ValueNesting(Value.MAXIMUM_NESTING)))
        )
    }

    "accepts just right nesting" in {
      // Because Z has a nesting of 1, toNat(Value.MAXIMUM_NESTING - 1) has a nesting of
      // Value.MAXIMUM_NESTING
      Try(toNat(Value.MAXIMUM_NESTING - 1)) shouldBe a[Success[_]]
    }
  }

}
