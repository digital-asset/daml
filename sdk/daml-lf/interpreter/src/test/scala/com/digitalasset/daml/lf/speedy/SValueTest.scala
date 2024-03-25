// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package speedy

import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.speedy.SValue.{SInt64, SMap, SText}
import com.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.tailrec
import scala.collection.IndexedSeqView
import scala.util.{Failure, Success, Try}

class SValueTest extends AnyWordSpec with Inside with Matchers with TableDrivenPropertyChecks {

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
      inside(x) { case Failure(SError.SErrorDamlException(IError.ValueNesting(limit))) =>
        limit shouldBe Value.MAXIMUM_NESTING
      }
    }

    "accepts just right nesting" in {
      // Because Z has a nesting of 1, toNat(Value.MAXIMUM_NESTING - 1) has a nesting of
      // Value.MAXIMUM_NESTING
      Try(toNat(Value.MAXIMUM_NESTING - 1)) shouldBe a[Success[_]]
    }
  }

  "SMap" should {

    "fail on creation with unordered indexed sequences" in {
      val testCases = Table[Boolean, IndexedSeqView[(SValue, SValue)]](
        ("isTextMap", "entries"),
        (
          true,
          ImmArray(SText("1") -> SValue.SValue.True, SText("0") -> SValue.SValue.False).toSeq.view,
        ),
        (
          false,
          ImmArray(SText("1") -> SValue.SValue.True, SText("0") -> SValue.SValue.False).toSeq.view,
        ),
        (
          true,
          ImmArray(SInt64(42) -> SText("42"), SInt64(1) -> SText("1")).toSeq.view,
        ),
        (
          false,
          ImmArray(SInt64(42) -> SText("42"), SInt64(1) -> SText("1")).toSeq.view,
        ),
      )

      forAll(testCases) { (isTextMap, entries) =>
        val result = Try(SMap.fromOrderedEntries(isTextMap, entries))

        inside(result) { case Failure(error: IllegalArgumentException) =>
          error.getMessage shouldBe "the entries are not ordered"
        }
      }
    }

    "succeed on creation with ordered indexed sequences" in {
      val testCases = Table[Boolean, IndexedSeqView[(SValue, SValue)], SMap](
        ("isTextMap", "entries", "result"),
        (
          true,
          ImmArray(SText("0") -> SValue.SValue.True, SText("1") -> SValue.SValue.False).toSeq.view,
          SMap(true, SText("0") -> SValue.SValue.True, SText("1") -> SValue.SValue.False),
        ),
        (
          false,
          ImmArray(SText("0") -> SValue.SValue.True, SText("1") -> SValue.SValue.False).toSeq.view,
          SMap(false, SText("0") -> SValue.SValue.True, SText("1") -> SValue.SValue.False),
        ),
        (
          true,
          ImmArray(SInt64(1) -> SText("1"), SInt64(42) -> SText("42")).toSeq.view,
          SMap(true, SInt64(1) -> SText("1"), SInt64(42) -> SText("42")),
        ),
        (
          false,
          ImmArray(SInt64(1) -> SText("1"), SInt64(42) -> SText("42")).toSeq.view,
          SMap(false, SInt64(1) -> SText("1"), SInt64(42) -> SText("42")),
        ),
      )

      forAll(testCases) { (isTextMap, entries, result) =>
        SMap.fromOrderedEntries(isTextMap, entries) shouldBe result
      }
    }
  }
}
