// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.speedy.SValue.{SInt64, SMap, SPAP, SText}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.tailrec
import scala.collection.IndexedSeqView
import scala.util.{Try, Success, Failure}

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

    val nonComparableFailure =
      Failure(SError.SErrorDamlException(interpretation.Error.NonComparableValues))

    "fail on creation with non-comparable values" in {
      val tyConEither = stablepackages.StablePackagesV2.Either
      val leftName = Ref.Name.assertFromString("Left")
      val rightName = Ref.Name.assertFromString("Right")

      def left(v: SValue) = SValue.SVariant(tyConEither, leftName, 0, v)

      def right(v: SValue) = SValue.SVariant(tyConEither, rightName, 1, v)

      val builtin = SBuiltinFun.SBToText
      // function are incomparable
      val fun = SPAP(SValue.PBuiltin(builtin), ArrayList.empty, builtin.arity)
      val testCases = Table[Seq[(SValue, SValue)]](
        "entries",
        ImmArray(left(fun) -> SValue.SValue.True, right(SText("0")) -> SValue.SValue.False).toSeq,
        ImmArray(left(SText("1")) -> SValue.SValue.True, right(fun) -> SValue.SValue.False).toSeq,
        ImmArray(left(SInt64(42)) -> SText("42"), right(fun) -> SText("1")).toSeq,
        ImmArray(fun -> SText("42")).toSeq,
      )

      forEvery(testCases) { entries =>
        Try(SMap.fromOrderedEntries(true, entries)) shouldBe nonComparableFailure
        Try(SMap(false, entries)) shouldBe nonComparableFailure
        Try(SMap(true, entries: _*)) shouldBe nonComparableFailure
      }
    }

    "fail on inserting/lookup/deleting a non-comparable value" in {
      val tyConEither = stablepackages.StablePackagesV2.Either
      val leftName = Ref.Name.assertFromString("Left")
      val rightName = Ref.Name.assertFromString("Right")
      def left(v: SValue) = SValue.SVariant(tyConEither, leftName, 0, v)
      def right(v: SValue) = SValue.SVariant(tyConEither, rightName, 1, v)
      val builtin = SBuiltinFun.SBToText
      // functions are incomparable
      val fun = SPAP(SValue.PBuiltin(builtin), ArrayList.empty, builtin.arity)
      val testCases = Table[SMap](
        "entries",
        SMap(true, left(SText("1")) -> SValue.SValue.True, left(SText("2")) -> SValue.SValue.False),
        SMap(true, left(SText("0")) -> SValue.SValue.True, left(SText("1")) -> SValue.SValue.False),
        SMap(true, left(SInt64(42)) -> SValue.SValue.True),
        SMap(true),
      )

      forEvery(testCases) { smap =>
        Try(smap.insert(right(fun), SValue.SValue.False)) shouldBe nonComparableFailure
        Try(smap.delete(right(fun))) shouldBe nonComparableFailure
        Try(smap.get(right(fun))) shouldBe nonComparableFailure
      }
    }

  }
}
