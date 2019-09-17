// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http.query

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.ImmArray
import ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.daml.lf.value.TypedValueGenerators.{ValueAddend => VA}

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{WordSpec, Matchers}
import spray.json._

class ValuePredicateTest extends WordSpec with Matchers with TableDrivenPropertyChecks {
  type Cid = V.AbsoluteContractId

  "fromJsObject" should {
    def c(query: String, ty: VA)(expected: ty.Inj[Cid], shouldMatch: Boolean) =
      (query.parseJson, ty, ty.inj(expected), shouldMatch)

    val successes = Table(
      ("query", "type", "expected", "should match?"),
      c("\"foo\"", VA.text)("foo", true),
      c("\"foo\"", VA.text)("bar", false),
      c("42", VA.int64)(42, true),
      c("42", VA.int64)(43, false),
    )

    val dummyId = Ref.Identifier(
      Ref.PackageId assertFromString "dummy-package-id",
      Ref.QualifiedName assertFromString "Foo:Bar")
    val dummyFieldName = Ref.Name assertFromString "foo"

    "examine simple fields literally" in forAll(successes) { (query, va, expected, shouldMatch) =>
      val vp = ValuePredicate.fromJsObject(
        Map((dummyFieldName: String) -> query),
        iface.TypeCon(iface.TypeConName(dummyId), ImmArraySeq.empty),
        Map(
          dummyId -> iface
            .DefDataType(ImmArraySeq.empty, iface.Record(ImmArraySeq((dummyFieldName, va.t))))).lift
      )
      val wrappedExpected = V.ValueRecord(Some(dummyId), ImmArray((Some(dummyFieldName), expected)))
      vp.toFunPredicate(wrappedExpected) shouldBe shouldMatch
    }
  }
}
