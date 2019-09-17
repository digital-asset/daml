// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.http
package query

import json.JsonProtocol.LfValueCodec.jsValueToApiValue
import com.digitalasset.daml.lf.data.{ImmArray, Ref, SortedLookupList}
import ImmArray.ImmArraySeq
import com.digitalasset.daml.lf.iface
import com.digitalasset.daml.lf.value.{Value => V}
import com.digitalasset.daml.lf.value.TypedValueGenerators.{ValueAddend => VA}

import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}
import spray.json._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class ValuePredicateTest extends WordSpec with Matchers with TableDrivenPropertyChecks {
  type Cid = V.AbsoluteContractId

  private[this] val dummyId = Ref.Identifier(
    Ref.PackageId assertFromString "dummy-package-id",
    Ref.QualifiedName assertFromString "Foo:Bar")
  private[this] val dummyFieldName = Ref.Name assertFromString "foo"
  private[this] val dummyTypeCon = iface.TypeCon(iface.TypeConName(dummyId), ImmArraySeq.empty)
  private[this] def valueAndTypeInObject(
      v: V[Cid],
      ty: iface.Type): (V[Cid], ValuePredicate.TypeLookup) =
    (
      V.ValueRecord(Some(dummyId), ImmArray((Some(dummyFieldName), v))),
      Map(
        dummyId -> iface
          .DefDataType(ImmArraySeq.empty, iface.Record(ImmArraySeq((dummyFieldName, ty))))).lift)

  "fromJsObject" should {
    def c(query: String, ty: VA)(expected: ty.Inj[Cid], shouldMatch: Boolean) =
      (query.parseJson, ty, ty.inj(expected), shouldMatch)

    val literals = Table(
      ("query or json", "type"),
      ("\"foo\"", VA.text),
      ("42", VA.int64),
      ("[1, 2, 3]", VA.list(VA.int64)),
      ("""["1", "2", "3"]""", VA.list(VA.int64)),
    )

    val successes = Table(
      ("query", "type", "expected", "should match?"),
      c("\"foo\"", VA.text)("foo", true),
      c("\"foo\"", VA.text)("bar", false),
      c("42", VA.int64)(42, true),
      c("42", VA.int64)(43, false),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(SortedLookupList(Map("a" -> 1, "b" -> 2)), true),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(SortedLookupList(Map("a" -> 1, "c" -> 2)), false),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(SortedLookupList(Map()), false),
      c("[1, 2, 3]", VA.list(VA.int64))(Vector(1, 2, 3), true),
      c("[1, 2, 3]", VA.list(VA.int64))(Vector(3, 2, 1), false),
      c("""["1", "2", "3"]""", VA.list(VA.int64))(Vector(1, 2, 3), true),
      c("""["1", "2", "3"]""", VA.list(VA.int64))(Vector(3, 2, 1), false),
    )

    "treat literals exactly like ApiCodecCompressed" in forAll(literals) { (queryOrJson, va) =>
      val qj = queryOrJson.parseJson
      val accVal = jsValueToApiValue(qj, va.t, (_: Any) => None)
      val (wrappedExpected, defs) = valueAndTypeInObject(accVal, va.t)
      val vp = ValuePredicate.fromJsObject(Map((dummyFieldName: String) -> qj), dummyTypeCon, defs)
      vp.toFunPredicate(wrappedExpected) shouldBe true
    }

    "examine simple fields literally" in forAll(successes) { (query, va, expected, shouldMatch) =>
      val (wrappedExpected, defs) = valueAndTypeInObject(expected, va.t)
      val vp = ValuePredicate.fromJsObject(
        Map((dummyFieldName: String) -> query),
        dummyTypeCon,
        defs
      )
      vp.toFunPredicate(wrappedExpected) shouldBe shouldMatch
    }
  }
}
