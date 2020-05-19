// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package query

import json.JsonProtocol.LfValueCodec.{apiValueToJsValue, jsValueToApiValue}
import com.daml.lf.data.{Decimal, ImmArray, Numeric, Ref, SortedLookupList, Time}
import ImmArray.ImmArraySeq
import com.daml.lf.iface
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.TypedValueGenerators.{genAddendNoListMap, ValueAddend => VA}

import org.scalacheck.{Arbitrary, Gen}
import org.scalactic.source
import org.scalatest.prop.{GeneratorDrivenPropertyChecks, TableDrivenPropertyChecks}
import org.scalatest.{Inside, Matchers, WordSpec}
import scalaz.Order
import spray.json._

@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.NonUnitStatements"))
class ValuePredicateTest
    extends WordSpec
    with Matchers
    with Inside
    with GeneratorDrivenPropertyChecks
    with TableDrivenPropertyChecks {
  import ValuePredicateTest._
  type Cid = V.AbsoluteContractId
  private[this] implicit val arbCid: Arbitrary[Cid] = Arbitrary(
    Gen.alphaStr map (t => V.AbsoluteContractId.V0 assertFromString ('#' +: t)))
  // only V0 supported in this test atm
  private[this] implicit val ordCid: Order[Cid] = Order[V.AbsoluteContractId.V0] contramap (inside(
    _) {
    case a0 @ V.AbsoluteContractId.V0(_) => a0
  })

  private[this] val dummyId = Ref.Identifier(
    Ref.PackageId assertFromString "dummy-package-id",
    Ref.QualifiedName assertFromString "Foo:Bar")
  private[this] val dummyFieldName = Ref.Name assertFromString "foo"
  private[this] val dummyTypeCon = iface.TypeCon(iface.TypeConName(dummyId), ImmArraySeq.empty)
  private[this] def valueAndTypeInObject(
      v: V[Cid],
      ty: iface.Type): (V[Cid], ValuePredicate.TypeLookup) =
    (V.ValueRecord(Some(dummyId), ImmArray((Some(dummyFieldName), v))), typeInObject(ty))
  private[this] def typeInObject(ty: iface.Type): ValuePredicate.TypeLookup =
    Map(
      dummyId -> iface
        .DefDataType(ImmArraySeq.empty, iface.Record(ImmArraySeq((dummyFieldName, ty))))).lift

  "fromJsObject" should {
    def c[M](query: String, ty: VA)(expected: ty.Inj[Cid], shouldMatch: M)(
        implicit pos: source.Position) =
      (pos.lineNumber, query.parseJson, ty, ty.inj(expected), shouldMatch)

    object VAs {
      val oi = VA.optional(VA.int64)
      val ooi = VA.optional(oi)
      val oooi = VA.optional(ooi)
    }

    val literals = Table(
      ("query or json", "type"),
      ("\"foo\"", VA.text),
      ("42", VA.int64),
      ("111.11", VA.numeric(Numeric.Scale assertFromInt 10)),
      ("null", VAs.oi),
      ("42", VAs.oi),
      ("null", VAs.ooi),
      ("[]", VAs.ooi),
      ("[42]", VAs.ooi),
      ("null", VAs.oooi),
      ("[]", VAs.oooi),
      ("[[]]", VAs.oooi),
      ("[[42]]", VAs.oooi),
    )

    val failLiterals = Table(
      ("query or json", "type", "message"),
      ("[1, 2, 3]", VA.list(VA.int64), "PrimTypeList not supported"),
    )

    val excl4143 = """{"%gt": 41, "%lt": 43}"""
    val incl42 = """{"%gte": 42, "%lte": 42}"""
    val VAtestNumeric = VA.numeric(Decimal.scale)

    val successes = Table(
      ("line#", "query", "type", "expected", "should match?"),
      c("\"foo\"", VA.text)("foo", true),
      c("\"foo\"", VA.text)("bar", false),
      c("42", VA.int64)(42, true),
      c("42", VA.int64)(43, false),
      c(excl4143, VA.int64)(42, true),
      c(excl4143, VA.int64)(41, false),
      c(excl4143, VA.int64)(43, false),
      c(incl42, VA.int64)(42, true),
      c(incl42, VA.int64)(41, false),
      c(incl42, VA.int64)(43, false),
      c(excl4143, VAtestNumeric)(Numeric assertFromString "42.0000000000", true),
      c(excl4143, VAtestNumeric)(Numeric assertFromString "41.0000000000", false),
      c(excl4143, VAtestNumeric)(Numeric assertFromString "43.0000000000", false),
      c(incl42, VAtestNumeric)(Numeric assertFromString "42.0000000000", true),
      c(incl42, VAtestNumeric)(Numeric assertFromString "41.0000000000", false),
      c(incl42, VAtestNumeric)(Numeric assertFromString "43.0000000000", false),
      c("""{"%lte": 42}""", VA.int64)(42, true),
      c("""{"%lte": 42}""", VA.int64)(43, false),
      c("""{"%gte": 42}""", VA.int64)(42, true),
      c("""{"%gte": 42}""", VA.int64)(41, false),
      c("""{"%lt": 42}""", VA.int64)(41, true),
      c("""{"%lt": 42}""", VA.int64)(42, false),
      c("""{"%gt": 42}""", VA.int64)(43, true),
      c("""{"%gt": 42}""", VA.int64)(42, false),
      c("""{"%gt": "bar", "%lt": "foo"}""", VA.text)("baz", true),
      c("""{"%gte": "1980-01-01", "%lt": "2000-01-01"}""", VA.date)(
        Time.Date assertFromString "1986-06-21",
        true),
      c("""{"%gte": "1980-01-01T00:00:00Z", "%lt": "2000-01-01T00:00:00Z"}""", VA.timestamp)(
        Time.Timestamp assertFromString "1986-06-21T00:00:00Z",
        true),
    )

    val failures = Table(
      ("line#", "query", "type", "expected", "error message"),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported"),
      c("""{"b": 2, "a": 1}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported"),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "c" -> 2)),
        "PrimTypeTextMap not supported"),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(
        SortedLookupList(Map()),
        "PrimTypeTextMap not supported"),
      c("""{"a": 1}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported"),
      c("""{}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported"),
      c("""{}""", VA.genMap(VA.int64, VA.int64))(Map(), "PrimTypeGenMap not supported"),
      c("[1, 2, 3]", VA.list(VA.int64))(Vector(1, 2, 3), "PrimTypeList not supported"),
      c("[1, 2, 3]", VA.list(VA.int64))(Vector(3, 2, 1), "PrimTypeList not supported"),
    )

    "treat literals exactly like ApiCodecCompressed" in forAll(literals) { (queryOrJson, va) =>
      val qj = queryOrJson.parseJson
      val accVal = jsValueToApiValue(qj, va.t, (_: Any) => None)
      val (wrappedExpected, defs) = valueAndTypeInObject(accVal, va.t)
      val vp = ValuePredicate.fromJsObject(Map((dummyFieldName: String) -> qj), dummyTypeCon, defs)
      vp.toFunPredicate(wrappedExpected) shouldBe true
    }

    "reject literals with disallowed query types" in forAll(failLiterals) {
      (queryOrJson, va, failMessage) =>
        val qj = queryOrJson.parseJson
        val accVal = jsValueToApiValue(qj, va.t, (_: Any) => None)
        val (_, defs) = valueAndTypeInObject(accVal, va.t)
        val ex = the[Exception] thrownBy {
          ValuePredicate.fromJsObject(Map((dummyFieldName: String) -> qj), dummyTypeCon, defs)
        }
        ex.getMessage should include(failMessage)
    }

    "examine simple fields literally" in forAll(successes) {
      (_, query, va, expected, shouldMatch) =>
        val (wrappedExpected, defs) = valueAndTypeInObject(expected, va.t)
        val vp = ValuePredicate.fromJsObject(
          Map((dummyFieldName: String) -> query),
          dummyTypeCon,
          defs
        )
        vp.toFunPredicate(wrappedExpected) shouldBe shouldMatch
    }

    "reject queries of disallowed type" in forAll(failures) {
      (_, query, va, expected, shouldMatch) =>
        val (_, defs) = valueAndTypeInObject(expected, va.t)
        val ex = the[Exception] thrownBy {
          ValuePredicate.fromJsObject(
            Map((dummyFieldName: String) -> query),
            dummyTypeCon,
            defs
          )
        }
        ex.getMessage should include(shouldMatch)
    }

    "examine all sorts of primitives literally, except lists and maps" in forAll(
      genAddendNoListMap,
      minSuccessful(100)) { va =>
      import va.injshrink
      implicit val arbInj: Arbitrary[va.Inj[Cid]] = va.injarb
      forAll(minSuccessful(20)) { v: va.Inj[Cid] =>
        val expected = va.inj(v)
        val (wrappedExpected, defs) = valueAndTypeInObject(expected, va.t)
        val query = apiValueToJsValue(expected)
        val vp =
          ValuePredicate.fromJsObject(Map((dummyFieldName: String) -> query), dummyTypeCon, defs)
        vp.toFunPredicate(wrappedExpected) shouldBe true
      }
    }

    val sqlWheres = {
      import doobie.implicits._, dbbackend.Queries.Implicits._
      Table(
        ("query", "type", "sql"),
        ("42", VA.int64, sql"payload = ${s"""{"$dummyFieldName":42}""".parseJson}::jsonb"),
        (
          """{"%lte": 42}""",
          VA.int64,
          sql"payload->${"foo": String} <= ${JsNumber(42): JsValue}::jsonb AND payload @> ${JsObject(): JsValue}::jsonb"),
      )
    }

    "compile to SQL" in forEvery(sqlWheres) { (query, va, sql: doobie.Fragment) =>
      val defs = typeInObject(va.t)
      val vp = ValuePredicate.fromJsObject(
        Map((dummyFieldName: String) -> query.parseJson),
        dummyTypeCon,
        defs)
      val frag = vp.toSqlWhereClause
      frag.toString should ===(sql.toString)
      import language.reflectiveCalls
      flattenFragmentExistential(frag.asInstanceOf[{ def a: AnyRef }].a) should ===(
        flattenFragmentExistential(sql.asInstanceOf[{ def a: AnyRef }].a))
    }
  }
}

object ValuePredicateTest {
  import shapeless.{::, HNil}

  /** Flatten tuples and hlists in Fragment.a. */
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def flattenFragmentExistential(v: Any): Seq[Any] = v match {
    case (l, r) => flattenFragmentExistential(l) ++ flattenFragmentExistential(r)
    case hd :: tl => hd +: flattenFragmentExistential(tl)
    case HNil => Seq.empty
    case x => Seq(x)
  }
}
