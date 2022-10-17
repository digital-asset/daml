// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http
package query

import json.JsonProtocol.LfValueCodec.{apiValueToJsValue, jsValueToApiValue}
import com.daml.lf.data.{Decimal, ImmArray, Numeric, Ref, SortedLookupList, Time}
import ImmArray.ImmArraySeq
import com.daml.http.dbbackend.SurrogateTemplateIdCache
import com.daml.lf.typesig
import com.daml.lf.value.{Value => V}
import com.daml.lf.value.test.TypedValueGenerators.{genAddendNoListMap, ValueAddend => VA}
import com.daml.lf.value.test.ValueGenerators.coidGen
import com.daml.metrics.Metrics
import org.scalacheck.Arbitrary
import org.scalactic.source
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.\/
import spray.json._

@SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
class ValuePredicateTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with ScalaCheckDrivenPropertyChecks
    with TableDrivenPropertyChecks {
  import ValuePredicateTest._
  type Cid = V.ContractId
  private[this] implicit val arbCid: Arbitrary[Cid] = Arbitrary(coidGen)

  import Ref.QualifiedName.{assertFromString => qn}

  private[this] val dummyPackageId = Ref.PackageId assertFromString "dummy-package-id"

  private[this] val (tuple3Id, (tuple3DDT, tuple3VA)) = {
    import shapeless.record.{Record => HRecord}
    val sig = HRecord(_1 = VA.int64, _2 = VA.text, _3 = VA.bool)
    val id = Ref.Identifier(dummyPackageId, qn("Foo:Tuple3"))
    (id, VA.record(id, sig))
  }

  private[this] def eitherT = {
    import shapeless.record.{Record => HRecord}
    val sig = HRecord(Left = VA.int64, Right = VA.text)
    val id = Ref.Identifier(dummyPackageId, qn("Foo:Either"))
    (id, VA.variant(id, sig))
  }
  private[this] val dummyId = Ref.Identifier(
    dummyPackageId,
    qn("Foo:Bar"),
  )
  private[this] val dummyFieldName = Ref.Name assertFromString "foo"
  private[this] val dummyTypeCon = typesig.TypeCon(typesig.TypeConName(dummyId), ImmArraySeq.empty)
  private[this] val (eitherId, (eitherDDT, eitherVA)) = eitherT
  private[this] def valueAndTypeInObject(
      v: V,
      ty: typesig.Type,
  ): (V, ValuePredicate.TypeLookup) =
    (V.ValueRecord(Some(dummyId), ImmArray((Some(dummyFieldName), v))), typeInObject(ty))
  private[this] def typeInObject(ty: typesig.Type): ValuePredicate.TypeLookup =
    Map(
      dummyId -> typesig
        .DefDataType(ImmArraySeq.empty, typesig.Record(ImmArraySeq((dummyFieldName, ty)))),
      eitherId -> eitherDDT,
      tuple3Id -> tuple3DDT,
    ).lift

  "fromJsObject" should {
    def c[M](query: String, ty: VA)(expected: ty.Inj, shouldMatch: M)(implicit
        pos: source.Position
    ) =
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
        true,
      ),
      c("""{"%gte": "1980-01-01T00:00:00Z", "%lt": "2000-01-01T00:00:00Z"}""", VA.timestamp)(
        Time.Timestamp assertFromString "1986-06-21T00:00:00Z",
        true,
      ),
    )

    val failures = Table(
      ("line#", "query", "type", "expected", "error message"),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported",
      ),
      c("""{"b": 2, "a": 1}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported",
      ),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "c" -> 2)),
        "PrimTypeTextMap not supported",
      ),
      c("""{"a": 1, "b": 2}""", VA.map(VA.int64))(
        SortedLookupList(Map()),
        "PrimTypeTextMap not supported",
      ),
      c("""{"a": 1}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported",
      ),
      c("""{}""", VA.map(VA.int64))(
        SortedLookupList(Map("a" -> 1, "b" -> 2)),
        "PrimTypeTextMap not supported",
      ),
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
          defs,
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
            defs,
          )
        }
        ex.getMessage should include(shouldMatch)
    }

    "examine all sorts of primitives literally, except lists and maps" in forAll(
      genAddendNoListMap,
      minSuccessful(100),
    ) { va =>
      import va.injshrink
      implicit val arbInj: Arbitrary[va.Inj] = va.injarb
      forAll(minSuccessful(20)) { v: va.Inj =>
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
        ("query", "type", "postgresql", "oraclesql"),
        (
          "42",
          VA.int64,
          sql"payload = ${s"""{"$dummyFieldName":42}""".parseJson}::jsonb",
          sql"JSON_EQUAL(JSON_QUERY(payload, '$$' RETURNING CLOB), ${s"""{"$dummyFieldName":42}""".parseJson})",
        ),
        (
          """{"_2": "hi there", "_3": false}""",
          tuple3VA,
          sql"payload @> ${"""{"foo":{"_2":"hi there","_3":false}}""".parseJson}::jsonb",
          sql"""JSON_EXISTS(payload, '$$."foo"."_2"?(@ == $$X)' PASSING ${"hi there"} AS X)"""
            ++ sql""" AND JSON_EXISTS(payload, '$$."foo"."_3"?(@ == false)')""",
        ),
        (
          "{}",
          tuple3VA,
          sql"payload @> ${"""{"foo":{}}""".parseJson}::jsonb",
          sql"""JSON_EXISTS(payload, '$$."foo"?(!(@ == null))')""",
        ),
        (
          """{"%lte": 42}""",
          VA.int64,
          sql"payload->${"foo": String} <= ${JsNumber(42): JsValue}::jsonb AND payload @> ${JsObject(): JsValue}::jsonb",
          sql"""JSON_EXISTS(payload, '$$."foo"?(@ <= $$X)' PASSING ${42L} AS X) AND 1 = 1""",
        ),
        (
          """{"tag": "Left", "value": "42"}""",
          eitherVA,
          sql"payload = ${"""{"foo": {"tag": "Left", "value": 42}}""".parseJson}::jsonb",
          sql"JSON_EQUAL(JSON_QUERY(payload, '$$' RETURNING CLOB), ${s"""{"foo": {"tag": "Left", "value": 42}}""".parseJson})",
        ),
        (
          """{"tag": "Left", "value": {"%lte": 42}}""",
          eitherVA,
          sql"payload->${"foo"}->${"value"} <= ${"42".parseJson}::jsonb AND payload @> ${"""{"foo": {"tag": "Left"}}""".parseJson}::jsonb",
          sql"""JSON_EXISTS(payload, '$$."foo"."value"?(@ <= $$X)' PASSING ${42L} AS X)"""
            ++ sql""" AND JSON_EXISTS(payload, '$$."foo"."tag"?(@ == $$X)' PASSING ${"Left"} AS X)""",
        ),
      )
    }

    "compile to SQL" in forEvery(sqlWheres) { (query, va, postgreSql, oracleSql) =>
      val defs = typeInObject(va.t)
      val vp = ValuePredicate.fromJsObject(
        Map((dummyFieldName: String) -> query.parseJson),
        dummyTypeCon,
        defs,
      )
      forEvery(
        Table(
          "backend" -> "sql",
          dbbackend.SupportedJdbcDriver.Postgres -> postgreSql,
          dbbackend.SupportedJdbcDriver.Oracle -> oracleSql,
        )
      ) { (backend, sql: doobie.Fragment) =>
        // we aren't running the SQL, just looking at it
        import org.scalatest.EitherValues._
        implicit val metrics: Metrics = Metrics.ForTesting
        implicit val sjd: dbbackend.SupportedJdbcDriver.TC =
          backend.configure("", Map.empty, SurrogateTemplateIdCache.MaxEntries).value
        val frag = vp.toSqlWhereClause
        frag.toString should ===(sql.toString)
        fragmentElems(frag) should ===(fragmentElems(sql))
      }
    }
  }
}

object ValuePredicateTest {
  import cats.data.Chain, doobie.util.fragment.{Elem, Fragment}

  private def fragmentElems(frag: Fragment): Chain[Any \/ Option[Any]] = {
    import language.reflectiveCalls, Elem.{Arg, Opt}
    frag.asInstanceOf[{ def elems: Chain[Elem] }].elems.map {
      case Arg(a, _) => \/.left(a)
      case Opt(o, _) => \/.right(o)
    }
  }
}
