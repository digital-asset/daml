// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import doobie.implicits._
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Integrity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks => STSC}
import STSC.{forAll => scForAll}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.immutable.Seq
import scalaz.\/

class QueriesSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {
  import QueriesSpec._

  "projectedIndex" should {
    import Queries.{projectedIndex, SurrogateTpId}

    val cases = Table(
      ("map", "projection"),
      (
        Seq((SurrogateTpId(55L), fr"foo") -> 0),
        sql"CASE WHEN (tpid = ${55L}) THEN ${0}||''  ELSE NULL END ",
      ),
      (
        Seq((SurrogateTpId(55L), fr"foo") -> 0, (SurrogateTpId(66L), fr"foo") -> 1),
        sql"CASE WHEN (tpid = ${55L}) THEN ${0}||''  WHEN (tpid = ${66L}) THEN ${1}||''  ELSE NULL END ",
      ),
      (
        Seq(
          (SurrogateTpId(55L), fr"foo") -> 0,
          (SurrogateTpId(55L), fr"bar") -> 1,
          (SurrogateTpId(66L), fr"baz") -> 2,
        ),
        sql"CASE WHEN (tpid = ${55L}) THEN " ++
          sql"(CASE WHEN (foo ) THEN ${0}||',' ELSE '' END) || (CASE WHEN (bar ) THEN ${1}||',' ELSE '' END) " ++
          sql" WHEN (tpid = ${66L}) THEN ${2}||''  ELSE NULL END ",
      ),
    )

    "yield expected expressions for sample inputs" in forEvery(cases) { (map, projection) =>
      val frag = projectedIndex(map, sql"tpid")
      frag.toString should ===(projection.toString)
      fragmentElems(frag) should ===(fragmentElems(projection))
    }
  }

  "groupUnsyncedOffsets" should {
    import Queries.groupUnsyncedOffsets
    "not drop duplicate template IDs" in {
      groupUnsyncedOffsets(Set.empty, Vector((0, (1, 2)), (0, (3, 4)))) should ===(
        Map(0 -> Map(1 -> 2, 3 -> 4))
      )
    }

    "add empty maps for template IDs missing from the input" in {
      groupUnsyncedOffsets(Set(0, 1, 2), Vector((0, (1, 2)), (0, (3, 4)))) should ===(
        Map(0 -> Map(1 -> 2, 3 -> 4), 1 -> Map.empty, 2 -> Map.empty)
      )
    }
  }

  { // quotedJsonSearchToken tests
    import Queries.quotedJsonSearchToken
    import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits._
    import doobie.Fragment.const0

    val sqlInjectionSecurity: SecurityTest =
      SecurityTest(property = Integrity, asset = "HTTP JSON API Service").setAttack(
        Attack(
          actor = "JSON API client",
          threat = "request a /query with a JSON body containing an SQL injection",
          mitigation = "escape string literals, for situations where query params are not possible",
        )
      )

    "quotedJsonSearchToken should escape known cases correctly" taggedAs sqlInjectionSecurity in forEvery(
      Table(
      // format: off
      ("literal",                           "quoted"),
      "ohai"                             -> sql"'ohai'",                                // Simple string surrounded by single quotes
      "'ohai'"                           -> sql"'''ohai'''",                            // Quoted string gets both quotes escaped
      "'"                                -> sql"''''",                                  // Single quote gets escaped and re-quoted
      "''"                               -> sql"''''''",                                // Same for two single quotes
      "'''"                              -> sql"''''''''",                              // Same for three single quotes
      "Robert'); DROP TABLE Students;--" -> sql"'Robert''); DROP TABLE Students;--'",   // A quote in the middle gets escaped
      raw"O\'Reilly"                     -> const0(raw"'O\''Reilly'"),                  // A \' in the middle is converted to \''
      "O\u02bcReilly"                    -> const0("'O\u02bcReilly'"),                  // A fancy quote-like char is untouched
      """"hello""""                      -> sql"""'"hello"'""",                         // Double-quotes are not treated specially
      // format: on
      )
    ) { (input, expected) =>
      val actual = quotedJsonSearchToken(input)
      fragmentSql(actual) shouldBe fragmentSql(expected)
    }

    "quotedJsonSearchToken should produce well-formed sql string expressions" taggedAs sqlInjectionSecurity in {
      import org.scalacheck.{Arbitrary, Gen}

      // Healthy balance of fully arbitrary chars, ascii chars, and plenty of quotes.
      val literals = Gen.frequency(
        1 -> Arbitrary.arbitrary[String],
        1 -> Gen.asciiStr,
        1 -> Gen.oneOf("'", "\"", "\\"),
      )

      implicit val generatorDrivenConfig: STSC.PropertyCheckConfiguration =
        STSC.generatorDrivenConfig.copy(minSuccessful = 10000)

      scForAll(literals) { (literal: String) =>
        val sql = fragmentSql(quotedJsonSearchToken(literal))
        sql.count(_ == '\'') % 2 shouldBe 0 // Contains an even number of '
        sql.take(1) shouldBe "\'" // Starts with '
        sql.takeRight(1) shouldBe "'" // Ends with '
        val quotedVal = sql.drop(1).dropRight(1)
        quotedVal should not include regex(
          "(?<!')'(?!')"
        ) // The contents does not contain unaccompanied '
      }
    }
  }
}

object QueriesSpec {
  // XXX dedup with ValuePredicateTest
  import cats.data.Chain, doobie.util.fragment.{Elem, Fragment}
  import language.reflectiveCalls, Elem.{Arg, Opt}

  private def fragmentElems(frag: Fragment): Chain[Any \/ Option[Any]] = {
    frag.asInstanceOf[{ def elems: Chain[Elem] }].elems.map {
      case Arg(a, _) => \/.left(a)
      case Opt(o, _) => \/.right(o)
    }
  }

  private def fragmentSql(frag: Fragment): String =
    frag.asInstanceOf[{ def sql: String }].sql
}
