// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.http.dbbackend

import doobie.implicits._
import com.daml.nonempty.NonEmpty
import com.daml.test.evidence.tag.Security.SecurityTest.Property.Integrity
import com.daml.test.evidence.tag.Security.{Attack, SecurityTest}
import org.scalatest.Inspectors.{forAll => cForAll}
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks => STSC}
import STSC.{forAll => scForAll}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec
import scala.collection.{immutable => imm}
import scalaz.\/

class QueriesSpec extends AnyWordSpec with Matchers with TableDrivenPropertyChecks {

  import QueriesSpec._

  "projectedIndex" should {
    import Queries.{projectedIndex, SurrogateTpId}

    val cases = Table(
      ("map", "projection"),
      (
        NonEmpty(Seq, (SurrogateTpId(55L), fr"foo") -> 0),
        sql"CASE WHEN (tpid = ${55L}) THEN ${0}||''  ELSE NULL END ",
      ),
      (
        NonEmpty(Seq, (SurrogateTpId(55L), fr"foo") -> 0, (SurrogateTpId(66L), fr"foo") -> 1),
        sql"CASE WHEN (tpid = ${55L}) THEN ${0}||''  WHEN (tpid = ${66L}) THEN ${1}||''  ELSE NULL END ",
      ),
      (
        NonEmpty(
          Seq,
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

  "chunkBySetSize" should {
    import org.scalacheck.{Arbitrary, Gen, Shrink}
    import Arbitrary.arbitrary
    import Queries.chunkBySetSize

    type Arg = NonEmpty[Map[Int, NonEmpty[Set[Int]]]]
    val sizes: Gen[Int] = Gen.choose(1, 20)
    implicit val doNotShrinkSize: Shrink[Int] = Shrink.shrinkAny
    val randomArg: Gen[Arg] = Gen
      .nonEmptyMap(
        Gen.zip(
          arbitrary[Int],
          Gen.nonEmptyContainerOf[Set, Int](arbitrary[Int]).map { case NonEmpty(xs) => xs },
        )
      )
      .map { case NonEmpty(xs) => xs }
    implicit def shrinkNE[Self <: imm.Iterable[Any]: Shrink]: Shrink[NonEmpty[Self]] =
      Shrink { nes => Shrink shrink nes.forgetNE collect { case NonEmpty(s) => s } }

    // at 1k each test takes ~500ms; at 10k each ~5s
    implicit val generatorDrivenConfig: STSC.PropertyCheckConfiguration =
      STSC.generatorDrivenConfig.copy(minSuccessful = 1000)

    "include all arguments in the result" in scForAll(sizes, randomArg) { (s, r) =>
      chunkBySetSize(s, r).transform((_, chunks) => chunks.flatten.toSet) should ===(r)
    }

    def measuredChunkSize(chunk: NonEmpty[Iterable[_]]) =
      chunk.size

    "never exceed size in each chunk" in scForAll(sizes, randomArg) { (s, r) =>
      all(chunkBySetSize(s, r).values.flatten map measuredChunkSize) should be <= s
    }

    "make chunks as large as possible" in scForAll(sizes, randomArg) { (s, r) =>
      all(chunkBySetSize(s, r).values.flatMap(_.init) map measuredChunkSize) should ===(s)
    }

    "make chunks that do not intersect" in scForAll(sizes, randomArg) { (s, r) =>
      cForAll(chunkBySetSize(s, r).toSeq.forgetNE) { case (_, chunks) =>
        cForAll(chunks.sliding(2, 1).toSeq) {
          case Seq(a, b) => a intersect b shouldBe empty
          case Seq(_) => succeed
          case _ => fail("impossible sliding or empty output")
        }
      }
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

  "deterministicDeleteOrder" should {
    import Queries.deterministicDeleteOrder
    import util.Random.shuffle
    import collection.immutable.{ListMap, ListSet}

    implicit val generatorDrivenConfig: STSC.PropertyCheckConfiguration =
      STSC.generatorDrivenConfig.copy(minSuccessful = 1000)

    "give the same sequence for any map/set order" in scForAll { m: Map[Int, Set[Int]] =>
      deterministicDeleteOrder(shuffle(m.toSeq) map { case (k, v) =>
        (k, shuffle(v.toSeq) to ListSet)
      } to ListMap) should ===(deterministicDeleteOrder(m))
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
