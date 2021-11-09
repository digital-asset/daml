// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import io.circe.parser._
import org.scalatest._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList
import scalaz.Scalaz._

class TextMapsSpec
    extends AnyFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File(rlocation("extractor/test.dar"))

  override protected val initScript = Some("PrimitiveTypes:textMaps")

  override protected val parties = NonEmptyList("TextMaps")

  private val party: String = parties.head

  "TextMaps" should "be extracted" in {
    val contracts = getContracts

    contracts should have length 2
  }

  it should "contain the correct JSON data" in {
    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      s"""
        {
          "reference" : "Empty maps",
          "map" : {},
          "deep_map" : {},
          "party" : "$party"
        }
      """,
      s"""
        {
         "reference" : "Non-empty maps",
         "map" : { "1" : 1 ,
                   "2" : 2 ,
                   "3" : 3 ,
                   "4" : 4 ,
                   "5" : 5 },
         "deep_map" : {},
         "party" : "$party"
        }
      """,
    ).traverse(parse)

    expected should be(Symbol("right")) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.toOption.get
  }
}
