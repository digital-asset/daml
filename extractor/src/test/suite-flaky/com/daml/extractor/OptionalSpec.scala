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

class OptionalSpec
    extends AnyFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File(rlocation("extractor/test.dar"))

  override protected val initScript = Some("PrimitiveTypes:optionals")

  override protected val parties = NonEmptyList("Optionals")

  private val party: String = parties.head

  "Optionals" should "be extracted" in {
    val contracts = getContracts

    contracts should have length 3
  }

  it should "contain the correct JSON data" in {
    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      s"""
        {
          "reference" : "Nones",
          "optional" : null,
          "deep_optional" : null,
          "party" : "$party"
        }
      """,
      s"""
        {
          "reference" : "Somes",
          "optional" : "foo",
          "deep_optional" : ["foo"],
          "party" : "$party"
        }
      """,
      s"""
        {
          "reference" : "Some None",
          "optional" : "foo",
          "deep_optional" : [],
          "party" : "$party"
        }
      """,
    ).traverse(parse)

    expected should be(Symbol("right")) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.toOption.get
  }
}
