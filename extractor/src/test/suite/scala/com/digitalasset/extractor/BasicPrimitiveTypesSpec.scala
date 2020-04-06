// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import io.circe.parser._
import org.scalatest._
import scalaz.Scalaz._

class BasicPrimitiveTypesSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File(rlocation("extractor/PrimitiveTypes.dar"))

  override def scenario: Option[String] = Some("PrimitiveTypes:primitives")

  "Contracts" should "be extracted" in {
    val contracts = getContracts

    contracts should have length 3
  }

  it should "contain the correct JSON data" in {
    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      """
        {
          "reference" : "Simple values",
          "int_field" : 5,
          "decimal_field" : "5.5",
          "numeric0_field" : "42.0",
          "numeric37_field" : "0.25",
          "text_field" : "Hey",
          "bool_field" : true,
          "party_field" : "Bob",
          "date_field" : "2020-02-22",
          "time_field" : "2020-02-22T12:13:14Z"
        }
      """,
      """
        {
          "reference" : "Positive extremes",
          "int_field" : 9223372036854775807,
          "decimal_field" : "9999999999999999999999999999.9999999999",
          "numeric0_field" : "99999999999999999999999999999999999999.0",
          "numeric37_field" : "9.9999999999999999999999999999999999999",
          "text_field" : "Hey",
          "bool_field" : true,
          "party_field" : "Bob",
          "date_field" : "9999-12-31",
          "time_field" : "9999-12-31T23:59:59Z"
        }
      """,
      """
        {
          "reference" : "Negative extremes",
          "int_field" : -9223372036854775808,
          "decimal_field" : "-9999999999999999999999999999.9999999999",
          "numeric0_field" : "-99999999999999999999999999999999999999.0",
          "numeric37_field" : "-9.9999999999999999999999999999999999999",
          "text_field" : "Hey",
          "bool_field" : true,
          "party_field" : "Bob",
          "date_field" : "0001-01-01",
          "time_field" : "0001-01-01T00:00:00Z"
        }
      """
    ).traverseU(parse)

    expected should be('right) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.right.get
  }
}
