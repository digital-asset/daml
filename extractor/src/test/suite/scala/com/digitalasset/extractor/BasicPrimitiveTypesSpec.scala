// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll

import org.scalatest._
import io.circe.parser._
import java.io.File

import scalaz._
import Scalaz._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class BasicPrimitiveTypesSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File("extractor/PrimitiveTypes.dar")

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
          "text_field" : "Hey",
          "bool_field" : true,
          "party_field" : "Bob",
          "date_field" : 18314,
          "time_field" : 1582373594000000
        }
      """,
      """
        {
          "reference" : "Positive extremes",
          "int_field" : 9223372036854775807,
          "decimal_field" : "9999999999999999999999999999.9999999999",
          "text_field" : "Hey",
          "bool_field" : true,
          "party_field" : "Bob",
          "date_field" : 2932896,
          "time_field" : 253402300799000000
        }
      """,
      """
        {
          "reference" : "Negative extremes",
          "int_field" : -9223372036854775808,
          "decimal_field" : "-9999999999999999999999999999.9999999999",
          "text_field" : "Hey",
          "bool_field" : true,
          "party_field" : "Bob",
          "date_field" : -719162,
          "time_field" : -62135596800000000
        }
      """
    ).traverseU(parse)

    expected should be('right) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.right.get
  }
}
