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
class OptionalSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File("extractor/PrimitiveTypes.dar")

  override def scenario: Option[String] = Some("PrimitiveTypes:optionals")

  "Optionals" should "be extracted" in {
    val contracts = getContracts

    contracts should have length 3
  }

  it should "contain the correct JSON data" in {
    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      """
        {
          "reference" : "Nones",
          "optional" : {
            "None" : {}
          },
          "deep_optional" : {
            "None" : {}
          },
          "party" : "Bob"
        }
      """,
      """
        {
          "reference" : "Somes",
          "optional" : {
            "Some" : "foo"
          },
          "deep_optional" : {
            "Some" : {
              "Some" : "foo"
            }
          },
          "party" : "Bob"
        }
      """,
      """
        {
          "reference" : "Some None",
          "optional" : {
            "Some" : "foo"
          },
          "deep_optional" : {
            "Some" : {
              "None" : {}
            }
          },
          "party" : "Bob"
        }
      """
    ).traverseU(parse)

    expected should be('right) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.right.get
  }
}
