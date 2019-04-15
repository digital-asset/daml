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
class MapsSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File("extractor/PrimitiveTypes.dar")

  override def scenario: Option[String] = Some("PrimitiveTypes:maps")

  "Lists" should "be extracted" in {
    val contracts = getContracts

    contracts should have length 2
  }

  it should "contain the correct JSON data" in {
    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      """
        {
          "reference" : "Empty maps",
          "map" : { "Map" : {} },
          "deep_map" : { "Map" : {} },
          "party" : "Bob"
        }
      """,
      """
        {
         "reference" : "Non-empty maps",
         "map" : { "Map" :   { "1" : 1 ,
                               "2" : 2 ,
                               "3" : 3 ,
                               "4" : 4 ,
                               "5" : 5 } },
         "deep_map" : { "Map" : {} },
         "party" : "Bob"
        }
      """
    ).traverseU(parse)

    expected should be('right) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.right.get
  }
}
