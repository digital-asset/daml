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

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class RecordsAndVariantsSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File(rlocation("extractor/RecordsAndVariants.dar"))

  override def scenario: Option[String] = Some("RecordsAndVariants:suite")

  "Contracts" should "be extracted" in {
    val contracts = getContracts

    contracts should have length 1
  }

  it should "contain the correct JSON data" in {
    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      """
        {
          "party" : "Bob",
          "reference" : "All-in-one",
          "deepNested" : {
            "tag" : "MaybeRecRecordABRight",
            "value" : {
              "baz" : {
                "baz" : false,
                "foo" : "foo"
              },
              "foo" : [
                [1, 2, 3],
                null,
                [4, 5, 6],
                null,
                [7, 8, 9]
              ]
            }
          },
          "enum" : "EitherRight",
          "simpleRecord" : {
            "foo" : true
          },
          "eitherVariant" : {
            "tag": "RightM",
            "value" : 7
          },
          "recordTextInt" : {
            "baz" : 6,
            "foo" : "Foo"
          }
        }
      """
    ).traverseU(parse)

    expected should be('right) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.right.get
  }
}
