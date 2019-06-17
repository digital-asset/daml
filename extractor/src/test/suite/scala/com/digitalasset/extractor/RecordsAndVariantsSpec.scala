// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll

import org.scalatest._
import io.circe.parser._
import java.io.File

import scalaz._
import Scalaz._

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
            "MaybeRecRecordABRight" : {
              "baz" : {
                "baz" : false,
                "foo" : "foo"
              },
              "foo" : [
                { "Some" : [1, 2, 3] },
                { "None" : {} },
                { "Some" : [4, 5, 6] },
                { "None" : {} },
                { "Some" : [7, 8, 9] }
              ]
            }
          },
          "enumVariant" : {
            "EitherRight" : {}
          },
          "simpleRecord" : {
            "foo" : true
          },
          "eitherVariant" : {
            "RightM" : 7
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
