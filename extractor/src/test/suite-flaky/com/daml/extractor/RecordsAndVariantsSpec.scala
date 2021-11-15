// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import io.circe.parser._
import org.scalatest.{Inside, Suite}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList
import scalaz.Scalaz._

class RecordsAndVariantsSpec
    extends AnyFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File(rlocation("extractor/test.dar"))

  override protected val initScript = Some("RecordsAndVariants:suite")

  override protected val parties = NonEmptyList("Suite")

  private val party: String = parties.head

  "Contracts" should "be extracted" in {
    val contracts = getContracts

    contracts should have length 1
  }

  it should "contain the correct JSON data" in {
    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      s"""
        {
          "party" : "$party",
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
    ).traverse(parse)

    expected should be(Symbol("right")) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.toOption.get
  }
}
