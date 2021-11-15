// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles.rlocation
import com.daml.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import io.circe.parser._
import org.scalatest.{Inside, Suite}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList
import scalaz.Scalaz._

class EnumSpec
    extends AnyFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File(rlocation("extractor/test.dar"))

  override protected val initScript: Option[String] = Some("EnumMod:createContracts")

  override protected val parties: NonEmptyList[String] = NonEmptyList("EnumMod")

  private val party = parties.head

  "Enum" should "be extracted" in {
    getContracts should have length 3
  }

  it should "contain the correct JSON data" in {

    val contractsJson = getContracts.map(_.create_arguments)

    val expected = List(
      s"""{
      "x" : "Red",
      "party" : "$party"
      }""",
      s"""{
      "x" : "Green",
      "party" : "$party"
      }""",
      s"""{
      "x" : "Blue",
      "party" : "$party"
      }""",
    ).traverse(parse)

    expected should be(Symbol("right")) // That should only fail if this JSON^^ is ill-formatted

    contractsJson should contain theSameElementsAs expected.toOption.get
  }

}
