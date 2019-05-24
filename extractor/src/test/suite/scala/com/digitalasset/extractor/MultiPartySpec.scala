// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import com.digitalasset.extractor.config.ExtractorConfig
import com.digitalasset.extractor.services.{CustomMatchers, ExtractorFixtureAroundAll}
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll

import org.scalatest._
import java.io.File

import scalaz._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.foldable._

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class MultiPartySpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Inside
    with Matchers
    with CustomMatchers {

  override protected def darFile = new File("extractor/RecordsAndVariants.dar")

  override def scenario: Option[String] = Some("RecordsAndVariants:multiParty")

  override def configureExtractor(ec: ExtractorConfig): ExtractorConfig = {
    val ec2 = super.configureExtractor(ec)
    ec2.copy(parties = OneAnd("Alice", ec2.parties.toList))
  }

  "Contracts" should "contain the visible contracts" in {
    val ticks = getContracts.map { ct =>
      for {
        o <- ct.create_arguments.asObject
        tick <- o("tick")
        n <- tick.asNumber
        i <- n.toInt
      } yield i
    }

    val expected = List(1, 2, 4, 5, 7).map(some)

    ticks should contain theSameElementsAs expected
  }
}
