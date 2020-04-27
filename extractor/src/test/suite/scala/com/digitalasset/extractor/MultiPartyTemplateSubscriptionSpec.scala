// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.data.Ref.Party
import com.daml.extractor.config.{ExtractorConfig, TemplateConfig}
import com.daml.extractor.services.ExtractorFixtureAroundAll
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.{FlatSpec, Inside, Matchers, Suite}
import scalaz.OneAnd

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class MultiPartyTemplateSubscriptionSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Matchers
    with Inside {

  override protected def darFile = new File(rlocation("extractor/TransactionExample.dar"))

  override def scenario: Option[String] = Some("TransactionExample:templateFilterTest")

  private final val alice = Party assertFromString "Alice"
  private final val bob = Party assertFromString "Bob"

  override def configureExtractor(ec: ExtractorConfig): ExtractorConfig = {
    val ec2 = super.configureExtractor(ec)
    ec2.copy(
      parties = OneAnd(alice, List(bob)),
      templateConfigs = Set(
        TemplateConfig("TransactionExample", "RightOfUseOffer"),
        TemplateConfig("TransactionExample", "RightOfUseAgreement"))
    )
  }

  "Transactions" should "be extracted" in {
    getTransactions should have length 2
  }

  "Exercises" should "be extracted" in {
    inside(getExercises) {
      case List(e) =>
        e.template should ===("TransactionExample:RightOfUseOffer")
        e.choice should ===("Accept")
    }
  }

  "Contracts" should "be extracted" in {
    inside(getContracts) {
      case List(a1, a2) =>
        a1.template should ===("TransactionExample:RightOfUseOffer")
        a2.template should ===("TransactionExample:RightOfUseAgreement")
    }
  }
}
