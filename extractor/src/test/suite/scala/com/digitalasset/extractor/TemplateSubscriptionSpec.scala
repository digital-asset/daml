// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.extractor

import java.io.File

import com.digitalasset.daml.bazeltools.BazelRunfiles._
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.extractor.config.{ExtractorConfig, TemplateConfig}
import com.digitalasset.extractor.services.ExtractorFixtureAroundAll
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import org.scalatest.{FlatSpec, Inside, Matchers, Suite}
import scalaz.OneAnd

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TemplateSubscriptionSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Matchers
    with Inside {

  override protected def darFile = new File(rlocation("extractor/TransactionExample.dar"))

  override def scenario: Option[String] = Some("TransactionExample:templateFilterTest")

  override def configureExtractor(ec: ExtractorConfig): ExtractorConfig = {
    val ec2 = super.configureExtractor(ec)
    ec2.copy(
      parties = OneAnd(Party assertFromString "Bob", Nil),
      templateConfigs = Set(TemplateConfig("TransactionExample", "RightOfUseAgreement")))
  }

  "Transactions" should "be extracted" in {
    getTransactions should have length 1
  }

  "Exercises" should "be extracted" in {
    getExercises should have length 0
  }

  "Contracts" should "be extracted" in {
    inside(getContracts) {
      case List(contract) =>
        contract.template should ===("TransactionExample:RightOfUseAgreement")
    }
  }
}
