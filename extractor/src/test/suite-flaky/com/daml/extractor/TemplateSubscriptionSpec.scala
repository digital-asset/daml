// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.extractor

import java.io.File

import com.daml.bazeltools.BazelRunfiles._
import com.daml.extractor.config.{ExtractorConfig, TemplateConfig}
import com.daml.extractor.services.ExtractorFixtureAroundAll
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.testing.postgresql.PostgresAroundAll
import org.scalatest.{Inside, Suite}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scalaz.NonEmptyList

class TemplateSubscriptionSpec
    extends AnyFlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Matchers
    with Inside {

  override protected def darFile = new File(rlocation("extractor/test.dar"))

  override protected val initScript: Option[String] = Some("TransactionExample:templateFilterTest")

  override protected val parties = NonEmptyList("TemplateFilterTest2")

  override def configureExtractor(ec: ExtractorConfig): ExtractorConfig = {
    val ec2 = super.configureExtractor(ec)
    ec2.copy(
      templateConfigs = Set(TemplateConfig("TransactionExample", "RightOfUseAgreement"))
    )
  }

  "Transactions" should "be extracted" in {
    getTransactions should have length 1
  }

  "Exercises" should "be extracted" in {
    getExercises should have length 0
  }

  "Contracts" should "be extracted" in {
    inside(getContracts) { case List(contract) =>
      contract.template should ===("TransactionExample:RightOfUseAgreement")
    }
  }
}
