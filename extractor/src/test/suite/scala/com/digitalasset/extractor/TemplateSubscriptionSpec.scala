package com.digitalasset.extractor

import java.io.File

import com.digitalasset.extractor.config.{ExtractorConfig, TemplateConfig}
import com.digitalasset.extractor.services.ExtractorFixtureAroundAll
import com.digitalasset.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.digitalasset.platform.sandbox.persistence.PostgresAroundAll
import org.scalatest.{FlatSpec, Matchers, Suite}

class TemplateSubscriptionSpec
    extends FlatSpec
    with Suite
    with PostgresAroundAll
    with SuiteResourceManagementAroundAll
    with ExtractorFixtureAroundAll
    with Matchers {

  override protected def darFile = new File("extractor/TransactionExample.dar")

  override def scenario: Option[String] = Some("TransactionExample:example")

  override def configureExtractor(ec: ExtractorConfig): ExtractorConfig = {
    val ec2 = super.configureExtractor(ec)
    ec2.copy(templateConfigs = Set(TemplateConfig("TransactionExample", "RightOfUseAgreement")))
  }

  "Transactions" should "be extracted" in {
    getTransactions should have length 2
  }

  "Exercises" should "be extracted" in {
    getExercises should have length 1
  }

  "Contracts" should "be extracted" in {
    getContracts should have length 2
  }

}
