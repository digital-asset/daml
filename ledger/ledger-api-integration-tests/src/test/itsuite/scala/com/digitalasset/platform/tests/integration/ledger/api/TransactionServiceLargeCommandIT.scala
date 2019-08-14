// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.commands.Command.Command.Create
import com.digitalasset.ledger.api.v1.commands.{Command, CreateCommand}
import com.digitalasset.ledger.api.v1.value.{Identifier, Value}
import com.digitalasset.platform.apitesting.LedgerContextExtensions._
import com.digitalasset.platform.apitesting.TestParties._
import com.digitalasset.platform.apitesting.{
  MultiLedgerFixture,
  TestTemplateIds,
  TransactionFilters
}
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import com.digitalasset.platform.participant.util.ValueConversions._
import com.digitalasset.platform.services.time.TimeProviderType
import com.google.protobuf.timestamp.Timestamp
import org.scalatest._
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import scala.collection.immutable

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class TransactionServiceLargeCommandIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with Inside
    with AsyncTimeLimitedTests
    with TestExecutionSequencerFactory
    with ParameterShowcaseTesting
    with OptionValues
    with Matchers {

  override protected val config: Config =
    Config.default.withTimeProvider(TimeProviderType.Static)

  protected val helpers = new TransactionServiceHelpers(config)
  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds

  override val timeLimit: Span = scaled(300.seconds)

  "Transaction Service" when {

    "submitting and reading transactions" should {

      "accept huge submissions with a large number of commands" in allFixtures { c =>
        val targetNumberOfSubCommands = 15000
        val superSizedCommand = c
          .command(
            "Huge-composite-command",
            Alice,
            List.fill(targetNumberOfSubCommands)(
              Command(create(templateIds.dummy, List("operator" -> Alice.asParty)))))
          .update(_.commands.maximumRecordTime := Timestamp(60L, 0))

        c.testingHelpers
          .submitAndListenForSingleResultOfCommand(
            superSizedCommand,
            TransactionFilters.allForParties(Alice))
          .map { tx =>
            tx.events.size shouldEqual targetNumberOfSubCommands
          }
      }
    }
  }

  private def create(templateId: Identifier, arguments: immutable.Seq[(String, Value)]): Create =
    Create(CreateCommand(Some(templateId), Some(arguments.asRecordOf(templateId))))

  def getHead[T](elements: Iterable[T]): T = {
    elements should have size 1
    elements.headOption.value
  }

}
