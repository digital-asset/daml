// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.CreateCommand
import com.digitalasset.ledger.api.v1.value.{Record, RecordField, Value}
import com.digitalasset.platform.apitesting.{MultiLedgerFixture, TestTemplateIds}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.participant.util.ValueConversions._
import org.scalatest.AsyncTestSuite
import scalaz.syntax.tag._

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any"
  ))
trait MultiLedgerCommandUtils extends MultiLedgerFixture {
  self: AsyncTestSuite =>

  protected val testTemplateIds = new TestTemplateIds(config)
  protected val templateIds = testTemplateIds.templateIds

  protected val testLedgerId = domain.LedgerId("ledgerId")
  protected val testNotLedgerId = domain.LedgerId("hotdog")
  protected val submitRequest: SubmitRequest =
    MockMessages.submitRequest.update(
      _.commands.ledgerId := testLedgerId.unwrap,
      _.commands.commands := List(
        CreateCommand(
          Some(templateIds.dummy),
          Some(
            Record(
              Some(templateIds.dummy),
              Seq(RecordField(
                "operator",
                Option(
                  Value(Value.Sum.Party(MockMessages.submitAndWaitRequest.commands.get.party)))))))
        ).wrap)
    )

  override protected def config: Config =
    Config.default.withLedgerIdMode(LedgerIdMode.Static(testLedgerId))
}
