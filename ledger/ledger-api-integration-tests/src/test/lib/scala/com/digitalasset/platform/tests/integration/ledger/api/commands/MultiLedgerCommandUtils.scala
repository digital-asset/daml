// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.testing.utils.MockMessages
import com.digitalasset.ledger.api.testing.utils.MockMessages.{applicationId, workflowId}
import com.digitalasset.ledger.api.v1.command_service.SubmitAndWaitRequest
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.Commands
import com.digitalasset.ledger.client.services.commands.SynchronousCommandClient
import com.digitalasset.platform.apitesting.{LedgerContext, MultiLedgerFixture}
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.tests.integration.ledger.api.TransactionServiceHelpers
import org.scalatest.AsyncTestSuite

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any"
  ))
trait MultiLedgerCommandUtils extends TransactionServiceHelpers with MultiLedgerFixture {
  self: AsyncTestSuite =>

  protected final def newSynchronousCommandClient(ctx: LedgerContext): SynchronousCommandClient =
    new SynchronousCommandClient(ctx.commandService)

  protected val testLedgerId = Ref.LedgerName.assertFromString("ledgerId")
  protected val testNotLedgerId = Ref.LedgerName.assertFromString("hotdog")
  protected val submitRequest: SubmitRequest =
    MockMessages.submitRequest.update(_.commands.ledgerId := testLedgerId)

  protected val failingRequest: SubmitRequest =
    submitRequest.copy(
      commands = Some(
        Commands()
          .withParty("Alice")
          .withLedgerId(testLedgerId)
          .withCommandId(failingCommandId)
          .withWorkflowId(workflowId)
          .withApplicationId(applicationId)
          .withCommands(Seq(wrongCreate))))

  protected val submitAndWaitRequest: SubmitAndWaitRequest =
    MockMessages.submitAndWaitRequest
      .update(_.commands.ledgerId := testLedgerId)
      .copy(traceContext = None)
  protected val failingSubmitAndWaitRequest: SubmitAndWaitRequest = submitAndWaitRequest.copy(
    commands = MockMessages.submitAndWaitRequest.commands
      .map(_.copy(commandId = "fails", ledgerId = "not ledger id")))

  override protected def config: Config =
    Config.default.withLedgerIdMode(LedgerIdMode.Static(testLedgerId))
}
