// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.testing.utils.{MockMessages => M}
import com.digitalasset.ledger.api.v1.command_submission_service.SubmitRequest
import com.digitalasset.ledger.api.v1.commands.CreateCommand
import com.digitalasset.ledger.api.v1.completion.Completion
import com.digitalasset.ledger.api.v1.value.{Identifier, Record}
import com.digitalasset.platform.apitesting.LedgerContext
import com.digitalasset.platform.participant.util.ValueConversions._
import com.google.rpc.code.Code
import org.scalatest.concurrent.Waiters
import org.scalatest.{Assertion, Matchers, OptionValues}
import scalaz.syntax.tag._

import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Any"))
class LedgerTestingHelpers(
    submitCommand: SubmitRequest => Future[Completion],
    context: LedgerContext,
    timeoutScaleFactor: Double = 1.0)
    extends Matchers
    with Waiters
    with OptionValues {

  override def spanScaleFactor: Double = timeoutScaleFactor

  def failingCreate(
      commandId: String,
      submitter: String,
      template: Identifier,
      arg: Record,
      code: Code,
      pattern: String
  ): Future[Assertion] =
    assertCommandFailsWithCode(
      M.submitRequest.update(
        _.commands.party := submitter,
        _.commands.commandId := commandId,
        _.commands.ledgerId := context.ledgerId.unwrap,
        _.commands.commands :=
          List(CreateCommand(Some(template), Some(arg)).wrap)
      ),
      code,
      pattern
    )

  def assertCommandFailsWithCode(
      submitRequest: SubmitRequest,
      expectedErrorCode: Code,
      expectedMessageSubString: String): Future[Assertion] =
    submitCommand(submitRequest).map { completion =>
      completion.getStatus should have('code (expectedErrorCode.value))
      completion.getStatus.message should include(expectedMessageSubString)
    }(DirectExecutionContext)

}
