// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api.commands

import com.digitalasset.ledger.api.testing.utils.MockMessages.submitRequest
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.command_submission_service.{
  CommandSubmissionServiceGrpc,
  SubmitRequest
}
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import io.grpc.Channel
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import org.scalatest.{AsyncWordSpec, Matchers, OptionValues}

class CommandSubmissionServiceIT
    extends AsyncWordSpec
    with SuiteResourceManagementAroundAll
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with AsyncTimeLimitedTests
    with Matchers
    with OptionValues {

  override def timeLimit: Span = 5.seconds

  override protected def config: Config = Config.default

  private def client(channel: Channel) = CommandSubmissionServiceGrpc.stub(channel)

  "Command Submission Service" when {

    "commands arrive with extreme TTLs" should {

      "successfully submit commands" in allFixtures { implicit c =>
        c.commandSubmissionService.submit(
          SubmitRequest(Some(submitRequest.getCommands.withLedgerId(config.assertStaticLedgerId)))) map {
          _ =>
            succeed
        }
      }
    }
  }
}
