// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandboxnext.services.configuration

import java.time.Duration

import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundEach
import com.daml.ledger.api.v1.command_completion_service.{
  CommandCompletionServiceGrpc,
  CompletionEndRequest
}
import com.daml.platform.ApiOffset
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandbox.services.TestCommands
import com.daml.platform.sandbox.services.TimeModelHelpers.publishATimeModel
import com.daml.platform.sandboxnext.SandboxNextFixture
import org.scalatest.Inspectors
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

final class ConfigurationServiceWithEmptyLedgerIT
    extends AsyncWordSpec
    with Matchers
    with Inspectors
    with SandboxNextFixture
    with SandboxBackend.Postgresql
    with TestCommands
    with SuiteResourceManagementAroundEach {

  // Start with no DAML packages and a large configuration delay, such that we can test the API's
  // behavior on an empty index.
  override protected def config: SandboxConfig =
    super.config.copy(
      damlPackages = List.empty,
      ledgerConfig = super.config.ledgerConfig.copy(
        initialConfigurationSubmitDelay = Duration.ofDays(5),
      ),
      implicitPartyAllocation = false,
    )

  "ConfigManagementService accepts a configuration when none is set" in {
    val lid = ledgerId().unwrap
    val completionService = CommandCompletionServiceGrpc.stub(channel)
    for {
      _ <- publishATimeModel(channel)
      end <- completionService.completionEnd(CompletionEndRequest(lid))
    } yield {
      end.getOffset.value.absolute.get should not be ApiOffset.begin.toHexString
    }
  }
}
