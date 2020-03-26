// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.services.configuration

import com.digitalasset.ledger.api.testing.utils.{
  SuiteResourceManagement,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  LedgerConfiguration,
  LedgerConfigurationServiceGrpc
}
import com.digitalasset.platform.sandbox.SandboxBackend
import com.digitalasset.platform.sandbox.services.SandboxFixture
import com.google.protobuf.duration.Duration
import org.scalatest.{Matchers, WordSpec}
import scalaz.syntax.tag._

sealed trait LedgerConfigurationServiceITBase extends WordSpec with Matchers {
  self: SandboxFixture with SuiteResourceManagement =>

  "LedgerConfigurationService" when {
    "asked for ledger configuration" should {
      "return expected configuration" in {
        val LedgerConfiguration(Some(maxDeduplicationTime)) =
          LedgerConfigurationServiceGrpc
            .blockingStub(channel)
            .getLedgerConfiguration(GetLedgerConfigurationRequest(ledgerId().unwrap))
            .next()
            .getLedgerConfiguration

        maxDeduplicationTime shouldEqual toProto(config.submissionConfig.maxDeduplicationTime)
      }
    }
  }

  private def toProto(t: java.time.Duration): Duration =
    Duration.of(t.getSeconds, t.getNano)
}

final class LedgerConfigurationServiceInMemoryIT
    extends LedgerConfigurationServiceITBase
    with SandboxFixture
    with SuiteResourceManagementAroundAll

final class LedgerConfigurationServicePostgresIT
    extends LedgerConfigurationServiceITBase
    with SandboxFixture
    with SandboxBackend.Postgresql
    with SuiteResourceManagementAroundAll
