// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.services.configuration

import com.daml.ledger.api.testing.utils.{SuiteResourceManagement, SuiteResourceManagementAroundAll}
import com.daml.ledger.api.v1.ledger_configuration_service.{
  GetLedgerConfigurationRequest,
  LedgerConfiguration,
  LedgerConfigurationServiceGrpc,
}
import com.daml.ledger.sandbox.BridgeConfig
import com.daml.platform.sandbox.SandboxBackend
import com.daml.platform.sandbox.fixture.SandboxFixture
import com.google.protobuf.duration.Duration
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import scalaz.syntax.tag._

sealed trait LedgerConfigurationServiceITBase extends AnyWordSpec with Matchers {
  self: SandboxFixture with SuiteResourceManagement =>

  "LedgerConfigurationService" when {
    "asked for ledger configuration" should {
      "return expected configuration" in {
        val LedgerConfiguration(Some(maxDeduplicationDuration)) =
          LedgerConfigurationServiceGrpc
            .blockingStub(channel)
            .getLedgerConfiguration(GetLedgerConfigurationRequest(ledgerId().unwrap))
            .next()
            .getLedgerConfiguration

        maxDeduplicationDuration shouldEqual toProto(
          BridgeConfig.DefaultMaximumDeduplicationDuration
        )
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
