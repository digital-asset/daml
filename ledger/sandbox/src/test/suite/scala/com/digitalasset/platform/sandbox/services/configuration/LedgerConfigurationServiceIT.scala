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
import com.digitalasset.platform.api.grpc.GrpcApiUtil
import com.digitalasset.platform.sandbox.services.SandboxFixture
import com.digitalasset.resources.ResourceOwner
import com.digitalasset.testing.postgresql.PostgresResource
import org.scalatest.{Matchers, WordSpec}
import scalaz.syntax.tag._

sealed trait LedgerConfigurationServiceITBase extends WordSpec with Matchers {
  self: SandboxFixture with SuiteResourceManagement =>

  "LedgerConfigurationService" when {
    "asked for ledger configuration" should {
      "return expected configuration" in {
        val LedgerConfiguration(Some(minTtl), Some(maxTtl)) =
          LedgerConfigurationServiceGrpc
            .blockingStub(channel)
            .getLedgerConfiguration(GetLedgerConfigurationRequest(ledgerId().unwrap))
            .next()
            .getLedgerConfiguration

        minTtl shouldEqual GrpcApiUtil.durationToProto(config.timeModel.minTtl)
        maxTtl shouldEqual GrpcApiUtil.durationToProto(config.timeModel.maxTtl)
      }
    }
  }
}

final class LedgerConfigurationServiceInMemoryIT
    extends LedgerConfigurationServiceITBase
    with SandboxFixture
    with SuiteResourceManagementAroundAll

final class LedgerConfigurationServicePostgresIT
    extends LedgerConfigurationServiceITBase
    with SandboxFixture
    with SuiteResourceManagementAroundAll {
  override protected def database: Option[ResourceOwner[String]] =
    Some(PostgresResource.owner().map(_.jdbcUrl))
}
