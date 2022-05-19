// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.sandbox

import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.runner.common.{CliConfigConverter, Config}
import com.daml.platform.apiserver.ApiServerConfig
import com.daml.platform.sandbox.config.{LedgerName, SandboxConfig}
import munit.{ComparisonFailException, Assertions => MUnit}

object SandboxConfigChecker {

  def assertEquals[T](context: String, actual: T, expected: T): Unit = {
    try {
      MUnit.assertEquals(actual, expected, context)
    } catch {
      case e: ComparisonFailException =>
        println(e.message)
    }
  }

  def compare(sandboxConfig: SandboxConfig, newSandboxConfig: NewSandboxServer.CustomConfig) = {
    val genericCliConfig =
      ConfigConverter.toSandboxOnXConfig(sandboxConfig, None, LedgerName("LedgerName"))
    val bridgeConfigAdaptor: BridgeConfigAdaptor = new BridgeConfigAdaptor {
      override def authService(apiServerConfig: ApiServerConfig): AuthService =
        sandboxConfig.authService.getOrElse(AuthServiceWildcard)
    }
    val converted = CliConfigConverter.toConfig(bridgeConfigAdaptor, genericCliConfig)
    check(newSandboxConfig.genericConfig, converted)
  }

  def check(converted: Config, defined: Config): Unit = {
    assertEquals("config objects", converted, defined)
  }

}
