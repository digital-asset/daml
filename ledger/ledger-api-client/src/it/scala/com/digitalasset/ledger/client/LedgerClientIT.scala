// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundEach}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement
}
import com.daml.platform.sandboxnext.SandboxNextFixture
import io.grpc.ManagedChannel
import org.scalatest.{AsyncWordSpec, Inside, Matchers}

final class LedgerClientIT
    extends AsyncWordSpec
    with Matchers
    with Inside
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundEach
    with SandboxNextFixture {
  "the ledger client" should {
    "shut down the channel when closed" in {
      val clientConfig = LedgerClientConfiguration(
        applicationId = classOf[LedgerClientIT].getSimpleName,
        ledgerIdRequirement = LedgerIdRequirement("", enabled = false),
        commandClient = CommandClientConfiguration.default,
        sslContext = None,
        token = None,
      )

      for {
        client <- LedgerClient(channel, clientConfig)
      } yield {
        inside(channel) {
          case channel: ManagedChannel =>
            channel.isShutdown should be(false)
            channel.isTerminated should be(false)

            client.close()

            channel.isShutdown should be(true)
            channel.isTerminated should be(true)
        }
      }
    }
  }
}
