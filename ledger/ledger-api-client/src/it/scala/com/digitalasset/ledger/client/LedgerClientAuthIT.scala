// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.client

import com.daml.grpc.GrpcException
import com.daml.ledger.api.domain
import com.daml.ledger.api.testing.utils.{AkkaBeforeAndAfterAll, SuiteResourceManagementAroundEach}
import com.daml.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
  LedgerIdRequirement,
}
import com.daml.platform.common.LedgerIdMode
import com.daml.platform.sandbox.SandboxRequiringAuthorization
import com.daml.platform.sandbox.config.SandboxConfig
import com.daml.platform.sandboxnext.SandboxNextFixture
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec
import scalaz.syntax.tag._

final class LedgerClientAuthIT
    extends AsyncWordSpec
    with Matchers
    with Inside
    with AkkaBeforeAndAfterAll
    with SuiteResourceManagementAroundEach
    with SandboxNextFixture
    with SandboxRequiringAuthorization {

  private val LedgerId =
    domain.LedgerId(s"${classOf[LedgerClientAuthIT].getSimpleName.toLowerCase}-ledger-id")

  private val ClientConfigurationWithoutToken = LedgerClientConfiguration(
    applicationId = classOf[LedgerClientAuthIT].getSimpleName,
    ledgerIdRequirement = LedgerIdRequirement.none,
    commandClient = CommandClientConfiguration.default,
    sslContext = None,
    token = None,
  )

  private val ClientConfiguration = ClientConfigurationWithoutToken.copy(
    token = Some(toHeader(readOnlyToken("Read-only party")))
  )

  override protected def config: SandboxConfig = super.config.copy(
    ledgerIdMode = LedgerIdMode.Static(LedgerId)
  )

  "the ledger client" when {
    "it has a read-only token" should {
      "retrieve the ledger ID" in {
        for {
          client <- LedgerClient(channel, ClientConfiguration)
        } yield {
          client.ledgerId should be(LedgerId)
        }
      }

      "fail to conduct an admin operation with the same token" in {
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          exception <- client.partyManagementClient
            .allocateParty(hint = Some("Bob"), displayName = None)
            .failed
        } yield {
          inside(exception) { case GrpcException.PERMISSION_DENIED() =>
            succeed
          }
        }
      }

      "succeed in conducting an admin operation with an admin token" in {
        val partyName = "Carol"
        for {
          client <- LedgerClient(channel, ClientConfiguration)
          allocatedParty <- client.partyManagementClient
            .allocateParty(
              hint = Some(partyName),
              displayName = Some(partyName),
              token = Some(toHeader(adminToken)),
            )
        } yield {
          allocatedParty.displayName should be(Some(partyName))
        }
      }
    }

    "it does not have a token & no ledgerIdRequirement specified" should {
      "fail to construct" in {
        for {
          exception <- LedgerClient(channel, ClientConfigurationWithoutToken).failed
        } yield {
          inside(exception) { case GrpcException.UNAUTHENTICATED() =>
            succeed
          }
        }
      }
    }

    "it does have a ledgerIdRequirement but not a token " should {
      "construct" in {
        for {
          client <- LedgerClient(
            channel,
            ClientConfigurationWithoutToken.copy(ledgerIdRequirement =
              LedgerIdRequirement(Some(LedgerId.unwrap))
            ),
          )
        } yield client.ledgerId should equal(this.LedgerId)
      }
    }
  }
}
