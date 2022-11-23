// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import akka.stream.scaladsl.Flow
import com.daml.ledger.api.refinements.ApiTypes.ApplicationId
import com.daml.ledger.api.testing.utils.SuiteResourceManagementAroundAll
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.ledger.client.configuration.LedgerClientConfiguration
import com.daml.lf.data.Ref._
import com.daml.lf.engine.trigger.Runner.TriggerContext
import com.daml.lf.engine.trigger.TriggerMsg
import com.daml.platform.sandbox.SandboxRequiringAuthorization
import com.daml.platform.sandbox.fixture.SandboxFixture
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class Jwt
    extends AsyncWordSpec
    with AbstractTriggerTest
    with SandboxFixture
    with SandboxRequiringAuthorization
    with Matchers
    with SuiteResourceManagementAroundAll
    with TryValues {
  self: Suite =>

  // Override to make sure we set it correctly.
  override protected val applicationId: ApplicationId = ApplicationId("custom app id")

  override protected def ledgerClientConfiguration: LedgerClientConfiguration =
    super.ledgerClientConfiguration.copy(
      token = Some(toHeader(forApplicationId("custom app id", readWriteToken(party))))
    )

  private val party = "AliceAuth"

  "Jwt" can {
    // We just need something simple to test the connection.
    val assetId = LedgerApi.Identifier(packageId, "ACS", "Asset")
    val assetMirrorId = LedgerApi.Identifier(packageId, "ACS", "AssetMirror")

    def asset(party: String): CreateCommand =
      CreateCommand(
        templateId = Some(assetId),
        createArguments = Some(
          LedgerApi.Record(
            fields = Seq(LedgerApi.RecordField("issuer", Some(LedgerApi.Value().withParty(party))))
          )
        ),
      )
    "1 create" in {
      for {
        adminClient <- ledgerClient(config =
          Some(
            ledgerClientConfiguration.copy(
              token = Some(toHeader(forApplicationId("custom app id", adminToken)))
            )
          )
        )
        _ <- adminClient.partyManagementClient.allocateParty(Some(party), None)
        client <- ledgerClient()
        runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
        (acs, offset) <- runner.queryACS()
        // Start the future here
        finalStateF = runner
          .runWithACS(acs, offset, msgFlow = Flow[TriggerContext[TriggerMsg]].take(6))
          ._2
        // Execute commands
        _ <- create(client, party, asset(party))
        // Wait for the trigger to terminate
        _ <- finalStateF
        acs <- queryACS(client, party)
      } yield {
        acs(assetId) should have size 1
        acs(assetMirrorId) should have size 1
      }
    }
  }
}
