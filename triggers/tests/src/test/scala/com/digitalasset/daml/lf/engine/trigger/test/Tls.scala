// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import akka.stream.scaladsl.{Flow}
import com.daml.bazeltools.BazelRunfiles._
import com.daml.lf.data.Ref._
import com.daml.ledger.api.testing.utils.{SuiteResourceManagementAroundAll}
import com.daml.ledger.api.tls.TlsConfiguration
import com.daml.ledger.api.v1.commands._
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.{value => LedgerApi}
import java.io.File
import org.scalatest._

import com.daml.lf.engine.trigger.TriggerMsg

class Tls
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with SuiteResourceManagementAroundAll
    with TryValues {
  self: Suite =>

  val List(serverCrt, serverPem, caCrt, clientCrt, clientPem) = {
    List("server.crt", "server.pem", "ca.crt", "client.crt", "client.pem").map { src =>
      Some(new File(rlocation("ledger/test-common/test-certificates/" + src)))
    }
  }

  override protected def config =
    super.config
      .copy(tlsConfig = Some(TlsConfiguration(enabled = true, serverCrt, serverPem, caCrt)))

  override protected def ledgerClientConfiguration =
    super.ledgerClientConfiguration
      .copy(sslContext = TlsConfiguration(enabled = true, clientCrt, clientPem, caCrt).client)

  "TLS" can {
    // We just need something simple to test the connection.
    val assetId = LedgerApi.Identifier(packageId, "ACS", "Asset")
    val assetMirrorId = LedgerApi.Identifier(packageId, "ACS", "AssetMirror")
    def asset(party: String): CreateCommand =
      CreateCommand(
        templateId = Some(assetId),
        createArguments = Some(LedgerApi.Record(
          fields = Seq(LedgerApi.RecordField("issuer", Some(LedgerApi.Value().withParty(party)))))))
    "1 create" in {
      for {
        client <- ledgerClient()
        party <- allocateParty(client)
        runner = getRunner(client, QualifiedName.assertFromString("ACS:test"), party)
        (acs, offset) <- runner.queryACS()
        // Start the future here
        finalStateF = runner.runWithACS(acs, offset, msgFlow = Flow[TriggerMsg].take(6))._2
        // Execute commands
        contractId <- create(client, party, asset(party))
        // Wait for the trigger to terminate
        _ <- finalStateF
        acs <- queryACS(client, party)
      } yield {
        assert(acs(assetId).size == 1)
        assert(acs(assetMirrorId).size == 1)
      }
    }
  }
}
