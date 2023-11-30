// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger.test

import org.apache.pekko.stream.scaladsl.Flow
import com.daml.ledger.api.v1.commands.CreateCommand
import com.daml.ledger.api.v1.{value => LedgerApi}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref._
import com.daml.lf.engine.trigger.Runner.TriggerContext
import com.daml.lf.engine.trigger.TriggerMsg
import com.daml.lf.language.LanguageMajorVersion
import org.scalatest._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class TlsV1 extends Tls(LanguageMajorVersion.V1)
class TlsV2 extends Tls(LanguageMajorVersion.V2)

class Tls(override val majorLanguageVersion: LanguageMajorVersion)
    extends AsyncWordSpec
    with AbstractTriggerTest
    with Matchers
    with TryValues {

  import AbstractTriggerTest._

  final override protected lazy val tlsEnable: Boolean = true

  "TLS" can {
    // We just need something simple to test the connection.
    val assetId = LedgerApi.Identifier(packageId, "ACS", "Asset")
    val assetMirrorId = LedgerApi.Identifier(packageId, "ACS", "AssetMirror")
    def asset(party: Ref.Party): CreateCommand =
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
        client <- defaultLedgerClient()
        party <- allocateParty(client)
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
