// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.tests.integration.ledger.api

import java.util.UUID

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.ledger.api.testing.utils.{
  AkkaBeforeAndAfterAll,
  SuiteResourceManagementAroundAll
}
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
import com.digitalasset.ledger.client.services.admin.PartyManagementClient
import com.digitalasset.platform.apitesting.MultiLedgerFixture
import com.digitalasset.platform.esf.TestExecutionSequencerFactory
import org.scalatest._
import org.scalatest.concurrent.AsyncTimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._
import scalaz.syntax.tag._

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Option2Iterable",
    "org.wartremover.warts.StringPlusAny"
  ))
class PartyManagementServiceIT
    extends AsyncWordSpec
    with AkkaBeforeAndAfterAll
    with MultiLedgerFixture
    with SuiteResourceManagementAroundAll
    with AsyncTimeLimitedTests
    with TestExecutionSequencerFactory
    with Matchers
    with Inside
    with OptionValues {

  override def timeLimit: Span = 15.seconds

  override protected def config: Config = Config.default

  private def partyManagementClient(stub: PartyManagementService): PartyManagementClient = {
    new PartyManagementClient(stub)
  }

  "Party Management Service" when {

    "returning the participant ID" should {
      "succeed" in allFixtures { c =>
        partyManagementClient(c.partyManagementService)
          .getParticipantId()
          .map(id => id.unwrap.isEmpty shouldBe false)

      }
    }

    "allocating parties" should {
      "list the new party" in allFixtures { c =>
        val client = partyManagementClient(c.partyManagementService)
        val hint = Ref.Party.assertFromString(s"party-${UUID.randomUUID().toString}")

        // Note: The ledger has some freedom in how to implement the allocateParty() call.
        // In particular, it may require that the given hint is a valid party name and may reject
        // the call if such a party already exists. This test therefore generates a unique hint.
        for {
          initialParties <- client.listKnownParties()
          result <- client.allocateParty(
            Some(hint),
            Some(s"Test party '$hint' for PartyManagementServiceIT"))
          finalParties <- client.listKnownParties()
        } yield {
          initialParties.exists(p => p.party == result.party) shouldBe false
          finalParties.contains(result) shouldBe true
        }

      }
    }

  }
}
