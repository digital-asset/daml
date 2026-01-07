// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.damltests.java.test.Dummy
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*

trait LedgerApiJavaCodegenIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  var alice: PartyId = _
  var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonTestsPath)
        alice = participant1.parties.enable("alice")
        bob = participant1.parties.enable("bob")

        eventually() {
          // wait until
          participant1.parties
            .list(asOf = Some(environment.clock.now.toInstant))
            .map(_.party) should contain allElementsOf (Seq(alice, bob))
        }
      }

  "java codegen integration commands are supported" in { implicit env =>
    import env.*
    // submit ledger effects
    val offset = participant1.ledger_api.state.end()
    val ledgerEffectsTx = participant1.ledger_api.javaapi.commands.submit(
      actAs = Seq(alice),
      commands = new Dummy(alice.toProtoPrimitive).create.commands.asScala.toSeq,
      transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
    )
    // ledger effects
    val ledgerEffectsTxs = participant1.ledger_api.javaapi.updates
      .transactions(
        partyIds = Set(alice),
        completeAfter = PositiveInt.one,
        beginOffsetExclusive = offset,
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )
      .map(_.getTransaction.get())
    ledgerEffectsTxs shouldBe Seq(ledgerEffectsTx)
    // submit acs delta
    val acsDeltaTx = participant1.ledger_api.javaapi.commands.submit(
      actAs = Seq(bob),
      commands = new Dummy(bob.toProtoPrimitive).create.commands.asScala.toSeq,
    )
    // acs delta
    val acsDeltaTxs = participant1.ledger_api.javaapi.updates
      .transactions(
        partyIds = Set(bob),
        completeAfter = 1,
        beginOffsetExclusive = offset,
      )
      .map(_.getTransaction.get())
    acsDeltaTxs shouldBe Seq(acsDeltaTx)
    // await
    val dummyContract =
      participant1.ledger_api.javaapi.state.acs.await(Dummy.COMPANION)(alice)
    dummyContract.data shouldBe new Dummy(alice.toProtoPrimitive)
    // filter
    val dummyContracts =
      participant1.ledger_api.javaapi.state.acs.filter(Dummy.COMPANION)(alice)
    dummyContracts shouldBe Seq(dummyContract)
  }
}

class LedgerApiJavaCodegenIntegrationTestDefault extends LedgerApiJavaCodegenIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
