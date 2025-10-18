// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.javaapi.data.*
import com.daml.ledger.javaapi.data.codegen.HasCommands
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.damltests.java.explicitdisclosure.PriceQuotation
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.ContractNotFound
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.util.Collections

sealed trait OfflinePartyReplicationExplicitDisclosureIntegrationTest
    extends OfflinePartyReplicationIntegrationTestBase {

  override lazy val environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*
        participants.all.dars.upload(CantonTestsPath)

        source = participant1
        target = participant2
      }

  "Explicit disclosure should work on replicated contracts" in { implicit env =>
    import env.*

    import scala.language.implicitConversions
    implicit def hasCommandToCommands(hasCommands: HasCommands): Seq[Command] = {
      import scala.jdk.CollectionConverters.IteratorHasAsScala
      hasCommands.commands.iterator.asScala.toSeq
    }

    // Create a contract visible only to `alice`
    val (quote, disclosedQuote) = {
      val quote = new PriceQuotation(alice.toProtoPrimitive, "DAML", 6865)
      participant1.ledger_api.javaapi.commands.submit(
        actAs = Seq(alice),
        commands = quote.create,
      )
      val tx = participant1.ledger_api.javaapi.updates
        .transactions_with_tx_format(
          transactionFormat = transactionFormat(alice -> PriceQuotation.TEMPLATE_ID),
          completeAfter = 1,
        )
        .loneElement
      val flatTx = tx.getTransaction.get
      val creation = JavaDecodeUtil.flatToCreated(flatTx).loneElement
      val contract = JavaDecodeUtil.decodeCreated(PriceQuotation.COMPANION)(creation).value
      val disclosedContract = JavaDecodeUtil.decodeDisclosedContracts(flatTx).loneElement
      (contract.id, disclosedContract)
    }

    val beforeActivationOffset = authorizeAliceWithTargetDisconnect(daId, source, target)

    // Replicate `alice` from `source` (`participant1`) to `target` (`participant2`)
    source.parties.export_party_acs(
      alice,
      daId,
      target,
      beforeActivationOffset,
      acsSnapshotPath,
    )
    target.parties.import_party_acs(acsSnapshotPath)

    val beforeTargetReconnectOffset = target.ledger_api.state.end()

    target.synchronizers.reconnect_all()

    eventually(timeUntilSuccess = 2.minutes, maxPollInterval = 30.seconds) {
      val (onboard, earliestRetryTimestamp) =
        target.parties.clear_party_onboarding_flag(alice, daId, beforeTargetReconnectOffset)
      (onboard, earliestRetryTimestamp) shouldBe (true, None)
    }

    // Verify that `alice` can see the contract with explicit disclosure
    target.ledger_api.javaapi.commands.submit(
      actAs = Seq(alice),
      commands = quote.exercisePriceQuotation_Fetch(alice.toProtoPrimitive),
      disclosedContracts = Seq(disclosedQuote),
    )

    // Verify that `bob` can't see the contract without explicit disclosure
    assertThrowsAndLogsCommandFailures(
      target.ledger_api.javaapi.commands.submit(
        actAs = Seq(bob),
        commands = quote.exercisePriceQuotation_Fetch(bob.toProtoPrimitive),
      ),
      _.shouldBeCantonErrorCode(ContractNotFound),
    )

    // Verify that `bob` can see the contract with explicit disclosure
    target.ledger_api.javaapi.commands.submit(
      actAs = Seq(bob),
      commands = quote.exercisePriceQuotation_Fetch(bob.toProtoPrimitive),
      disclosedContracts = Seq(disclosedQuote),
    )
  }

  private def transactionFormat(f: (PartyId, Identifier)): TransactionFormat = {
    import scala.jdk.CollectionConverters.MapHasAsJava
    import scala.jdk.OptionConverters.RichOption
    val (party, templateId) = f
    new TransactionFormat(
      new EventFormat(
        Map(
          party.toProtoPrimitive -> (new CumulativeFilter(
            Collections.emptyMap[Identifier, Filter.Interface](),
            Map(templateId -> Filter.Template.INCLUDE_CREATED_EVENT_BLOB).asJava,
            None.toJava,
          ): Filter)
        ).asJava,
        None.toJava,
        false,
      ),
      TransactionShape.ACS_DELTA,
    )
  }

}

final class OfflinePartyReplicationExplicitDisclosureIntegrationTestPostgres
    extends OfflinePartyReplicationExplicitDisclosureIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
