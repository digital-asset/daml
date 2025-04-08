// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.daml.ledger.javaapi.data.codegen.HasCommands
import com.daml.ledger.javaapi.data.{
  Command,
  CumulativeFilter,
  Filter,
  Identifier,
  TransactionFilter,
}
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.damltests.java.explicitdisclosure.PriceQuotation
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.ContractNotFound
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId

import java.util.Collections

sealed trait OfflinePartyMigrationExplicitDisclosureIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val acsFilename: String = s"${getClass.getSimpleName}.gz"

  private var alice: PartyId = _
  private var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participants.all.dars.upload(CantonTestsPath)

      alice = participant1.parties.enable(
        "Alice",
        synchronizeParticipants = Seq(participant2),
      )
      bob = participant2.parties.enable(
        "Bob",
        synchronizeParticipants = Seq(participant1),
      )
    }

  override def afterAll(): Unit =
    try {
      val acsExport = File(acsFilename)
      if (acsExport.exists) {
        acsExport.delete()
      }
    } finally super.afterAll()

  "Explicit disclosure should work on migrated contracts" in { implicit env =>
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
        .flat_with_tx_filter(
          filter = filter(alice -> PriceQuotation.TEMPLATE_ID),
          completeAfter = 1,
        )
        .loneElement
      val flatTx = tx.getTransaction.get
      val creation = JavaDecodeUtil.flatToCreated(flatTx).loneElement
      val contract = JavaDecodeUtil.decodeCreated(PriceQuotation.COMPANION)(creation).value
      val disclosedContract = JavaDecodeUtil.decodeDisclosedContracts(flatTx).loneElement
      (contract.id, disclosedContract)
    }

    // Migrate `alice` from `participant1` to `participant2`
    repair.party_migration.step1_hold_and_store_acs(
      alice,
      partiesOffboarding = true,
      participant1,
      participant2.id,
      acsFilename,
    )
    repair.party_migration.step2_import_acs(alice, participant2, acsFilename)
    participant1.synchronizers.disconnect_all()
    repair.party_migration.step4_clean_up_source(alice, participant1, acsFilename)
    participant1.synchronizers.reconnect_all()

    // Verify that `alice` can see the contract with explicit disclosure
    participant2.ledger_api.javaapi.commands.submit(
      actAs = Seq(alice),
      commands = quote.exercisePriceQuotation_Fetch(alice.toProtoPrimitive),
      disclosedContracts = Seq(disclosedQuote),
    )

    // Verify that `bob` can't see the contract without explicit disclosure
    assertThrowsAndLogsCommandFailures(
      participant2.ledger_api.javaapi.commands.submit(
        actAs = Seq(bob),
        commands = quote.exercisePriceQuotation_Fetch(bob.toProtoPrimitive),
      ),
      _.shouldBeCantonErrorCode(ContractNotFound),
    )

    // Verify that `bob` can see the contract with explicit disclosure
    participant2.ledger_api.javaapi.commands.submit(
      actAs = Seq(bob),
      commands = quote.exercisePriceQuotation_Fetch(bob.toProtoPrimitive),
      disclosedContracts = Seq(disclosedQuote),
    )
  }

  private def filter(f: (PartyId, Identifier)): TransactionFilter = {
    import scala.jdk.CollectionConverters.MapHasAsJava
    import scala.jdk.OptionConverters.RichOption
    val (party, templateId) = f
    new TransactionFilter(
      Map(
        party.toProtoPrimitive -> (new CumulativeFilter(
          Collections.emptyMap[Identifier, Filter.Interface](),
          Map(templateId -> Filter.Template.INCLUDE_CREATED_EVENT_BLOB).asJava,
          None.toJava,
        ): Filter)
      ).asJava,
      None.toJava,
    )
  }

}

final class OfflinePartyMigrationExplicitDisclosureIntegrationTestPostgres
    extends OfflinePartyMigrationExplicitDisclosureIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
