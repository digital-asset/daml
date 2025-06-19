// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import better.files.File
import com.daml.ledger.javaapi.data.codegen.HasCommands
import com.daml.ledger.javaapi.data.{
  Command,
  CumulativeFilter,
  EventFormat,
  Filter,
  Identifier,
  TransactionFormat,
  TransactionShape,
}
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.damltests.java.explicitdisclosure.PriceQuotation
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.ContractNotFound
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP

import java.util.Collections

sealed trait OfflinePartyReplicationExplicitDisclosureIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private val acsFilename: String = s"${getClass.getSimpleName}.gz"

  private var alice: PartyId = _
  private var bob: PartyId = _

  private val mediatorReactionTimeout = config.NonNegativeFiniteDuration.ofSeconds(1)
  private val confirmationResponseTimeout = config.NonNegativeFiniteDuration.ofSeconds(1)
  private val waitTimeMs =
    (mediatorReactionTimeout + confirmationResponseTimeout + config.NonNegativeFiniteDuration
      .ofMillis(500)).duration.toMillis

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

      sequencers.all.foreach(
        _.topology.synchronizer_parameters.propose_update(
          synchronizerId = daId,
          _.update(
            mediatorReactionTimeout = mediatorReactionTimeout,
            confirmationResponseTimeout = confirmationResponseTimeout,
          ),
        )
      )
    }

  override def afterAll(): Unit =
    try {
      val acsExport = File(acsFilename)
      if (acsExport.exists) {
        acsExport.delete()
      }
    } finally super.afterAll()

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

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
      participant1,
      alice,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant2, PP.Submission),
      ),
    )

    val onboardingTx = participant1.topology.party_to_participant_mappings
      .list(
        synchronizerId = daId,
        filterParty = alice.filterString,
        filterParticipant = participant2.filterString,
      )
      .loneElement
      .context

    sequencer1.topology.synchronizer_parameters.propose_update(
      sequencer1.synchronizer_id,
      _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(0)),
    )

    eventually() {
      val confirmationRequestsMaxRate = participant1.topology.synchronizer_parameters
        .list(store = daId)
        .map { change =>
          change.item.participantSynchronizerLimits.confirmationRequestsMaxRate
        }
      confirmationRequestsMaxRate.head.unwrap shouldBe 0
    }

    Threading.sleep(waitTimeMs)

    // Replicate `alice` from `participant1` to `participant2`
    repair.party_replication.step1_hold_and_store_acs(
      alice,
      daId,
      participant1,
      participant2.id,
      acsFilename,
      onboardingTx.validFrom,
    )
    repair.party_replication.step2_import_acs(alice, daId, participant2, acsFilename)

    sequencer1.topology.synchronizer_parameters.propose_update(
      sequencer1.synchronizer_id,
      _.update(confirmationRequestsMaxRate = NonNegativeInt.tryCreate(10000)),
    )

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

// TODO(#25931) – Make this test non-flaky
//final class OfflinePartyReplicationExplicitDisclosureIntegrationTestPostgres
//    extends OfflinePartyReplicationExplicitDisclosureIntegrationTest {
//  registerPlugin(new UsePostgres(loggerFactory))
//  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
//}
