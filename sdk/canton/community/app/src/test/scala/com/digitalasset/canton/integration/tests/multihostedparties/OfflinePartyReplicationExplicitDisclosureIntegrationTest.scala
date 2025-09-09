// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

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
import com.digitalasset.canton.HasTempDirectory
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.damltests.java.explicitdisclosure.PriceQuotation
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.ledger.error.groups.ConsistencyErrors.ContractNotFound
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP

import java.util.Collections

sealed trait OfflinePartyReplicationExplicitDisclosureIntegrationTest
    extends UseSilentSynchronizerInTest
    with HasTempDirectory {

  private val acsSnapshot = tempDirectory.toTempFile(s"${getClass.getSimpleName}.gz")
  private val acsSnapshotPath: String = acsSnapshot.toString

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // Party replication to the target participant may trigger ACS commitment mismatch warnings.
  // This is expected behavior. To reduce the frequency of these warnings and avoid associated
  // test flakes, `reconciliationInterval` is set to one year.
  private val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  private var alice: PartyId = _
  private var bob: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.useStaticTime)
      .withSetup { implicit env =>
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

        sequencers.all.foreach { s =>
          adjustTimeouts(s)
          s.topology.synchronizer_parameters
            .propose_update(
              daId,
              _.update(reconciliationInterval = reconciliationInterval.toConfig),
            )
        }
      }

  "Explicit disclosure should work on replicated contracts" in { implicit env =>
    import env.*

    import scala.language.implicitConversions
    implicit def hasCommandToCommands(hasCommands: HasCommands): Seq[Command] = {
      import scala.jdk.CollectionConverters.IteratorHasAsScala
      hasCommands.commands.iterator.asScala.toSeq
    }

    val simClock = Some(env.environment.simClock.value)

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

    val beforeActivationOffset = participant1.ledger_api.state.end()

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
      participant1,
      alice,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant2, PP.Observation),
      ),
    )

    silenceSynchronizerAndAwaitEffectiveness(daId, sequencer1, participant1, simClock)

    // Replicate `alice` from `participant1` to `participant2`
    repair.party_replication.step1_hold_and_store_acs(
      alice,
      daId,
      participant1,
      participant2.id,
      acsSnapshotPath,
      beforeActivationOffset,
    )
    repair.party_replication.step2_import_acs(alice, daId, participant2, acsSnapshotPath)

    resumeSynchronizerAndAwaitEffectiveness(daId, sequencer1, participant2, simClock)

    PartyToParticipantDeclarative.forParty(Set(participant1, participant2), daId)(
      participant1,
      alice,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant2, PP.Submission),
      ),
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

final class OfflinePartyReplicationExplicitDisclosureIntegrationTestPostgres
    extends OfflinePartyReplicationExplicitDisclosureIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))
}
