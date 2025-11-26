// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.upgrade
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.{FetchQuote, Quote}
import com.digitalasset.canton.integration.EnvironmentDefinition
import com.digitalasset.canton.integration.tests.ledgerapi.submission.InteractiveSubmissionIntegrationTestSetup
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.ExternalParty
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.{HasExecutionContext, LfPackageId}
import org.scalatest.OptionValues

import scala.jdk.CollectionConverters.SeqHasAsJava

class InteractiveSubmissionUpgradingTest
    extends InteractiveSubmissionIntegrationTestSetup
    with OptionValues
    with HasExecutionContext {

  private var lse: ExternalParty = _
  private var alice: ExternalParty = _
  private var bob: ExternalParty = _
  private val v1PackageId = upgrade.v1.java.upgrade.Quote.PACKAGE_ID
  private val v2PackageId = upgrade.v2.java.upgrade.Quote.PACKAGE_ID

  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .withSetup { implicit env =>
        import env.*

        participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
        participant1.dars.upload(UpgradingBaseTest.UpgradeV2)
        participant2.dars.upload(UpgradingBaseTest.UpgradeV2)
        participant3.dars.upload(UpgradingBaseTest.UpgradeV1)

        lse = participant1.parties.testing.external.enable("LSE")
        alice = participant2.parties.testing.external.enable("Alice")
        bob = participant3.parties.testing.external.enable("Bob")

      }

  "Interactive submission" should {

    "use a v2 disclosed contract available on all participants" in { implicit env =>
      import env.*
      val (quoteCid, disclosedQuote) = discloseQuote(lse, participant1, v2PackageId)
      val (fetchQuote, _) = createFetchQuote(participant2, alice)
      exerciseFetch(participant2, quoteCid, disclosedQuote, fetchQuote, alice)
    }

    "use a v1 disclosed contract on a participant that only has v2 available" in { implicit env =>
      import env.*
      val (quoteCid, disclosedQuote) = discloseQuote(lse, participant1, v1PackageId)
      val (fetchQuote, _) = createFetchQuote(participant2, alice)
      exerciseFetch(participant2, quoteCid, disclosedQuote, fetchQuote, alice)
    }

    "use a v1 disclosed contract on a participant that only has v1 available" in { implicit env =>
      import env.*
      val (quoteCid, disclosedQuote) = discloseQuote(lse, participant1, v1PackageId)
      val (fetchQuote, disclosedFetch) = createFetchQuote(participant3, bob)

      def setBobConfirmer(
          confirmingParticipant: LocalParticipantReference
      ): Unit =
        PartyToParticipantDeclarative(
          participants = Set(participant1, participant2, participant3),
          synchronizerIds = Set(daId),
        )(
          owningParticipants = Map.empty,
          targetTopology = Map(
            bob -> Map(
              daId -> (PositiveInt.one, Set(
                (confirmingParticipant, ParticipantPermission.Confirmation)
              ))
            )
          ),
        )(executorService, env)

      // Set Bob confirmer to participant2 so that V2 gets used for the prepare step
      setBobConfirmer(participant2)
      val preparedExercise = participant1.ledger_api.javaapi.interactive_submission.prepare(
        Seq(bob.partyId),
        Seq(fetchQuote.id.exerciseFQ_ExFetch(quoteCid).commands().loneElement),
        disclosedContracts = Seq(disclosedQuote, disclosedFetch),
      )

      // Set Bob confirmer to participant3 where V2 is not available
      setBobConfirmer(participant3)
      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.commands.external.submit_prepared(bob, preparedExercise),
        { le =>
          le.errorMessage should include regex raw"(?s)FAILED_PRECONDITION/INVALID_PRESCRIBED_SYNCHRONIZER_ID"
          le.errorMessage should include regex raw"(?s)because: Some packages are not known to all informees.*on synchronizer synchronizer1"
          le.errorMessage should include regex raw"(?s)Participant PAR::participant3.*has not vetted ${v2PackageId
              .take(10)}"
        },
      )

    }

  }

  private def exerciseFetch(
      participant: => LocalParticipantReference,
      quoteCid: Quote.ContractId,
      disclosedQuote: DisclosedContract,
      fetchQuote: FetchQuote.Contract,
      party: ExternalParty,
  ): Transaction = {
    val preparedExercise = participant.ledger_api.javaapi.interactive_submission.prepare(
      Seq(party.partyId),
      Seq(fetchQuote.id.exerciseFQ_ExFetch(quoteCid).commands().loneElement),
      disclosedContracts = Seq(disclosedQuote),
    )
    participant.ledger_api.commands.external.submit_prepared(party, preparedExercise)
  }

  private def createFetchQuote(
      participant1: => LocalParticipantReference,
      party: ExternalParty,
  ): (FetchQuote.Contract, DisclosedContract) = {
    val txFetchQuote = participant1.ledger_api.javaapi.commands.submit(
      Seq(party),
      Seq(
        new FetchQuote(
          party.toProtoPrimitive,
          party.toProtoPrimitive,
          party.toProtoPrimitive,
        ).create.commands.loneElement
      ),
      includeCreatedEventBlob = true,
    )

    val disclosed = JavaDecodeUtil.decodeDisclosedContracts(txFetchQuote).loneElement

    val fetchQuote =
      JavaDecodeUtil.decodeAllCreated(FetchQuote.COMPANION)(txFetchQuote).loneElement
    (fetchQuote, disclosed)
  }

  private def discloseQuote(
      quoter: ExternalParty,
      participant: LocalParticipantReference,
      quotePackageId: LfPackageId,
  ): (Quote.ContractId, DisclosedContract) = {
    val quoteTx = participant.ledger_api.javaapi.commands.submit(
      Seq(quoter),
      Seq(
        new Quote(
          Seq(quoter.toProtoPrimitive).asJava,
          Seq.empty.asJava,
          "VOD",
          100,
        ).create.commands.loneElement
      ),
      includeCreatedEventBlob = true,
      userPackageSelectionPreference = Seq(quotePackageId),
    )

    val disclosedQuote = JavaDecodeUtil.decodeDisclosedContracts(quoteTx).loneElement

    val quoteCid = new Quote.ContractId(disclosedQuote.contractId.get())
    (quoteCid, disclosedQuote)
  }

}
