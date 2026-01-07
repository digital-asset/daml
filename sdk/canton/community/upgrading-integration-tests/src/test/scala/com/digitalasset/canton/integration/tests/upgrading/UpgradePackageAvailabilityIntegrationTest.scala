// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.value.Identifier.toJavaProto
import com.daml.ledger.javaapi.data.DisclosedContract
import com.digitalasset.canton.admin.api.client.data.TemplateId.fromJavaIdentifier
import com.digitalasset.canton.damltests.upgrade.v1.java as v1
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.Quote
import com.digitalasset.canton.damltests.upgrade.v2.java as v2
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.daml.lf.data.Ref

import java.util.Optional
import scala.jdk.CollectionConverters.*

/** Primary concern is ensuring that correct packages are vetted and used when upgrading
  */
sealed abstract class UpgradePackageAvailabilityIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var alice: PartyId = _
  private var bob: PartyId = _
  private var charlie: PartyId = _
  private var dan: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P4_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)

      alice = participant1.parties.enable("alice")

      bob = participant2.parties.enable("bob")

      charlie = participant3.parties.enable("charlie")

      dan = participant4.parties.enable("dan")

      // Participant 1 (alice) has V1 and V2 loaded
      participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
      participant1.dars.upload(UpgradingBaseTest.UpgradeV2)

      // Participant 2 (bob) also has V1 and V2 loaded
      participant2.dars.upload(UpgradingBaseTest.UpgradeV1)
      participant2.dars.upload(UpgradingBaseTest.UpgradeV2)

      // Participant 3 (charlie) has loaded V1 and V2 loaded but has then unvetted V1
      participant3.dars.upload(UpgradingBaseTest.UpgradeV1)
      participant3.dars.upload(UpgradingBaseTest.UpgradeV2)
      participant3.topology.vetted_packages.propose_delta(
        participant3,
        removes = Seq(Ref.PackageId.assertFromString(v1.upgrade.Quote.PACKAGE_ID)),
      )

      // Participant 4 (dan) has only ever had V2 loaded
      participant4.dars.upload(UpgradingBaseTest.UpgradeV2)
    }

  private def discloseQuote(
      value: Long,
      observer: Option[PartyId] = None,
  )(implicit env: FixtureParam): (v2.upgrade.Quote.ContractId, DisclosedContract) = {

    import env.*

    val quoteV1Cid = JavaDecodeUtil
      .decodeAllCreated(v1.upgrade.Quote.COMPANION)(
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new Quote(
            Seq(alice.toProtoPrimitive).asJava,
            observer.map(_.toProtoPrimitive).toList.asJava,
            "S1",
            value,
          ).create.commands.overridePackageId(v1.upgrade.Quote.PACKAGE_ID).asScala.toSeq,
        )
      )
      .loneElement
      .id

    val disclosedQuote = {
      val createdEvent = eventually() {
        participant1.ledger_api.state.acs
          .active_contracts_of_party(
            alice,
            filterTemplates = Seq(fromJavaIdentifier(v1.upgrade.Quote.COMPANION.TEMPLATE_ID)),
            includeCreatedEventBlob = true,
          )
          .flatMap(_.createdEvent)
          .filter(_.contractId == quoteV1Cid.contractId)
          .loneElement
      }

      val synchronizerId = participant1.ledger_api.javaapi.event_query
        .by_contract_id(quoteV1Cid.contractId, Seq(alice))
        .getCreated
        .getSynchronizerId

      new com.daml.ledger.javaapi.data.DisclosedContract(
        createdEvent.createdEventBlob,
        synchronizerId,
        Optional.of(
          com.daml.ledger.javaapi.data.Identifier
            .fromProto(toJavaProto(createdEvent.templateId.value))
        ),
        Optional.of(createdEvent.contractId),
      )
    }

    val quoteV2Cid: v2.upgrade.Quote.ContractId =
      new v2.upgrade.Quote.ContractId(quoteV1Cid.contractId)

    (quoteV2Cid, disclosedQuote)
  }

  "Upgrading" when {

    "Use upgraded package for views that have a fetch action description" in { implicit env =>
      import env.*

      val (quoteCid, _) = discloseQuote(99, observer = Some(bob))

      val fetchQuoteCid = JavaDecodeUtil
        .decodeAllCreated(v2.upgrade.FetchQuote.COMPANION)(
          participant2.ledger_api.javaapi.commands.submit(
            Seq(bob),
            new v2.upgrade.FetchQuote(
              bob.toProtoPrimitive,
              bob.toProtoPrimitive,
              bob.toProtoPrimitive,
            ).create.commands.overridePackageId(v2.upgrade.FetchQuote.PACKAGE_ID).asScala.toSeq,
          )
        )
        .loneElement
        .id

      // When Bob selects the quote Alice should find out about the fetch, but not the exercise
      participant2.ledger_api.javaapi.commands.submit(
        Seq(bob),
        fetchQuoteCid
          .exerciseFQ_RawFetch(quoteCid)
          .commands
          .overridePackageId(v2.upgrade.FetchQuote.PACKAGE_ID)
          .asScala
          .toSeq,
      )

      // Fetch events are not observable via the ledger API but if the V1 package id was used
      // a model conformance error would be produced on view reconstruction.

      succeed
    }

    // TODO(#23876) - remove ignore
    "Not require that the V1 package is vetted in order to be able to confirm V2 package use" ignore {
      implicit env =>
        import env.*

        val (quoteCid, disclosedQuote) = discloseQuote(100)

        val fetchQuoteCid = JavaDecodeUtil
          .decodeAllCreated(v2.upgrade.FetchQuote.COMPANION)(
            participant3.ledger_api.javaapi.commands.submit(
              Seq(charlie),
              new v2.upgrade.FetchQuote(
                charlie.toProtoPrimitive,
                charlie.toProtoPrimitive,
                charlie.toProtoPrimitive,
              ).create.commands.overridePackageId(v2.upgrade.FetchQuote.PACKAGE_ID).asScala.toSeq,
            )
          )
          .loneElement
          .id

        participant3.ledger_api.javaapi.commands.submit(
          Seq(charlie),
          fetchQuoteCid
            .exerciseFQ_ExFetch(quoteCid)
            .commands
            .overridePackageId(v2.upgrade.FetchQuote.PACKAGE_ID)
            .asScala
            .toSeq,
          disclosedContracts = Seq(disclosedQuote),
        )

        succeed
    }

    // TODO(#23876) - remove ignore
    "Not require the V1 package to submit a V1 contract used in a V2 context" ignore {
      implicit env =>
        import env.*
        val (quoteCid, disclosedQuote) = discloseQuote(101)

        val fetchQuoteCid = JavaDecodeUtil
          .decodeAllCreated(v2.upgrade.FetchQuote.COMPANION)(
            participant4.ledger_api.javaapi.commands.submit(
              Seq(dan),
              new v2.upgrade.FetchQuote(
                dan.toProtoPrimitive,
                dan.toProtoPrimitive,
                dan.toProtoPrimitive,
              ).create.commands.asScala.toSeq,
            )
          )
          .loneElement
          .id

        // Submission will fail if V1 package is needed
        participant4.ledger_api.javaapi.commands.submit(
          Seq(dan),
          fetchQuoteCid.exerciseFQ_ExFetch(quoteCid).commands.asScala.toSeq,
          disclosedContracts = Seq(disclosedQuote),
        )

        succeed
    }

    // TODO(#23876) - remove ignore
    "Not require the V1 package to observe the use of a disclosed V1 contract used in a V2 context" ignore {
      implicit env =>
        import env.*
        val (quoteCid, disclosedQuote) = discloseQuote(102)

        val fetchQuoteCid = JavaDecodeUtil
          .decodeAllCreated(v2.upgrade.FetchQuote.COMPANION)(
            participant1.ledger_api.javaapi.commands.submit(
              Seq(alice),
              new v2.upgrade.FetchQuote(
                alice.toProtoPrimitive,
                alice.toProtoPrimitive,
                dan.toProtoPrimitive,
              ).create.commands.asScala.toSeq,
            )
          )
          .loneElement
          .id

        // When the observer (dan) attempts to validate the confirmed transaction
        // it will fail if the V1 package is required.
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          fetchQuoteCid.exerciseFQ_ConFetch(quoteCid).commands.asScala.toSeq,
          disclosedContracts = Seq(disclosedQuote),
        )

        succeed
    }

    // TODO(#23876) - remove ignore
    "Not require the V1 package to confirm the use of a disclosed V1 contract used in a V2 context" ignore {
      implicit env =>
        import env.*
        val (quoteCid, disclosedQuote) = discloseQuote(102)

        val aliceFetchQuoteCid = JavaDecodeUtil
          .decodeAllCreated(v2.upgrade.FetchQuote.COMPANION)(
            participant1.ledger_api.javaapi.commands.submit(
              Seq(alice),
              new v2.upgrade.FetchQuote(
                alice.toProtoPrimitive,
                alice.toProtoPrimitive,
                dan.toProtoPrimitive,
              ).create.commands.asScala.toSeq,
            )
          )
          .loneElement
          .id

        val aliceBobFetchQuoteCid = JavaDecodeUtil
          .decodeAllCreated(v2.upgrade.FetchQuote.COMPANION)(
            participant4.ledger_api.javaapi.commands.submit(
              Seq(dan),
              aliceFetchQuoteCid.exerciseFQ_ObserverConfirms().commands.asScala.toSeq,
            )
          )
          .loneElement
          .id

        // When confirmer dan
        // it will fail if the V1 package is required.
        participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          aliceBobFetchQuoteCid.exerciseFQ_ConFetch(quoteCid).commands.asScala.toSeq,
          disclosedContracts = Seq(disclosedQuote),
        )

        succeed
    }

  }

}

final class BftOrderingUpgradePackageAvailabilityIntegrationTestPostgres
    extends UpgradePackageAvailabilityIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
