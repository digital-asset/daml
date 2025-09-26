// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.api.v2.ValueOuterClass
import com.daml.ledger.api.v2.event.CreatedEvent
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  TransactionFormat,
}
import com.daml.ledger.api.v2.value.Identifier as ScalaPbIdentifier
import com.daml.ledger.api.v2.value.Identifier.toJavaProto
import com.daml.ledger.javaapi.data.CreatedEvent as JavaCreatedEvent
import com.daml.ledger.javaapi.data.codegen.{Contract, ContractCompanion}
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.DbConfig.Postgres
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.Upgrading as UpgradingV1
import com.digitalasset.canton.damltests.upgrade.v2.java.upgrade.Upgrading as UpgradingV2
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.platform.apiserver.SeedService
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.daml.lf.transaction.TransactionCoder
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.Assertion

import scala.concurrent.Future
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}
import scala.jdk.OptionConverters.RichOption

//TODO(#16651): Consider extending the conformance test suite with package-name scoping tests for stream subscriptions
abstract class LedgerApiDynamicTemplateFilterIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {
  private val Upgrading_Identifier = ScalaPbIdentifier
    .fromJavaProto(UpgradingV1.TEMPLATE_ID.toProto)

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup(setup)

  protected def setup(env: TestConsoleEnvironment): Unit = {
    import env.*
    participant1.synchronizers.connect_local(sequencer1, alias = daName)

    // Upload Upgrade V1
    participant1.dars.upload(UpgradingBaseTest.UpgradeV1)
  }

  "The Ledger API" when {
    "a template is upgraded and a new contract with it is created during an ongoing subscription" should {
      "retrieve the contracts from the upgraded template as well" in { implicit env =>
        import env.*

        val alice =
          participant1.parties.enable("alice")
        val aliceP = alice.toProtoPrimitive

        // Start ongoing subscriptions for Upgrade package name
        val upgradingSubscriptions_blob = new Subscriptions(
          participant = participant1,
          party = alice,
          filterIdentifier = Upgrading_Identifier,
          includeCreatedEventBlob = true,
          expectedCreatesSize = 2,
        )

        // Create an Upgrading V1 contract
        val payload_UpgradingV1 = new UpgradingV1(aliceP, aliceP, 1L)
        participant1.ledger_api.javaapi.commands
          .submit(Seq(alice), payload_UpgradingV1.create().commands().asScala.toSeq)

        // Upload Upgrade V2
        participant1.dars.upload(UpgradingBaseTest.UpgradeV2)

        // Create an Upgrading V2 contract
        val payload_UpgradingV2 =
          new UpgradingV2(aliceP, aliceP, 2L, Some(Seq("more").asJava).toJava)
        participant1.ledger_api.javaapi.commands
          .submit(Seq(alice), payload_UpgradingV2.create().commands().asScala.toSeq)

        upgradingSubscriptions_blob.creates match {
          case Seq(c1, c2) =>
            assertCreate[UpgradingV1.Contract, UpgradingV1.ContractId, UpgradingV1](
              create = c1,
              companion = UpgradingV1.COMPANION,
              expectedPayload = payload_UpgradingV1,
              expectedIdentifier = UpgradingV1.TEMPLATE_ID_WITH_PACKAGE_ID.toProto,
            )
            assertCreate[UpgradingV2.Contract, UpgradingV2.ContractId, UpgradingV2](
              create = c2,
              companion = UpgradingV2.COMPANION,
              expectedPayload = payload_UpgradingV2,
              expectedIdentifier = UpgradingV2.TEMPLATE_ID_WITH_PACKAGE_ID.toProto,
            )
          case other => fail(s"Expected two create events, got ${other.size}")
        }
      }
    }
  }

  private def assertCreate[Ct <: Contract[Id, Data], Id, Data](
      create: CreatedEvent,
      companion: ContractCompanion.WithoutKey[Ct, Id, Data],
      expectedPayload: Data,
      expectedIdentifier: ValueOuterClass.Identifier,
  ): Assertion = {
    toJavaProto(create.templateId.value) shouldBe expectedIdentifier
    create.createdEventBlob should not be empty
    TransactionCoder
      .decodeFatContractInstance(create.createdEventBlob)
      .value
      .packageName shouldBe UpgradingV1.PACKAGE_NAME
    create.packageName shouldBe UpgradingV1.PACKAGE_NAME

    JavaDecodeUtil
      .decodeCreated(companion)(JavaCreatedEvent.fromProto(CreatedEvent.toJavaProto(create)))
      .value
      .data shouldBe expectedPayload
  }

  private class Subscriptions(
      participant: LocalParticipantReference,
      party: PartyId,
      filterIdentifier: ScalaPbIdentifier,
      includeCreatedEventBlob: Boolean,
      expectedCreatesSize: Int,
  )(implicit env: FixtureParam) {
    import env.*

    private val flatTxsF = Future {
      participant.ledger_api.updates.transactions_with_tx_format(
        transactionFormat = transactionFormat(
          filterIdentifier,
          party.toProtoPrimitive,
          includeCreatedEventBlob = includeCreatedEventBlob,
        ),
        completeAfter = expectedCreatesSize,
      )
    }

    def creates: Seq[CreatedEvent] = {
      val updateCreates = flatTxsF.map(_.flatMap(_.createEvents)).futureValue

      val acsCreates = participant.ledger_api.state.acs
        .of_party(
          party,
          filterTemplates = Seq(TemplateId.fromIdentifier(Upgrading_Identifier)),
          includeCreatedEventBlob = includeCreatedEventBlob,
        )
        .map(_.event)

      acsCreates should have size expectedCreatesSize.toLong
      updateCreates should have size expectedCreatesSize.toLong

      acsCreates zip updateCreates foreach { case (acsCreate, updateCreate) =>
        updateCreate shouldBe acsCreate
      }

      acsCreates
    }
  }

  private def transactionFormat(
      identifier: ScalaPbIdentifier,
      party: String,
      includeCreatedEventBlob: Boolean,
  ) =
    TransactionFormat(
      eventFormat = Some(
        EventFormat(
          filtersByParty = Map(
            party -> Filters(
              Seq(
                CumulativeFilter(
                  IdentifierFilter.TemplateFilter(
                    TemplateFilter(
                      templateId = Some(identifier),
                      includeCreatedEventBlob = includeCreatedEventBlob,
                    )
                  )
                )
              )
            )
          ),
          filtersForAnyParty = None,
          verbose = true,
        )
      ),
      transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
    )
}

final class ReferenceLedgerApiDynamicTemplateFilterIntegrationTestPostgres
    extends LedgerApiDynamicTemplateFilterIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[Postgres](loggerFactory))
}

//TODO(#16651): Remove this test once package-name test coverage is ensured by conformance tests
final class ReferenceLedgerApiDynamicTemplateFilterIntegrationTestNoCaches
    extends LedgerApiDynamicTemplateFilterIntegrationTest {
  registerPlugin(new UseReferenceBlockSequencer[Postgres](loggerFactory))

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.updateParticipantConfig("participant1") {
        (c: ParticipantNodeConfig) =>
          c.focus(_.ledgerApi.userManagementService.enabled)
            .replace(true)
            .focus(_.ledgerApi.userManagementService.maxCacheSize)
            .replace(0)
            .focus(_.ledgerApi.userManagementService.maxRightsPerUser)
            .replace(100)
            .focus(_.parameters.ledgerApiServer.contractIdSeeding)
            .replace(SeedService.Seeding.Weak)
            .focus(_.ledgerApi.indexService.maxContractKeyStateCacheSize)
            .replace(0)
            .focus(_.ledgerApi.indexService.maxContractStateCacheSize)
            .replace(0)
            .focus(_.ledgerApi.indexService.maxTransactionsInMemoryFanOutBufferSize)
            .replace(0)
      })
      .withSetup(setup)
}
