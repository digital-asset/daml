// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.repair

import better.files.File
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.ledger.api.v2.{state_service, transaction_filter, value as apiValue}
import com.digitalasset.canton
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.{NonNegativeLong, PositiveInt}
import com.digitalasset.canton.console.{LocalParticipantReference, ParticipantReference}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.http.json.tests.upgrades
import com.digitalasset.canton.http.json.v2.JsContractEntry.JsActiveContract
import com.digitalasset.canton.http.json.v2.JsGetActiveContractsResponse
import com.digitalasset.canton.http.json.v2.JsStateServiceCodecs.{
  getActiveContractsRequestRW,
  jsGetActiveContractsResponseRW,
}
import com.digitalasset.canton.integration.plugins.{UsePostgres, UseReferenceBlockSequencer}
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{
  HasExecutionContext,
  LfPackageId,
  LfPackageName,
  SynchronizerAlias,
  protocol,
}
import com.digitalasset.daml.lf.value.Value.ContractId
import io.circe.Json
import io.circe.syntax.EncoderOps
import monocle.Monocle.toAppliedFocusOps
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.util.ByteString
import org.scalatest.Assertion

import java.util.UUID
import scala.concurrent.Future
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.chaining.scalaUtilChainingOps

abstract class AcsImportRepresentativePackageIdSelectionIntegrationTest
    extends CommunityIntegrationTest
    with HasExecutionContext
    with PekkoBeforeAndAfterAll
    with SharedEnvironment {

  private val FooV1PkgId = upgrades.v1.java.foo.Foo.PACKAGE_ID
  private val FooV2PkgId = upgrades.v2.java.foo.Foo.PACKAGE_ID
  private val FooV3PkgId = upgrades.v3.java.foo.Foo.PACKAGE_ID

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .prependConfigTransforms(ConfigTransforms.enableHttpLedgerApi)
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(FooV1Path)
      }

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseReferenceBlockSequencer[DbConfig.Postgres](loggerFactory))

  protected def importAcs(
      importParticipant: ParticipantReference,
      contractRpIdOverride: Map[canton.protocol.LfContractId, LfPackageId],
      packageIdOverride: Map[LfPackageId, LfPackageId],
      packageNameOverride: Map[LfPackageName, LfPackageId],
      contractImportMode: ContractImportMode,
      file: File,
  ): Unit

  private def createUniqueParty(
      participant: => LocalParticipantReference,
      daName: => SynchronizerAlias,
  ): PartyId =
    participant.parties.enable(s"Alice-${UUID.randomUUID().toString}", synchronizer = Some(daName))

  "Importing an ACS" should {
    // This test case uses participant 2 as import target
    "preserve the original package-id as representative package-id if no override" in {
      implicit env =>
        import env.*

        val party = createUniqueParty(participant1, daName)

        // Upload Foo V1 to P2
        participant2.dars.upload(FooV1Path)

        // Create a contract on P1
        val contractId = createContract(participant1, party)

        // Check the initial rp-id of the contract is the same as the original template-id
        expectRpId(contractId, party, participant1, FooV1PkgId)

        // Export on P1 and import on P2 without any override
        exportAndImportOn(participant2, party)

        // Check the rp-id of contract after import on P2 is unchanged
        expectRpId(contractId, party, participant2, FooV1PkgId)
    }

    // Tests below use participant 3 as import target
    "fail on unknown package-name for the imported contract" in { implicit env =>
      import env.*
      val party = createUniqueParty(participant1, daName)

      val contractId = createContract(participant1, party)

      exportAndImportOn(
        importParticipant = participant3,
        party = party,
        handleImport = importAcs =>
          assertThrowsAndLogsCommandFailures(
            importAcs(),
            entry => {
              entry.shouldBeCantonErrorCode(ImportAcsError)
              entry.message should include(
                show"Could not select a representative package-id for contract with id $contractId. No package in store for the contract's package-name 'foo'"
              )
            },
          ),
      )
    }

    "select a known package for a contract if the original rp-id is not known" in { implicit env =>
      import env.*

      val party = createUniqueParty(participant1, daName)

      // Upload only Foo V2 to P3
      participant3.dars.upload(FooV2Path)

      // Create a contract on P1
      // P1 has only Foo V1
      val contractId = createContract(participant1, party)

      // Export on P1 and import on P2 without any override
      exportAndImportOn(participant3, party)

      // Check the rp-id of contract after import on P3 (Foo V2)
      expectRpId(contractId, party, participant3, FooV2PkgId)
    }

    "consider representative package-id overrides" in { implicit env =>
      import env.*

      val party = createUniqueParty(participant1, daName)

      // Both participants have both versions of Foo
      participant1.dars.upload(FooV2Path)
      participant3.dars.upload(FooV1Path)

      // Create two contracts on P1
      val contractId1 = createContract(participant1, party)
      val contractId2 = createContract(participant1, party)

      // Check the initial rp-id of the contract is the same as the original template-id (Foo V2)
      expectRpId(
        contractId = contractId1,
        partyId = party,
        participantRef = participant1,
        expectedRpId = FooV2PkgId,
        expectedNumberOfEventsForUpdatesQuery = 2,
      )
      expectRpId(
        contractId = contractId2,
        partyId = party,
        participantRef = participant1,
        expectedRpId = FooV2PkgId,
        expectedNumberOfEventsForUpdatesQuery = 2,
      )

      // Export on P1 and import on P2 without any override
      exportAndImportOn(
        participant3,
        party,
        contractRpIdOverride = Map(
          contractId1 -> FooV1PkgId,
          contractId2 -> LfPackageId.assertFromString("unknown-pkg-id"),
        ),
      )

      // Representative package ID selection for contract 1 should have considered Foo V1 since it's known to P3
      expectRpId(
        contractId = contractId1,
        partyId = party,
        participantRef = participant3,
        expectedRpId = FooV1PkgId,
        expectedNumberOfEventsForUpdatesQuery = 2,
      )
      // Override for contract 2 should be ignored since the package-id is unknown to P3
      expectRpId(
        contractId = contractId2,
        partyId = party,
        participantRef = participant3,
        expectedRpId = FooV2PkgId,
        expectedNumberOfEventsForUpdatesQuery = 2,
      )
    }

    s"should fail on with import mode ${ContractImportMode.Accept} if the selected representative ID differs from the exported representative package ID" in {
      implicit env =>
        import env.*

        val party = createUniqueParty(participant1, daName)

        // Both participants have both versions of Foo
        // Create a contract on P1
        val contractId = createContract(participant1, party)

        // Check the initial rp-id of the contract is the same as the original template-id (Foo V2)
        expectRpId(contractId, party, participant1, FooV2PkgId)

        exportAndImportOn(
          participant3,
          party,
          contractRpIdOverride = Map(contractId -> FooV1PkgId),
          contractImportMode = ContractImportMode.Accept,
          handleImport = f =>
            assertThrowsAndLogsCommandFailures(
              f(),
              entry => {
                entry.shouldBeCantonErrorCode(ImportAcsError)
                entry.message should include(
                  show"Contract import mode is 'Accept' but the selected representative package-id ${LfPackageId
                      .assertFromString(FooV1PkgId)} for contract with id $contractId differs from the exported representative package-id ${LfPackageId.assertFromString(FooV2PkgId)}. Please use contract import mode 'Validation' or 'Recomputation' to change the representative package-id."
                )
              },
            ),
        )
    }

    s"should fail on contract validation failure if import mode is ${ContractImportMode.Validation}" in {
      implicit env =>
        import env.*

        val party = createUniqueParty(participant1, daName)
        val otherParty = createUniqueParty(participant1, daName)

        // Create a contract on P1
        val contractId = createContract(participant1, party, Some(otherParty))

        // Disable vetting so it is upgrade compatibility is not checked between V2 and V3 (that would fail)
        participant3.dars.upload(FooV3Path, vetAllPackages = false)

        // Import with validation mode should fail since the contract does not pass validation
        exportAndImportOn(
          participant3,
          party,
          contractRpIdOverride = Map(contractId -> FooV3PkgId),
          contractImportMode = ContractImportMode.Validation,
          handleImport = f =>
            assertThrowsAndLogsCommandFailures(
              f(),
              entry => {
                entry.shouldBeCantonErrorCode(ImportAcsError)
                entry.message should include(show"Failed to authenticate contract")
              },
            ),
        )
    }

    // TODO(#28075): Test vetting-based override when implemented
  }

  // TODO(#25385): Move representative package ID rendering assertions to LAPITT
  //               once it is possible to test
  //               a different representative ID than a contract's create package ID.
  //               Hint: when creation package unvetting for re-assignments is implemented,
  //               as currently ACS imports cannot be easily tested in LAPITT.
  private def expectRpId(
      contractId: LfContractId,
      partyId: PartyId,
      participantRef: LocalParticipantReference,
      expectedRpId: LfPackageId,
      expectedNumberOfEventsForUpdatesQuery: Int = 1,
  ): Assertion = {
    val queryIfooEventFormat = transaction_filter.EventFormat(
      filtersByParty = Map(
        partyId.toProtoPrimitive -> transaction_filter.Filters(
          cumulative = Seq(
            transaction_filter.CumulativeFilter(
              identifierFilter =
                transaction_filter.CumulativeFilter.IdentifierFilter.InterfaceFilter(
                  transaction_filter.InterfaceFilter(
                    interfaceId = Some(
                      apiValue.Identifier.fromJavaProto(
                        upgrades.v1.java.ifoo.IFoo.INTERFACE_ID.toProto
                      )
                    ),
                    includeInterfaceView = true,
                    includeCreatedEventBlob = false,
                  )
                )
            )
          )
        )
      ),
      filtersForAnyParty = None,
      verbose = true,
    )

    // We assert as follows:
    //   - For JSON API, only the ACS endpoint since the Daml value conversion and transcode logic is common with all other queries (e.g. updates)
    //   - For gRPC API, the updates, ACS and event query endpoints since they do not fully share the same backend logic for event serialization
    assertJsonApi(contractId, partyId, queryIfooEventFormat, participantRef, expectedRpId)
    assertGrpcApi(
      contractId,
      partyId,
      queryIfooEventFormat,
      participantRef,
      expectedRpId,
      expectedNumberOfEventsForUpdatesQuery,
    )
  }

  private def assertGrpcApi(
      contractId: LfContractId,
      party: PartyId,
      queryEventFormat: transaction_filter.EventFormat,
      participantRef: ParticipantReference,
      expectedRpId: LfPackageId,
      expectedNumberOfEventsForUpdatesQuery: Int,
  ): Assertion = {
    // Get create from update stream
    val updateSourcedCreatedEvent = participantRef.ledger_api.updates
      .updates(
        updateFormat = transaction_filter.UpdateFormat(
          includeTransactions = Some(
            transaction_filter.TransactionFormat(
              Some(queryEventFormat),
              transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA,
            )
          ),
          includeReassignments = None,
          includeTopologyEvents = None,
        ),
        completeAfter = PositiveInt.tryCreate(expectedNumberOfEventsForUpdatesQuery),
      )
      .flatMap(_.createEvents)
      .filter(_.contractId == contractId.coid)
      .loneElement

    // Get create from ACS
    val acsCreatedEvent = participantRef.ledger_api.state.acs
      .of_party(
        party,
        filterInterfaces = Seq(
          TemplateId.fromJavaIdentifier(upgrades.v1.java.ifoo.IFoo.INTERFACE_ID)
        ),
      )
      .filter(_.contractId == contractId.coid)
      .loneElement
      .event

    val eventQueryCreatedEvent = participantRef.ledger_api.event_query
      .by_contract_id(contractId.coid, Seq(party))
      .getCreated
      .getCreatedEvent

    // Equality check implies representative package IDs are the same
    updateSourcedCreatedEvent shouldBe acsCreatedEvent
    // Interface views not supported in event query endpoint, so check equality without interface views
    eventQueryCreatedEvent shouldBe acsCreatedEvent.copy(interfaceViews = Seq.empty)

    val expectedRepresentativeTemplateId = apiValue.Identifier(
      packageId = expectedRpId,
      moduleName = upgrades.v1.java.foo.Foo.TEMPLATE_ID.getModuleName,
      entityName = upgrades.v1.java.foo.Foo.TEMPLATE_ID.getEntityName,
    )

    // We don't care about the content but just that the view computation was successful
    // This verifies that the view computation works with the selected representative package-id
    acsCreatedEvent.interfaceViews.loneElement.viewValue shouldBe defined

    acsCreatedEvent.createArguments.value shouldBe apiValue.Record(
      recordId = Some(expectedRepresentativeTemplateId),
      fields = Vector(
        apiValue.RecordField(
          "owner",
          Some(apiValue.Value(apiValue.Value.Sum.Party(party.toProtoPrimitive))),
        ),
        apiValue.RecordField(
          "otherParty",
          Some(apiValue.Value(apiValue.Value.Sum.Party(party.toProtoPrimitive))),
        ),
      ),
    )

    acsCreatedEvent.representativePackageId shouldBe expectedRpId
  }

  private def assertJsonApi(
      contractId: LfContractId,
      partyId: PartyId,
      queryEventFormat: transaction_filter.EventFormat,
      participantRef: LocalParticipantReference,
      expectedRpId: LfPackageId,
  ): Assertion = {
    val endOffset = participantRef.ledger_api.state.end()
    val responses = queryJsonAcsFor(participantRef, queryEventFormat, endOffset).futureValue
    val targetCreatedEvent = responses
      .map(r =>
        inside(r.contractEntry) { case JsActiveContract(createdEvent, _, _) =>
          createdEvent
        }
      )
      .filter(_.contractId == contractId.coid)
      .loneElement
    inside(targetCreatedEvent) { createdEvent =>
      createdEvent.contractId shouldBe contractId.coid
      createdEvent.representativePackageId shouldBe expectedRpId
      // As in the gRPC assertion, we assert that rendering of the values is successful
      // which is relevant for situations where the representative package-id differs from
      // the creation package-id when the latter is unknown to the participant.
      createdEvent.createArgument.value shouldBe Json.obj(
        // Using two distinct parties to highlight validation failure on incompatible upgrade
        // when the order of the fields in a template changes
        "owner" -> Json.fromString(partyId.toProtoPrimitive),
        "otherParty" -> Json.fromString(partyId.toProtoPrimitive),
      )
      createdEvent.interfaceViews.loneElement.viewValue.value shouldBe Json.obj(
        "owner" -> Json.fromString(partyId.toProtoPrimitive)
      )
    }
  }

  private def queryJsonAcsFor(
      participantRef: LocalParticipantReference,
      queryEventFormat: transaction_filter.EventFormat,
      ledgerEndOffset: Long,
  ): Future[Seq[JsGetActiveContractsResponse]] = {
    val uri = Uri
      .from(
        scheme = "http",
        host = "localhost",
        port = participantRef.config.httpLedgerApi.port.unwrap,
      )
      .withPath(Uri.Path("/v2/state/active-contracts"))

    val getActiveContractsRequest = state_service
      .GetActiveContractsRequest(
        activeAtOffset = ledgerEndOffset,
        eventFormat = Some(queryEventFormat),
      )
      .asJson
      .toString()

    Http()
      .singleRequest(
        HttpRequest(
          method = HttpMethods.POST,
          uri = uri,
          headers = Seq.empty,
          entity = HttpEntity(ContentTypes.`application/json`, getActiveContractsRequest),
        )
      )
      .flatMap { response =>
        response.entity.dataBytes
          .runFold(ByteString.empty)((b, a) => b ++ a)
          .map(_.utf8String)
          .map(io.circe.parser.decode[Seq[JsGetActiveContractsResponse]])
      }
      .map(_.value)
  }

  private def exportAndImportOn(
      importParticipant: ParticipantReference,
      party: PartyId,
      contractRpIdOverride: Map[ContractId, LfPackageId] = Map.empty,
      packageIdOverride: Map[LfPackageId, LfPackageId] = Map.empty,
      packageNameOverride: Map[LfPackageName, LfPackageId] = Map.empty,
      contractImportMode: ContractImportMode = ContractImportMode.Validation,
      handleImport: (() => Unit) => Unit = (f: () => Unit) => f(),
  )(implicit env: FixtureParam): Unit = {
    import env.*

    // Replicate party on import participant
    PartyToParticipantDeclarative.forParty(Set(participant1, importParticipant), daId)(
      participant1.id,
      party,
      PositiveInt.one,
      Set(
        (participant1.id, ParticipantPermission.Submission),
        (importParticipant.id, ParticipantPermission.Submission),
      ),
    )(executionContext, env)

    File.usingTemporaryFile() { file =>
      participant1.repair.export_acs(
        parties = Set(party),
        exportFilePath = file.canonicalPath,
        synchronizerId = Some(env.daId),
        ledgerOffset = NonNegativeLong.tryCreate(participant1.ledger_api.state.end()),
      )

      importParticipant.synchronizers.disconnect_all()

      handleImport { () =>
        try {
          importAcs(
            importParticipant,
            contractRpIdOverride,
            packageIdOverride,
            packageNameOverride,
            contractImportMode,
            file,
          )
        } finally {
          importParticipant.synchronizers.reconnect_all()
        }
      }
    }
  }

  private def createContract(
      participantRef: ParticipantReference,
      party: PartyId,
      otherParty: Option[PartyId] = None,
  )(implicit
      env: FixtureParam
  ): protocol.LfContractId = {
    import env.*

    participantRef.ledger_api.javaapi.commands
      .submit(
        Seq(party.toLf),
        new upgrades.v1.java.foo.Foo(
          party.toProtoPrimitive,
          otherParty.getOrElse(party).toProtoPrimitive,
        )
          .create()
          .commands()
          .asScala
          .toSeq,
      )
      .getEvents
      .asScala
      .loneElement
      .getContractId
      .pipe(LfContractId.assertFromString)
  }
}

trait WithRepairServiceImportAcs {
  self: AcsImportRepresentativePackageIdSelectionIntegrationTest =>

  override protected def importAcs(
      importParticipant: ParticipantReference,
      contractRpIdOverride: Map[canton.protocol.LfContractId, LfPackageId],
      packageIdOverride: Map[LfPackageId, LfPackageId],
      packageNameOverride: Map[LfPackageName, LfPackageId],
      contractImportMode: ContractImportMode,
      file: File,
  ): Unit =
    importParticipant.repair
      .import_acs(
        importFilePath = file.canonicalPath,
        representativePackageIdOverride = RepresentativePackageIdOverride(
          contractOverride = contractRpIdOverride,
          packageIdOverride = packageIdOverride,
          packageNameOverride = packageNameOverride,
        ),
        contractImportMode = contractImportMode,
      )
      .discard
}

trait WithImportPartyAcs {
  self: AcsImportRepresentativePackageIdSelectionIntegrationTest =>

  override protected def importAcs(
      importParticipant: ParticipantReference,
      contractRpIdOverride: Map[canton.protocol.LfContractId, LfPackageId],
      packageIdOverride: Map[LfPackageId, LfPackageId],
      packageNameOverride: Map[LfPackageName, LfPackageId],
      contractImportMode: ContractImportMode,
      file: File,
  ): Unit = importParticipant.parties
    .import_party_acs(
      importFilePath = file.canonicalPath,
      representativePackageIdOverride = RepresentativePackageIdOverride(
        contractOverride = contractRpIdOverride,
        packageIdOverride = packageIdOverride,
        packageNameOverride = packageNameOverride,
      ),
      contractImportMode = contractImportMode,
    )
    .discard
}

// TODO(#25385): This test should be a variation in the conformance test suites
//               but since there is no possibility to test ACS import effects
//               and more importantly here
//               the representative package ID selection in LAPITT yet,
//               we keep it here for now.
trait WithoutImfoBuffer extends AcsImportRepresentativePackageIdSelectionIntegrationTest {
  override def environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransforms(ConfigTransforms.updateAllParticipantConfigs { case (_, config) =>
        config
          .focus(_.ledgerApi.indexService.maxTransactionsInMemoryFanOutBufferSize)
          .replace(0)
      })
}

// All combinations of with/without IMFO buffer and repair service/party ACS import
class AcsImportRpIdIntegrationTest_RepairService_ImfoBuffer
    extends AcsImportRepresentativePackageIdSelectionIntegrationTest
    with WithRepairServiceImportAcs

class AcsImportRpIdIntegrationTest_RepairService_NoImfoBuffer
    extends AcsImportRepresentativePackageIdSelectionIntegrationTest
    with WithRepairServiceImportAcs
    with WithoutImfoBuffer

class AcsImportRpIdIntegrationTest_PartyService_ImfoBuffer
    extends AcsImportRepresentativePackageIdSelectionIntegrationTest
    with WithImportPartyAcs

class AcsImportRpIdIntegrationTest_PartyService_NoImfoBuffer
    extends AcsImportRepresentativePackageIdSelectionIntegrationTest
    with WithImportPartyAcs
    with WithoutImfoBuffer
