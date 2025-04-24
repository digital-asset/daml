// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TemplateFilter,
  UpdateFormat,
}
import com.daml.ledger.javaapi.data.Identifier
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.ReassignmentWrapper
import com.digitalasset.canton.admin.api.client.data.TemplateId
import com.digitalasset.canton.config
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.examples.java.iou.{Dummy, GetCash, Iou}
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencerBase.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UseCommunityReferenceBlockSequencer,
  UsePostgres,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.{
  HasCommandRunnersHelpers,
  HasReassignmentCommandsHelpers,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}

import scala.jdk.CollectionConverters.*

abstract class UpdateServiceIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasReassignmentCommandsHelpers
    with HasCommandRunnersHelpers {

  private val otherPartyName = "otherParty"

  private var otherParty: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S2M1_S2M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.synchronizers.connect_local(sequencer3, alias = acmeName)

      participant1.dars.upload(CantonExamplesPath)

      // Allocate parties
      otherParty = participant1.parties.enable(otherPartyName, synchronizer = daName)
      participant1.parties.enable(otherPartyName, synchronizer = acmeName)

    }

  private lazy val plugin =
    new UseCommunityReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      sequencerGroups = MultiSynchronizer(
        Seq(
          Set(InstanceName.tryCreate("sequencer1"), InstanceName.tryCreate("sequencer2")),
          Set(InstanceName.tryCreate("sequencer3"), InstanceName.tryCreate("sequencer4")),
        )
      ),
    )

  registerPlugin(plugin)
  registerPlugin(new UsePostgres(loggerFactory))

  "serve merged completions and updates streams" in { implicit env =>
    import env.*

    val party = participant1.id.adminParty

    val userId = "CantonConsole"

    val ledgerEndBeforeCommands = participant1.ledger_api.state.end()

    val (_, updateOnDa, completionOnDa) = runCommand(
      cmd = IouSyntax.createIou(participant1, Some(daId))(party, party),
      synchronizerId = daId,
      userId = userId,
      submittingParty = party.toLf,
    )

    val (_, updateOnAcme, completionOnAcme) = runCommand(
      cmd = IouSyntax.createIou(participant1, Some(acmeId))(party, party),
      synchronizerId = acmeId,
      userId = userId,
      submittingParty = party.toLf,
    )

    val expectedUpdates = Seq(updateOnDa, updateOnAcme)
    val expectedCompletions = Seq(completionOnDa, completionOnAcme)

    val ledgerEndAfterCommands = participant1.ledger_api.state.end()

    val receivedUpdates = participant1.ledger_api.updates.flat(
      partyIds = Set(party.toLf),
      completeAfter = Int.MaxValue,
      beginOffsetExclusive = ledgerEndBeforeCommands,
      endOffsetInclusive = Some(ledgerEndAfterCommands),
    )

    receivedUpdates should have size 2
    receivedUpdates should contain theSameElementsAs expectedUpdates

    val receivedCompletions =
      getCompletions(
        participant = participant1,
        submittingParty = party.toLf,
        startExclusive = ledgerEndBeforeCommands,
        userId = userId,
      )
    receivedCompletions should have size 2
    receivedCompletions should contain theSameElementsAs expectedCompletions

  }

  "serve reassignments with filter for one party and template wildcard" in { implicit env =>
    import env.*

    val signatory = participant1.id.adminParty
    val observer = signatory

    createAndReassignContracts(signatory, observer)(env)

  }

  private def createAndReassignContracts(
      signatory: PartyId,
      observer: PartyId,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val submittingParty = signatory

    // Create contracts
    val iou =
      IouSyntax.createIou(participant1, Some(daId))(signatory, observer)
    val dummy =
      createDummy(participant1, Some(daId))(signatory)

    val cids = Seq(iou.id.toLf, dummy.id.toLf)

    val ledgerEndBeforeUnassignments = participant1.ledger_api.state.end()

    val reassignments: Seq[Reassignment] = cids flatMap { cid =>
      val (unassignedEvent, _) = unassign(
        cid = cid,
        source = daId,
        target = acmeId,
        submittingParty = submittingParty.toLf,
      )

      val assignedEvent = assign(
        unassignId = unassignedEvent.unassignId,
        source = daId,
        target = acmeId,
        submittingParty = submittingParty.toLf,
      )

      Seq(unassignedEvent.reassignment, assignedEvent._1.reassignment)
    }

    val ledgerEndAfterAssignments = participant1.ledger_api.state.end()

    // Cleaning
    IouSyntax.archive(participant1, Some(acmeId))(iou, signatory)
    archiveDummy(participant1, Some(acmeId))(dummy, signatory)

    // reassignments for specific party and all templates
    checkReassignments(
      partyIds = Set(submittingParty.toLf),
      templateIds = Seq.empty,
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 4,
    )

    checkReassignmentsPointwise(
      partyIds = Set(submittingParty.toLf),
      templateIds = Seq.empty,
      expectedReassignmentsSize = 4,
      reassignments = reassignments,
    )

    // reassignments for specific party and single specific template
    checkReassignments(
      partyIds = Set(submittingParty.toLf),
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID),
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 2,
    )

    checkReassignmentsPointwise(
      partyIds = Set(submittingParty.toLf),
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID),
      expectedReassignmentsSize = 2,
      reassignments = reassignments,
    )

    // reassignments for specific party and both specific templates
    checkReassignments(
      partyIds = Set(submittingParty.toLf),
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID, Dummy.TEMPLATE_ID_WITH_PACKAGE_ID),
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 4,
    )

    checkReassignmentsPointwise(
      partyIds = Set(submittingParty.toLf),
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID, Dummy.TEMPLATE_ID_WITH_PACKAGE_ID),
      expectedReassignmentsSize = 4,
      reassignments = reassignments,
    )

    // reassignments for all parties and all templates
    checkReassignments(
      partyIds = Set.empty,
      templateIds = Seq.empty,
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 4,
    )

    checkReassignmentsPointwise(
      partyIds = Set.empty,
      templateIds = Seq.empty,
      expectedReassignmentsSize = 4,
      reassignments = reassignments,
    )

    // reassignments for all parties and single specific template
    checkReassignments(
      partyIds = Set.empty,
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID),
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 2,
    )

    checkReassignmentsPointwise(
      partyIds = Set.empty,
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID),
      expectedReassignmentsSize = 2,
      reassignments = reassignments,
    )

    // reassignments for all parties and both specific template
    checkReassignments(
      partyIds = Set.empty,
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID, Dummy.TEMPLATE_ID_WITH_PACKAGE_ID),
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 4,
    )

    checkReassignmentsPointwise(
      partyIds = Set.empty,
      templateIds = Seq(Iou.TEMPLATE_ID_WITH_PACKAGE_ID, Dummy.TEMPLATE_ID_WITH_PACKAGE_ID),
      expectedReassignmentsSize = 4,
      reassignments = reassignments,
    )

    // reassignments for irrelevant party
    checkReassignments(
      partyIds = Set(otherParty),
      templateIds = Seq.empty,
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 0,
    )

    checkReassignmentsPointwise(
      partyIds = Set(otherParty),
      templateIds = Seq.empty,
      expectedReassignmentsSize = 0,
      reassignments = reassignments,
    )

    // reassignments for irrelevant template
    checkReassignments(
      partyIds = Set.empty,
      templateIds = Seq(GetCash.TEMPLATE_ID_WITH_PACKAGE_ID),
      ledgerEndBeforeUnassignments = ledgerEndBeforeUnassignments,
      ledgerEndAfterAssignments = ledgerEndAfterAssignments,
      expectedReassignmentsSize = 0,
    )

    checkReassignmentsPointwise(
      partyIds = Set.empty,
      templateIds = Seq(GetCash.TEMPLATE_ID_WITH_PACKAGE_ID),
      expectedReassignmentsSize = 0,
      reassignments = reassignments,
    )

  }

  private def checkReassignments(
      partyIds: Set[PartyId],
      templateIds: Seq[Identifier],
      ledgerEndBeforeUnassignments: Long,
      ledgerEndAfterAssignments: Long,
      expectedReassignmentsSize: Long,
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val receivedReassignments = participant1.ledger_api.updates
      .reassignments(
        partyIds = partyIds,
        filterTemplates = templateIds.map(TemplateId.fromJavaIdentifier),
        completeAfter = Int.MaxValue,
        beginOffsetExclusive = ledgerEndBeforeUnassignments,
        endOffsetInclusive = Some(ledgerEndAfterAssignments),
      )

    receivedReassignments should have size expectedReassignmentsSize
    receivedReassignments.filter(_.isUnassignment) should have size expectedReassignmentsSize / 2
  }

  private def checkReassignmentsPointwise(
      partyIds: Set[PartyId],
      templateIds: Seq[Identifier],
      expectedReassignmentsSize: Long,
      reassignments: Seq[Reassignment],
  )(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    val updateFormat = getUpdateFormat(partyIds, templateIds.map(TemplateId.fromJavaIdentifier))

    val reassignmentsById =
      reassignments flatMap { reassignment =>
        participant1.ledger_api.updates
          .update_by_id(reassignment.updateId, updateFormat)
          .collect { case reassignmentWrapper: ReassignmentWrapper => reassignmentWrapper }
      }

    reassignmentsById should have size expectedReassignmentsSize
    reassignmentsById.filter(_.isUnassignment) should have size expectedReassignmentsSize / 2

    val reassignmentsByOffset =
      reassignments flatMap { reassignment =>
        participant1.ledger_api.updates
          .update_by_offset(reassignment.offset, updateFormat)
          .collect { case reassignmentWrapper: ReassignmentWrapper => reassignmentWrapper }
      }

    reassignmentsByOffset should have size expectedReassignmentsSize
    reassignmentsByOffset.filter(_.isUnassignment) should have size expectedReassignmentsSize / 2

  }

  private def createDummy(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId],
  )(
      party: PartyId,
      optTimeout: Option[config.NonNegativeDuration] = Some(
        participant.consoleEnvironment.commandTimeouts.ledgerCommand
      ),
  ): Dummy.Contract = {
    val createDummyCmd = new Dummy(party.toProtoPrimitive).create().commands().asScala.toSeq

    val tx = participant.ledger_api.javaapi.commands.submit_flat(
      Seq(party),
      createDummyCmd,
      synchronizerId,
      optTimeout = optTimeout,
    )
    JavaDecodeUtil.decodeAllCreated(Dummy.COMPANION)(tx).headOption.value
  }

  private def archiveDummy(
      participant: ParticipantReference,
      synchronizerId: Option[SynchronizerId],
  )(
      contract: Dummy.Contract,
      submittingParty: PartyId,
  ): Unit =
    participant.ledger_api.commands
      .submit(
        Seq(submittingParty),
        contract.id
          .exerciseArchive()
          .commands
          .asScala
          .toSeq
          .map(c => Command.fromJavaProto(c.toProtoCommand)),
        synchronizerId,
      )
      .discard

  private def getUpdateFormat(
      partyIds: Set[PartyId],
      filterTemplates: Seq[TemplateId],
  ): UpdateFormat = {
    val filters: Filters = Filters(
      filterTemplates.map(templateId =>
        CumulativeFilter(
          IdentifierFilter.TemplateFilter(
            TemplateFilter(Some(templateId.toIdentifier), includeCreatedEventBlob = false)
          )
        )
      )
    )

    UpdateFormat(
      includeReassignments =
        if (partyIds.isEmpty)
          Some(
            EventFormat(
              filtersByParty = Map.empty,
              filtersForAnyParty = Some(filters),
              verbose = false,
            )
          )
        else
          Some(
            EventFormat(
              filtersByParty = partyIds.map(_.toLf -> filters).toMap,
              filtersForAnyParty = None,
              verbose = false,
            )
          ),
      includeTransactions = None,
      includeTopologyEvents = None,
    )

  }
}

class ReferenceUpdateServiceIntegrationTest extends UpdateServiceIntegrationTest
