// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import com.daml.ledger.api.v2.commands.Commands
import com.daml.ledger.api.v2.commands.Commands.DeduplicationPeriod.DeduplicationDuration
import com.daml.ledger.api.v2.event.{CreatedEvent as ScalaCreatedEvent, Event}
import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.state_service.ActiveContract
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v2.value.Identifier
import com.daml.ledger.javaapi.data.{CreatedEvent as JavaCreatedEvent, Identifier as JavaIdentifier}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.ledger.client.{LedgerClient, LedgerClientUtils}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.AdminWorkflowService
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow.*
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.ledger.api.client.{
  CommandResult,
  CommandSubmitterWithRetry,
  LedgerConnection,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

/** Daml admin workflow reacting to party management proposals and agreements among participants.
  */
class PartyReplicationAdminWorkflow(
    ledgerClient: LedgerClient,
    participantId: ParticipantId,
    val partyReplicator: PartyReplicator,
    syncService: CantonSyncService,
    clock: Clock,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext
) extends AdminWorkflowService
    with FlagCloseable
    with NamedLogging {

  /** Have the target/current participant submit a Daml PartyReplication.PartyReplicationProposal
    * contract to agree on with the source participant.
    */
  private[admin] def proposePartyReplication(
      partyReplicationId: Hash,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      sequencerCandidates: NonEmpty[Seq[SequencerId]],
      serial: Option[PositiveInt],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val partyReplicationIdS = partyReplicationId.toHexString
    val serialOrZero: Int = serial.map(_.value).getOrElse(0)
    val proposal = new M.partyreplication.PartyReplicationProposal(
      partyReplicationIdS,
      partyId.toProtoPrimitive,
      sourceParticipantId.adminParty.toProtoPrimitive,
      participantId.adminParty.toProtoPrimitive,
      sequencerCandidates.forgetNE.map(_.uid.toProtoPrimitive).asJava,
      serialOrZero,
    )
    EitherT(
      retrySubmitter
        .submitCommands(
          Commands(
            workflowId = "",
            applicationId = applicationId,
            commandId = s"proposal-$partyReplicationIdS",
            commands = proposal.create.commands.asScala.toSeq
              .map(LedgerClientUtils.javaCodegenToScalaProto),
            deduplicationPeriod =
              DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
            minLedgerTimeAbs = None,
            minLedgerTimeRel = None,
            actAs = Seq(participantId.adminParty.toProtoPrimitive),
            readAs = Nil,
            submissionId = "",
            disclosedContracts = Nil,
            synchronizerId = synchronizerId.toProtoPrimitive,
            packageIdSelectionPreference = Nil,
            prefetchContractKeys = Nil,
          ),
          timeouts.default.asFiniteApproximation,
        )
        .map(handleCommandResult(s"propose $partyReplicationIdS to replicate party"))
    ).mapK(FutureUnlessShutdown.outcomeK)
  }

  override private[admin] def filters: TransactionFilter =
    // we can't filter by template id as we don't know when the admin workflow package is loaded
    LedgerConnection.transactionFilterByParty(Map(participantId.adminParty -> Seq.empty))

  override private[admin] def processTransaction(tx: Transaction): Unit = {
    implicit val traceContext: TraceContext =
      LedgerClient.traceContextFromLedgerApi(tx.traceContext)

    tx.events
      .collect {
        case Event(Event.Event.Created(event))
            if event.templateId.exists(isTemplatePartyReplicationRelated) =>
          event
      }
      .foreach(
        createEventHandler(
          processProposalAtSourceParticipant(tx, _),
          processAgreementAtSourceOrTargetParticipant(tx, _),
        )
      )
  }

  private def processProposalAtSourceParticipant(
      tx: Transaction,
      contract: M.partyreplication.PartyReplicationProposal.Contract,
  )(implicit traceContext: TraceContext): Unit = {
    val synchronizerId = tx.synchronizerId
    logger.info(
      s"Received party replication proposal ${contract.data.partyReplicationId} for party ${contract.data.partyId} on synchronizer $synchronizerId" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )
    if (contract.data.sourceParticipant == participantId.adminParty.toProtoPrimitive) {
      val validationET = for {
        params <- EitherT.fromEither[FutureUnlessShutdown](
          PartyReplicationProposalParams.fromDaml(contract.data, synchronizerId)
        )
        sequencerId <- partyReplicator.validatePartyReplicationProposalAtSourceParticipant(params)
      } yield sequencerId

      val commandResultF = for {
        acceptOrReject <- validationET.fold(
          err => {
            logger.warn(err)
            (
              contract.id.exerciseReject(err).commands,
              // Upon reject use the contract id as the party-replication-id might be invalid
              s"proposal-reject-${contract.id.contractId}",
            )
          },
          sequencerId =>
            (
              contract.id.exerciseAccept(sequencerId.uid.toProtoPrimitive).commands,
              s"proposal-accept-${contract.data.partyReplicationId}",
            ),
        )
        (exercise, commandId) = acceptOrReject
        commandResult <- performUnlessClosingF(s"submit $commandId")(
          retrySubmitter.submitCommands(
            Commands(
              workflowId = "",
              applicationId = applicationId,
              commandId = commandId,
              commands = exercise.asScala.toSeq.map(LedgerClientUtils.javaCodegenToScalaProto),
              deduplicationPeriod =
                DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
              minLedgerTimeAbs = None,
              minLedgerTimeRel = None,
              actAs = Seq(participantId.adminParty.toProtoPrimitive),
              readAs = Nil,
              submissionId = "",
              disclosedContracts = Nil,
              synchronizerId = synchronizerId,
              packageIdSelectionPreference = Nil,
              prefetchContractKeys = Nil,
            ),
            timeouts.default.asFiniteApproximation,
          )
        )
      } yield commandResult

      superviseBackgroundSubmission("Accept or reject proposal", commandResultF)
    }
  }

  private def processAgreementAtSourceOrTargetParticipant(
      tx: Transaction,
      contract: M.partyreplication.PartyReplicationAgreement.Contract,
  )(implicit traceContext: TraceContext): Unit = {
    val synchronizerId = tx.synchronizerId
    logger.info(
      s"Received agreement for party ${contract.data.partyId} on synchronizer $synchronizerId" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )

    def processOrLogWarning(processAgreement: PartyReplicationAgreementParams => Unit): Unit =
      PartyReplicationAgreementParams
        .fromDaml(contract.data, synchronizerId)
        .fold(
          err => logger.warn(s"Failed to handle party replication agreement: $err"),
          agreementParams => processAgreement(agreementParams),
        )

    participantId.adminParty.toProtoPrimitive match {
      case `contract`.data.sourceParticipant =>
        processOrLogWarning(partyReplicator.processPartyReplicationAgreementAtSourceParticipant)
      case `contract`.data.targetParticipant =>
        processOrLogWarning(partyReplicator.processPartyReplicationAgreementAtTargetParticipant)
      case nonStakeholder =>
        logger.warn(
          s"Received unexpected party replication agreement between source ${contract.data.sourceParticipant}" +
            s"and target ${contract.data.targetParticipant} on non-stakeholder participant $nonStakeholder"
        )
    }
  }

  override private[admin] def processReassignment(tx: Reassignment): Unit =
    if (
      tx.event.assignedEvent.exists(
        _.createdEvent
          .exists(_.templateId.exists(isTemplatePartyReplicationRelated))
      ) ||
      tx.event.unassignedEvent.exists(
        _.templateId.exists(isTemplatePartyReplicationRelated)
      )
    ) {
      implicit val traceContext: TraceContext =
        LedgerClient.traceContextFromLedgerApi(tx.traceContext)
      // TODO(#20638): Should we archive unexpectedly reassigned party replication contracts or only warn?
      logger.warn(
        s"Received unexpected reassignment of party replication related contract: ${tx.event}"
      )
    }

  override private[admin] def processAcs(acs: Seq[ActiveContract])(implicit
      traceContext: TraceContext
  ): Unit = {
    val activeContracts = acs
      .filter(
        _.createdEvent
          .exists(_.templateId.exists(isTemplatePartyReplicationRelated))
      )

    // TODO(#20636): Upon node restart or synchronizer reconnect, archive previously created contracts
    //  to reflect that channels are in-memory only and need to be recreated
    if (activeContracts.nonEmpty) {
      logger.info(
        s"Received ${activeContracts.length} active contracts ${acs.flatMap(_.createdEvent.map(_.contractId))}"
      )
    }
  }

  // Event handler sharable outside create event handling, e.g. for acs handling
  private def createEventHandler(
      handleProposal: M.partyreplication.PartyReplicationProposal.Contract => Unit,
      handleAgreement: M.partyreplication.PartyReplicationAgreement.Contract => Unit,
  ): ScalaCreatedEvent => Unit = {
    case event
        if event.templateId
          .contains(proposalTemplate) =>
      val contract =
        M.partyreplication.PartyReplicationProposal.COMPANION
          .fromCreatedEvent(JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(event)))
      handleProposal(contract)
    case event
        if event.templateId
          .contains(agreementTemplate) =>
      val contract =
        M.partyreplication.PartyReplicationAgreement.COMPANION
          .fromCreatedEvent(JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(event)))
      handleAgreement(contract)
    case _ => ()
  }

  private def superviseBackgroundSubmission(
      operation: String,
      submission: FutureUnlessShutdown[CommandResult],
  )(implicit traceContext: TraceContext): Unit =
    futureSupervisor
      .supervisedUS(operation)(submission)
      .failOnShutdownToAbortException(operation)
      .foreach(handleCommandResult(operation))

  override def onClosed(): Unit =
    // Note that we can not time out requests nicely here on shutdown as the admin
    // server is closed first, which means that our requests will never
    // return properly on shutdown abort.
    LifeCycle.close(retrySubmitter, ledgerClient)(logger)

  private val retrySubmitter = new CommandSubmitterWithRetry(
    ledgerClient.commandService,
    clock,
    futureSupervisor,
    timeouts,
    loggerFactory,
    decideRetry = _ => None,
  )
}

object PartyReplicationAdminWorkflow {
  final case class PartyReplicationArguments(
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantIdO: Option[ParticipantId],
      serialO: Option[PositiveInt],
  )
  private def applicationId = "PartyReplicationAdminWorkflow"

  private def apiIdentifierFromJavaIdentifier(javaIdentifier: JavaIdentifier): Identifier =
    Identifier(
      packageId = javaIdentifier.getPackageId,
      moduleName = javaIdentifier.getModuleName,
      entityName = javaIdentifier.getEntityName,
    )

  @VisibleForTesting
  lazy val proposalTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.partyreplication.PartyReplicationProposal.TEMPLATE_ID_WITH_PACKAGE_ID
    )
  lazy val agreementTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.partyreplication.PartyReplicationAgreement.TEMPLATE_ID_WITH_PACKAGE_ID
    )

  private def isTemplatePartyReplicationRelated(id: Identifier) =
    id == proposalTemplate || id == agreementTemplate
}
