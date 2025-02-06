// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party

import cats.data.EitherT
import cats.syntax.either.*
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
import com.digitalasset.canton.CommandId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
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

  /** Have the target/current participant submit a Daml PartyReplication.ChannelProposal contract to agree on with the source participant.
    */
  private[admin] def proposePartyReplication(
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sequencerCandidates: NonEmpty[Seq[SequencerId]],
      acsSnapshotTs: CantonTimestamp,
      startAtWatermark: NonNegativeInt,
      sourceParticipantId: ParticipantId,
      channelId: ChannelId,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val ChannelId(CommandId(id)) = channelId
    val channelProposal = new M.partyreplication.ChannelProposal(
      sourceParticipantId.adminParty.toProtoPrimitive,
      participantId.adminParty.toProtoPrimitive,
      sequencerCandidates.forgetNE.map(_.uid.toProtoPrimitive).asJava,
      new M.partyreplication.PartyReplicationMetadata(
        id,
        partyId.toProtoPrimitive,
        acsSnapshotTs.toInstant,
        startAtWatermark.value,
      ),
    )
    EitherT(
      retrySubmitter
        .submitCommands(
          Commands(
            applicationId = applicationId,
            commandId = s"channel-proposal-$id",
            actAs = Seq(participantId.adminParty.toProtoPrimitive),
            commands = channelProposal.create.commands.asScala.toSeq
              .map(LedgerClientUtils.javaCodegenToScalaProto),
            deduplicationPeriod =
              DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
            synchronizerId = synchronizerId.toProtoPrimitive,
          ),
          timeouts.default.asFiniteApproximation,
        )
        .map(
          handleCommandResult(s"propose channel $id to replicate party")
        )
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
            if event.templateId.exists(isTemplateChannelRelated) =>
          event
      }
      .foreach(
        createEventHandler(
          processChannelProposalAtSourceParticipant(tx, _),
          processChannelAgreementAtSourceOrTargetParticipant(tx, _),
        )
      )
  }

  private def processChannelProposalAtSourceParticipant(
      tx: Transaction,
      contract: M.partyreplication.ChannelProposal.Contract,
  )(implicit traceContext: TraceContext): Unit = {
    val synchronizerId = tx.synchronizerId
    logger.info(
      s"Received channel proposal for party ${contract.data.payloadMetadata.partyId} on synchronizer $synchronizerId" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )
    if (contract.data.sourceParticipant == participantId.adminParty.toProtoPrimitive) {
      val validationET = for {
        params <- EitherT.fromEither[FutureUnlessShutdown](
          ChannelProposalParams.fromDaml(contract.data, synchronizerId)
        )
        sequencerId <- partyReplicator.validateChannelProposalAtSourceParticipant(params)
      } yield sequencerId

      val commandResultF = for {
        acceptOrReject <- validationET.fold(
          err => {
            logger.warn(err)
            (
              contract.id.exerciseReject(err).commands,
              // Upon reject use the contract id as the channel-id might be invalid
              s"channel-proposal-reject-${contract.id.contractId}",
            )
          },
          sequencerId =>
            (
              contract.id.exerciseAccept(sequencerId.uid.toProtoPrimitive).commands,
              s"channel-proposal-accept-${contract.data.payloadMetadata.id}",
            ),
        )
        (exercise, commandId) = acceptOrReject
        commandResult <- performUnlessClosingF(s"submit $commandId")(
          retrySubmitter.submitCommands(
            Commands(
              applicationId = applicationId,
              commandId = commandId,
              actAs = Seq(participantId.adminParty.toProtoPrimitive),
              commands = exercise.asScala.toSeq.map(LedgerClientUtils.javaCodegenToScalaProto),
              deduplicationPeriod =
                DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
              synchronizerId = synchronizerId,
            ),
            timeouts.default.asFiniteApproximation,
          )
        )
      } yield commandResult

      superviseBackgroundSubmission("Accept or reject channel proposal", commandResultF)
    }
  }

  private def processChannelAgreementAtSourceOrTargetParticipant(
      tx: Transaction,
      contract: M.partyreplication.ChannelAgreement.Contract,
  )(implicit traceContext: TraceContext): Unit = {
    val synchronizerId = tx.synchronizerId
    logger.info(
      s"Received channel agreement for party ${contract.data.payloadMetadata.partyId} on synchronizer $synchronizerId" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )

    def processOrLogWarning(processAgreement: ChannelAgreementParams => Unit): Unit =
      ChannelAgreementParams
        .fromDaml(contract.data, synchronizerId)
        .fold(
          err => logger.warn(s"Failed to handle party replication agreement: $err"),
          agreementParams => processAgreement(agreementParams),
        )

    participantId.adminParty.toProtoPrimitive match {
      case `contract`.data.sourceParticipant =>
        processOrLogWarning(partyReplicator.processChannelAgreementAtSourceParticipant)
      case `contract`.data.targetParticipant =>
        processOrLogWarning(partyReplicator.processChannelAgreementAtTargetParticipant)
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
          .exists(_.templateId.exists(isTemplateChannelRelated))
      ) ||
      tx.event.unassignedEvent.exists(
        _.templateId.exists(isTemplateChannelRelated)
      )
    ) {
      implicit val traceContext: TraceContext =
        LedgerClient.traceContextFromLedgerApi(tx.traceContext)
      // TODO(#20638): Should we archive unexpectedly reassigned channel contracts or only warn?
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
          .exists(_.templateId.exists(isTemplateChannelRelated))
      )

    // TODO(#20636): Upon node restart or synchronizer reconnect, archive previously created contracts
    //  to reflect that channels are in-memory only and need to be recreated
    if (activeContracts.nonEmpty) {
      logger.info(
        s"Received ${activeContracts.length} active contracts ${acs.flatMap(_.createdEvent.map(_.contractId))}"
      )
    }
  }

  // Event handler sharable outside of create event handling, e.g. for acs handling
  private def createEventHandler(
      handleChannelProposal: M.partyreplication.ChannelProposal.Contract => Unit,
      handleChannelAgreement: M.partyreplication.ChannelAgreement.Contract => Unit,
  ): ScalaCreatedEvent => Unit = {
    case event
        if event.templateId
          .contains(channelProposalTemplate) =>
      val contract =
        M.partyreplication.ChannelProposal.COMPANION
          .fromCreatedEvent(JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(event)))
      handleChannelProposal(contract)
    case event
        if event.templateId
          .contains(channelAgreementTemplate) =>
      val contract =
        M.partyreplication.ChannelAgreement.COMPANION
          .fromCreatedEvent(JavaCreatedEvent.fromProto(ScalaCreatedEvent.toJavaProto(event)))
      handleChannelAgreement(contract)
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
      id: ChannelId,
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      synchronizerId: SynchronizerId,
  )
  final case class ChannelId private (id: CommandId) {
    def unwrap: String = id.unwrap
  }
  object ChannelId {
    def fromString(id: String): Either[String, ChannelId] =
      // Ensure id can be embedded in a commandId i.e. does not contain non-allowed characters
      // and is not too long.
      CommandId
        .fromProtoPrimitive(id)
        .bimap(err => s"Invalid channel id $err", cmdId => ChannelId(cmdId))
  }

  private def applicationId = "PartyReplicationAdminWorkflow"

  private def apiIdentifierFromJavaIdentifier(javaIdentifier: JavaIdentifier): Identifier =
    Identifier(
      packageId = javaIdentifier.getPackageId,
      moduleName = javaIdentifier.getModuleName,
      entityName = javaIdentifier.getEntityName,
    )

  @VisibleForTesting
  lazy val channelProposalTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(M.partyreplication.ChannelProposal.TEMPLATE_ID_WITH_PACKAGE_ID)
  lazy val channelAgreementTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(M.partyreplication.ChannelAgreement.TEMPLATE_ID_WITH_PACKAGE_ID)

  private def isTemplateChannelRelated(id: Identifier) =
    id == channelProposalTemplate || id == channelAgreementTemplate
}
