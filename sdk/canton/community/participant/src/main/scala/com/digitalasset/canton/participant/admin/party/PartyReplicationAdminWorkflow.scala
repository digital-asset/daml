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
import com.daml.ledger.api.v2.transaction_filter.EventFormat
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
import com.digitalasset.canton.participant.config.UnsafeOnlinePartyReplicationConfig
import com.digitalasset.canton.participant.ledger.api.client.{
  CommandResult,
  CommandSubmitterWithRetry,
  LedgerConnection,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SequencerId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.google.common.annotations.VisibleForTesting
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*
import scala.util.chaining.scalaUtilChainingOps

/** Daml admin workflow reacting to party management proposals and agreements among participants.
  */
class PartyReplicationAdminWorkflow(
    ledgerClient: LedgerClient,
    participantId: ParticipantId,
    syncService: CantonSyncService,
    clock: Clock,
    config: UnsafeOnlinePartyReplicationConfig,
    futureSupervisor: FutureSupervisor,
    exitOnFatalFailures: Boolean,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    executionContext: ExecutionContext,
    actorSystem: ActorSystem,
) extends AdminWorkflowService
    with FlagCloseable
    with NamedLogging {

  // See the note in the PartyReplicator pertaining to lifetime.
  val partyReplicator =
    new PartyReplicator(
      participantId,
      syncService,
      clock,
      config,
      markOnPRAgreementDone,
      futureSupervisor,
      exitOnFatalFailures,
      timeouts,
      loggerFactory,
    )

  /** Have the target/current participant submit a Daml PartyReplication.PartyReplicationProposal
    * contract to agree on with the source participant.
    */
  private[admin] def proposePartyReplication(
      partyReplicationId: Hash,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      sequencerCandidates: NonEmpty[Seq[SequencerId]],
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val partyReplicationIdS = partyReplicationId.toHexString
    val proposal = new M.partyreplication.PartyReplicationProposal(
      partyReplicationIdS,
      partyId.toProtoPrimitive,
      sourceParticipantId.adminParty.toProtoPrimitive,
      participantId.adminParty.toProtoPrimitive,
      sequencerCandidates.forgetNE.map(_.uid.toProtoPrimitive).asJava,
      serial.unwrap,
      PartyParticipantPermission.toDaml(participantPermission),
    )
    EitherT(
      retrySubmitter
        .submitCommands(
          Commands(
            workflowId = "",
            userId = userId,
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

  override private[admin] def eventFormat: EventFormat = {
    val templates = Seq(
      M.partyreplication.PartyReplicationProposal.TEMPLATE_ID,
      M.partyreplication.PartyReplicationAgreement.TEMPLATE_ID,
    )

    LedgerConnection.eventFormatByParty(
      Map(participantId.adminParty -> templates.map(LedgerConnection.mapTemplateIds))
    )
  }

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
          processProposalAtSourceParticipant(tx.synchronizerId, _),
          processAgreementAtSourceOrTargetParticipant(
            tx.synchronizerId,
            _,
            mightNotRememberAgreement = false,
          ),
        )
      )
  }

  private def processProposalAtSourceParticipant(
      synchronizerIdS: String,
      contract: M.partyreplication.PartyReplicationProposal.Contract,
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Received party replication proposal ${contract.data.partyReplicationId} for party ${contract.data.partyId} on synchronizer $synchronizerIdS" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )

    def respondToProposal(
        eitherErrorOrSequencerId: Either[String, PartyReplicationAgreementParams]
    ): Unit = {
      val (exercise, commandId) = eitherErrorOrSequencerId.fold(
        err => {
          logger.warn(err)
          (
            contract.id.exerciseReject(err).commands,
            // Upon reject use the contract id as the party-replication-id might be an invalid
            // command id if the party replication proposal contract was created by hand.
            s"proposal-reject-${contract.id.contractId}",
          )
        },
        agreementParams =>
          (
            contract.id.exerciseAccept(agreementParams.sequencerId.uid.toProtoPrimitive).commands,
            s"proposal-accept-${contract.data.partyReplicationId}",
          ),
      )
      val commandResultF = synchronizeWithClosingF(s"submit $commandId")(
        retrySubmitter.submitCommands(
          Commands(
            workflowId = "",
            userId = userId,
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
            synchronizerId = synchronizerIdS,
            packageIdSelectionPreference = Nil,
            prefetchContractKeys = Nil,
          ),
          timeouts.default.asFiniteApproximation,
        )
      )
      superviseBackgroundSubmission(
        s"Accept or reject proposal ${contract.data.partyReplicationId}",
        commandResultF,
      )
    }

    if (contract.data.sourceParticipant == participantId.adminParty.toProtoPrimitive) {
      partyReplicator.processPartyReplicationProposalAtSourceParticipant(
        PartyReplicationProposalParams.fromDaml(contract.data, synchronizerIdS),
        respondToProposal,
      )
    }
  }

  private def processAgreementAtSourceOrTargetParticipant(
      synchronizerIdS: String,
      contract: M.partyreplication.PartyReplicationAgreement.Contract,
      mightNotRememberAgreement: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    logger.info(
      s"Received agreement for party ${contract.data.partyId} on synchronizer $synchronizerIdS" +
        s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
    )
    participantId.adminParty.toProtoPrimitive match {
      case `contract`.data.sourceParticipant | `contract`.data.targetParticipant =>
        (for {
          lfContractId <- LfContractId.fromString(contract.id.contractId)
          params <- PartyReplicationAgreementParams.fromDaml(contract.data, synchronizerIdS)
        } yield partyReplicator
          .processPartyReplicationAgreement(lfContractId, mightNotRememberAgreement)(params))
          .valueOr(err => logger.warn(s"Malformed party replication agreement: $err"))
      case nonStakeholder =>
        logger.warn(
          s"Received unexpected party replication agreement between source ${contract.data.sourceParticipant}" +
            s"and target ${contract.data.targetParticipant} on non-stakeholder participant $nonStakeholder"
        )
    }
  }

  override private[admin] def processReassignment(tx: Reassignment): Unit =
    if (
      tx.events
        .flatMap(_.event.assigned)
        .exists(
          _.createdEvent
            .exists(_.templateId.exists(isTemplatePartyReplicationRelated))
        ) ||
      tx.events
        .flatMap(_.event.unassigned)
        .exists(
          _.templateId.exists(isTemplatePartyReplicationRelated)
        )
    ) {
      implicit val traceContext: TraceContext =
        LedgerClient.traceContextFromLedgerApi(tx.traceContext)
      // TODO(#20638): Should we archive unexpectedly reassigned party replication contracts or only warn?
      logger.warn(
        s"Received unexpected reassignment of party replication related contract: ${tx.events}"
      )
    }

  override private[admin] def processAcs(acs: Seq[ActiveContract])(implicit
      traceContext: TraceContext
  ): Unit = {
    val createdEvents = acs
      .collect {
        case ActiveContract(Some(createdEvent), synchronizerIdS, _)
            if createdEvent.templateId.exists(isTemplatePartyReplicationRelated) =>
          createdEvent -> synchronizerIdS
      }

    // Upon source participant restart, check for party replications agreements that
    // may indicate an interrupted OnPR.
    // TODO(#20636): Once OnPR no longer pauses indexing, remove this eager action on the
    //  part of the SP, and have the TP renegotiate resuming OnPR via a new proposal.
    if (createdEvents.nonEmpty) {
      logger.info(
        s"Found ${createdEvents.length} active party replication agreement contracts ${createdEvents
            .map(_._1.contractId)} upon participant start"
      )
      createdEvents.foreach { case (createdEvent, synchronizerIdS) =>
        createEventHandler(
          processProposalAtSourceParticipant(synchronizerIdS, _),
          {
            case onPRAgreement
                if onPRAgreement.data.sourceParticipant == participantId.adminParty.toProtoPrimitive =>
              processAgreementAtSourceOrTargetParticipant(
                synchronizerIdS,
                onPRAgreement,
                // maybe unknown due to the participant restart and lack of SP-side persistence:
                mightNotRememberAgreement = true,
              )
            case _ => ()
          },
        )(createdEvent)
      }
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

  /** Attempts to mark an active agreement as done when the TP deems that OnPR has progressed
    * sufficiently far that the SP's involvement is no longer needed. As a result of the agreement
    * being archived, the SP will no longer act on the agreement's behalf particularly after the SP
    * restarts.
    *
    * @return
    *   {@code true} if the agreement was successfully marked as done, {@code false} otherwise.
    */
  private def markOnPRAgreementDone(
      ap: PartyReplicationAgreementParams,
      damlAgreementCid: LfContractId,
      tc: TraceContext,
  ): FutureUnlessShutdown[Boolean] = {
    implicit val traceContext: TraceContext = tc
    (for {
      connectedSynchronizer <-
        EitherT.fromEither[FutureUnlessShutdown](
          syncService
            .readyConnectedSynchronizerById(ap.synchronizerId)
            .toRight {
              s"Synchronizer ${ap.synchronizerId} not connected when marking agreement done for ${ap.requestId}"
                .tap(logger.debug(_))
            }
        )
      // Use the ActiveContractStore to query the activeness of the daml agreement contract as
      // that is significantly faster than querying the ACS via the ledger api and more robust than
      // monitoring for the archival which may be missed in case the participant is restarted.
      isAgreementActive <- EitherT.right[String](
        connectedSynchronizer.synchronizerHandle.syncPersistentState.activeContractStore
          .fetchStates(Seq(damlAgreementCid))
          .map(_.values.exists(_.status.isActive))
      )
      _ <- EitherTUtil.ifThenET(isAgreementActive)(
        exerciseAgreementDoneOnTargetParticipant(ap, damlAgreementCid)
      )
    } yield !isAgreementActive).getOrElse(
      false // when unable to check or submit changes, return false to have caller check again
    )
  }

  /** Archive the agreement contract by exercising the done choice. */
  private def exerciseAgreementDoneOnTargetParticipant(
      ap: PartyReplicationAgreementParams,
      damlAgreementCid: LfContractId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    logger.info(s"Marking agreement done for ${ap.requestId} on target participant")
    val commandId = s"agreement-done-${ap.requestId}"
    val agreementCid =
      new M.partyreplication.PartyReplicationAgreement.ContractId(damlAgreementCid.coid)
    val exercise = agreementCid.exerciseDone(participantId.adminParty.uid.toProtoPrimitive).commands
    val operation = s"submit $commandId"
    EitherTUtil.ifThenET(participantId == ap.targetParticipantId) {
      EitherT(
        synchronizeWithClosingF(operation)(
          retrySubmitter
            .submitCommands(
              Commands(
                workflowId = "",
                userId = userId,
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
                synchronizerId = ap.synchronizerId.toProtoPrimitive,
                packageIdSelectionPreference = Nil,
                prefetchContractKeys = Nil,
              ),
              timeouts.default.asFiniteApproximation,
            )
            .map(handleCommandResult(operation))
        )
      )
    }
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
    LifeCycle.close(partyReplicator, retrySubmitter, ledgerClient)(logger)

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
      sourceParticipantId: ParticipantId,
      serial: PositiveInt,
      participantPermission: ParticipantPermission,
  )
  private def userId = "PartyReplicationAdminWorkflow"

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

  @VisibleForTesting
  lazy val proposalTemplatePkgName: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.partyreplication.PartyReplicationProposal.TEMPLATE_ID
    )
  lazy val agreementTemplatePkgName: Identifier =
    apiIdentifierFromJavaIdentifier(
      M.partyreplication.PartyReplicationAgreement.TEMPLATE_ID
    )

  private def isTemplatePartyReplicationRelated(id: Identifier) =
    id == proposalTemplate || id == agreementTemplate
}
