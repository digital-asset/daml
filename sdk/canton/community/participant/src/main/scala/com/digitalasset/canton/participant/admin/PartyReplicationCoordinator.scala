// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

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
import com.digitalasset.canton.CommandId
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.ledger.client.{LedgerClient, LedgerClientUtils}
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PartyReplicationCoordinator.*
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.participant.ledger.api.client.{
  CommandResult,
  CommandSubmitterWithRetry,
  LedgerConnection,
}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

/** Daml admin workflow for coordinating distributed party management operations across participants.
  */
class PartyReplicationCoordinator(
    ledgerClient: LedgerClient,
    participantId: ParticipantId,
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

  private[admin] type ContractIdS = String

  /** Have the target/current participant submit a Daml PartyReplication.ChannelProposal contract to agree on with the source participant.
    */
  def startPartyReplication(
      args: PartyReplicationArguments
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    val PartyReplicationArguments(
      ChannelId(CommandId(id)),
      partyId,
      sourceParticipantId,
      domainId,
    ) = args
    logger.info(
      s"Party Replication $id: Initiating replication of party $partyId from participant $sourceParticipantId over domain $domainId"
    )
    if (syncService.isActive()) {
      val startAtWatermark = NonNegativeInt.zero

      for {
        domainTopologyClient <- EitherT.fromOption[Future](
          syncService.syncCrypto.ips.forDomain(domainId),
          s"Unknown domain $domainId",
        )
        topologySnapshot = domainTopologyClient.headSnapshot
        // TODO(#20634): Make the selection of the sequencer more flexible, e.g. by letting
        //  the SP choose among a list of sequencers that the TP is connected to.
        //  As of now the closest approximation of connected sequencers is the list of transports
        //  in the sequencer client, but even that information is not directly available.
        //  Revisit this once the sequencer client health state exposes the connected sequencers.
        sequencerId <- EitherT.fromOptionF(
          topologySnapshot.sequencerGroup().map(_.flatMap(_.active.headOption)),
          s"No sequencer group for domain $domainId",
        )
        _ <- ensurePartyIsAuthorizedOnParticipants(
          partyId,
          sourceParticipantId,
          participantId,
          topologySnapshot,
        )
        // TODO(#20581): Extract ACS snapshot timestamp from PTP onboarding effective time
        //  Currently the domain topology client does not expose the low-level effective time.
        acsSnapshotTs = topologySnapshot.timestamp
        channelProposal = new M.partyreplication.ChannelProposal(
          sourceParticipantId.adminParty.toProtoPrimitive,
          participantId.adminParty.toProtoPrimitive,
          sequencerId.uid.toProtoPrimitive,
          new M.partyreplication.PartyReplicationMetadata(
            id,
            partyId.toProtoPrimitive,
            acsSnapshotTs.toInstant,
            startAtWatermark.value,
          ),
        )
        _ <- EitherT(
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
                domainId = domainId.toProtoPrimitive,
              ),
              timeouts.default.asFiniteApproximation,
            )
            .map(
              handleCommandResult(s"propose channel $id to replicate party")
            )
        )
      } yield ()
    } else {
      EitherT.rightT[Future, String](())
    }
  }

  override private[admin] def filters: TransactionFilter =
    // we can't filter by template id as we don't know when the admin workflow package is loaded
    LedgerConnection.transactionFilterByParty(Map(participantId.adminParty -> Seq.empty))

  override private[admin] def processTransaction(scalaTx: Transaction): Unit = {
    implicit val traceContext: TraceContext =
      LedgerClient.traceContextFromLedgerApi(scalaTx.traceContext)

    def shouldHandleChannelProposal(
        contract: M.partyreplication.ChannelProposal.Contract
    ) =
      contract.data.sourceParticipant == participantId.adminParty.toProtoPrimitive

    def shouldHandleChannelAgreement(contract: M.partyreplication.ChannelAgreement.Contract) =
      contract.data.targetParticipant == participantId.adminParty.toProtoPrimitive

    scalaTx.events
      .collect {
        case Event(Event.Event.Created(event))
            if event.templateId.exists(isTemplateChannelRelated) =>
          event
      }
      .foreach(
        createEventHandler(
          contract => {
            logger.info(
              s"Received channel proposal for party ${contract.data.payloadMetadata.partyId} on domain ${scalaTx.domainId}" +
                s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
            )
            if (shouldHandleChannelProposal(contract)) {
              processChannelProposalAtSourceParticipant(scalaTx.domainId, contract)
            }
          },
          contract => {
            logger.info(
              s"Received channel agreement for party ${contract.data.payloadMetadata.partyId} on domain ${scalaTx.domainId}" +
                s" from source participant ${contract.data.sourceParticipant} to target participant ${contract.data.targetParticipant}"
            )
            if (shouldHandleChannelAgreement(contract)) {
              processChannelAgreementAtTargetParticipant(contract)
            }
          },
        )
      )
  }

  private def processChannelProposalAtSourceParticipant(
      domain: String,
      contract: M.partyreplication.ChannelProposal.Contract,
  )(implicit traceContext: TraceContext): Unit = {
    val validationET = for {
      params <- EitherT.fromEither[Future](ChannelProposalParams.fromDaml(contract.data, domain))
      ChannelProposalParams(ts, partyId, targetParticipantId, sequencerId, domainId) = params
      domainTopologyClient <-
        EitherT.fromEither[Future](
          syncService.syncCrypto.ips.forDomain(domainId).toRight(s"Unknown domain $domainId")
        )
      // Insist that the topology snapshot is known by the source participant to
      // avoid unbounded wait by awaitSnapshot().
      _ <- EitherT.cond[Future](
        domainTopologyClient.snapshotAvailable(ts),
        (),
        s"Specified timestamp $ts is not yet available on participant $participantId and domain $domainId",
      )
      topologySnapshot <- EitherT.right(domainTopologyClient.awaitSnapshot(ts))
      sequencerIds <- EitherT.fromOptionF(
        topologySnapshot.sequencerGroup().map(_.map(_.active)),
        s"No sequencer group for domain $domainId",
      )
      _ <- EitherT.cond[Future](
        sequencerIds.contains(sequencerId),
        (),
        s"Sequencer $sequencerId is not active on domain $domainId",
      )
      _ <- ensurePartyIsAuthorizedOnParticipants(
        partyId,
        participantId,
        targetParticipantId,
        topologySnapshot,
      )
    } yield ()

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
        _ =>
          (
            contract.id.exerciseAccept().commands,
            s"channel-proposal-accept-${contract.data.payloadMetadata.id}",
          ),
      )
      (exercise, commandId) = acceptOrReject
      commandResult <- retrySubmitter.submitCommands(
        Commands(
          applicationId = applicationId,
          commandId = commandId,
          actAs = Seq(participantId.adminParty.toProtoPrimitive),
          commands = exercise.asScala.toSeq.map(LedgerClientUtils.javaCodegenToScalaProto),
          deduplicationPeriod =
            DeduplicationDuration(syncService.maxDeduplicationDuration.toProtoPrimitive),
          domainId = domain,
        ),
        timeouts.default.asFiniteApproximation,
      )
    } yield commandResult

    superviseBackgroundSubmission("Accept or reject channel proposal", commandResultF)
  }

  private def processChannelAgreementAtTargetParticipant(
      contract: M.partyreplication.ChannelAgreement.Contract
  )(implicit traceContext: TraceContext): Unit =
    logger.info(
      s"Target participant ${contract.data.targetParticipant} is ready to build party replication channel shared with " +
        s"source participant ${contract.data.sourceParticipant} via sequencer ${contract.data.sequencerUid}."
    )

  override private[admin] def processReassignment(scalaTx: Reassignment): Unit =
    if (
      scalaTx.event.assignedEvent.exists(
        _.createdEvent
          .exists(_.templateId.exists(isTemplateChannelRelated))
      ) ||
      scalaTx.event.unassignedEvent.exists(
        _.templateId.exists(isTemplateChannelRelated)
      )
    ) {
      implicit val traceContext: TraceContext =
        LedgerClient.traceContextFromLedgerApi(scalaTx.traceContext)
      // TODO(#20638): Should we archive unexpectedly reassigned channel contracts or only warn?
      logger.warn(
        s"Received unexpected reassignment of party replication related contract: ${scalaTx.event}"
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

    // TODO(#20636): Upon node restart or domain reconnect, archive previously created contracts
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
      submission: Future[CommandResult],
  )(implicit traceContext: TraceContext): Unit =
    futureSupervisor
      .supervised(operation)(submission)
      .foreach(handleCommandResult(operation))

  private def ensurePartyIsAuthorizedOnParticipants(
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      snapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = for {
    _ <- EitherT.cond[Future](
      sourceParticipantId != targetParticipantId,
      (),
      s"Source and target participants $sourceParticipantId cannot match",
    )
    activeParticipantsOfParty <- EitherT.right(
      snapshot.activeParticipantsOf(partyId.toLf).map(_.keySet)
    )
    _ <- EitherT.cond[Future](
      activeParticipantsOfParty.contains(sourceParticipantId),
      (),
      s"Party $partyId is not hosted by source participant $sourceParticipantId",
    )
    _ <- EitherT.cond[Future](
      activeParticipantsOfParty.contains(targetParticipantId),
      (),
      s"Party $partyId is not hosted by target participant $targetParticipantId",
    )
  } yield ()

  override def onClosed(): Unit =
    // Note that we can not time out requests nicely here on shutdown as the admin
    // server is closed first, which means that our requests will never
    // return properly on shutdown abort.
    Lifecycle.close(retrySubmitter, ledgerClient)(logger)

  private val retrySubmitter = new CommandSubmitterWithRetry(
    ledgerClient.commandService,
    clock,
    futureSupervisor,
    timeouts,
    loggerFactory,
    decideRetry = _ => None,
  )
}

object PartyReplicationCoordinator {
  final case class PartyReplicationArguments(
      id: ChannelId,
      partyId: PartyId,
      sourceParticipantId: ParticipantId,
      domainId: DomainId,
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

  private def applicationId = "PartyReplicationCoordinator"

  private def apiIdentifierFromJavaIdentifier(javaIdentifier: JavaIdentifier): Identifier =
    Identifier(
      packageId = javaIdentifier.getPackageId,
      moduleName = javaIdentifier.getModuleName,
      entityName = javaIdentifier.getEntityName,
    )

  @VisibleForTesting
  lazy val channelProposalTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(M.partyreplication.ChannelProposal.TEMPLATE_ID)
  lazy val channelAgreementTemplate: Identifier =
    apiIdentifierFromJavaIdentifier(M.partyreplication.ChannelAgreement.TEMPLATE_ID)

  private def isTemplateChannelRelated(id: Identifier) =
    id == channelProposalTemplate || id == channelAgreementTemplate
}
