// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import com.daml.daml_lf_dev.DamlLf
import com.daml.error.GrpcStatuses
import com.daml.lf.CantonOnly
import com.daml.lf.data.{Bytes, ImmArray}
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.participant.state.v2.{
  CompletionInfo,
  DivulgedContract,
  Reassignment,
  ReassignmentInfo,
  TransactionMeta,
  Update,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.protocol.ProcessingSteps
import com.digitalasset.canton.protocol.{
  LfCommittedTransaction,
  LfContractId,
  LfHash,
  LfNode,
  LfNodeCreate,
  LfNodeExercises,
  LfNodeId,
  LfTemplateId,
  LfVersionedTransaction,
  SourceDomainId,
  TargetDomainId,
  TransferId,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.{
  LedgerConfiguration,
  LedgerParticipantId,
  LedgerSubmissionId,
  LedgerTransactionId,
  LfPartyId,
  LfTimestamp,
  LfWorkflowId,
}
import com.google.rpc.status.Status as RpcStatus
import io.scalaland.chimney.Transformer
import io.scalaland.chimney.dsl.*
import monocle.macros.syntax.lens.*

import scala.collection.immutable.HashMap

/** This a copy of [[com.digitalasset.canton.ledger.participant.state.v2.Update]].
  * Refer to [[com.digitalasset.canton.ledger.participant.state.v2.Update]] documentation for more information.
  */
sealed trait LedgerSyncEvent extends Product with Serializable with PrettyPrinting {
  def description: String
  def recordTime: LfTimestamp
  def toDamlUpdate: Option[Update]

  def setTimestamp(timestamp: LfTimestamp): LedgerSyncEvent =
    this match {
      case ta: LedgerSyncEvent.TransactionAccepted =>
        ta.copy(
          recordTime = timestamp,
          transactionMeta = ta.transactionMeta.copy(submissionTime = timestamp),
        )
      case ev: LedgerSyncEvent.PublicPackageUpload => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PublicPackageUploadRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.CommandRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PartyAddedToParticipant => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.PartyAllocationRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ConfigurationChanged => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ConfigurationChangeRejected => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.TransferredOut => ev.updateRecordTime(newRecordTime = timestamp)
      case ev: LedgerSyncEvent.TransferredIn => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ContractsAdded => ev.copy(recordTime = timestamp)
      case ev: LedgerSyncEvent.ContractsPurged => ev.copy(recordTime = timestamp)
    }
}

object LedgerSyncEvent {

  // TODO(i13313) Where relevant, the domain ID should be mandatory
  sealed trait WithDomainId extends LedgerSyncEvent {
    def domainId: Option[DomainId]
  }

  /** Produces a constant dummy transaction seed for transactions in which we cannot expose a seed. Essentially all of
    * them. TransactionMeta.submissionSeed can no longer be set to None starting with Daml 1.3
    */
  def noOpSeed: LfHash =
    LfHash.assertFromString("00" * LfHash.underlyingHashLength)

  final case class ConfigurationChanged(
      recordTime: LfTimestamp,
      submissionId: LedgerSubmissionId,
      participantId: LedgerParticipantId,
      newConfiguration: LedgerConfiguration,
  ) extends LedgerSyncEvent {
    override def description: String =
      s"Configuration change '$submissionId' from participant '$participantId' accepted with configuration: $newConfiguration"

    override def pretty: Pretty[ConfigurationChanged] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("newConfiguration", _.newConfiguration),
      )
    override def toDamlUpdate: Option[Update] = Some(
      this.transformInto[Update.ConfigurationChanged]
    )
  }

  final case class ConfigurationChangeRejected(
      recordTime: LfTimestamp,
      submissionId: LedgerSubmissionId,
      participantId: LedgerParticipantId,
      proposedConfiguration: LedgerConfiguration,
      rejectionReason: String,
  ) extends LedgerSyncEvent {
    override def description: String = {
      s"Configuration change '$submissionId' from participant '$participantId' was rejected: $rejectionReason"
    }

    override def pretty: Pretty[ConfigurationChangeRejected] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("rejectionReason", _.rejectionReason.doubleQuoted),
        param("proposedConfiguration", _.proposedConfiguration),
      )
    override def toDamlUpdate: Option[Update] = Some(
      this.transformInto[Update.ConfigurationChangeRejected]
    )
  }

  final case class PartyAddedToParticipant(
      party: LfPartyId,
      displayName: String,
      participantId: LedgerParticipantId,
      recordTime: LfTimestamp,
      submissionId: Option[LedgerSubmissionId],
  ) extends LedgerSyncEvent {
    override def description: String =
      s"Add party '$party' to participant"

    override def pretty: Pretty[PartyAddedToParticipant] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId.showValueOrNone),
        param("party", _.party),
        param("displayName", _.displayName.singleQuoted),
      )
    override def toDamlUpdate: Option[Update] = Some(
      this.transformInto[Update.PartyAddedToParticipant]
    )
  }

  final case class PartyAllocationRejected(
      submissionId: LedgerSubmissionId,
      participantId: LedgerParticipantId,
      recordTime: LfTimestamp,
      rejectionReason: String,
  ) extends LedgerSyncEvent {
    override val description: String =
      s"Request to add party to participant with submissionId '$submissionId' failed"

    override def pretty: Pretty[PartyAllocationRejected] =
      prettyOfClass(
        param("participantId", _.participantId),
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("rejectionReason", _.rejectionReason.doubleQuoted),
      )

    override def toDamlUpdate: Option[Update] = Some(
      this.transformInto[Update.PartyAllocationRejected]
    )
  }

  final case class PublicPackageUpload(
      archives: List[DamlLf.Archive],
      sourceDescription: Option[String],
      recordTime: LfTimestamp,
      submissionId: Option[LedgerSubmissionId],
  ) extends LedgerSyncEvent {
    override def description: String =
      s"Public package upload: ${archives.map(_.getHash).mkString(", ")}"

    override def pretty: Pretty[PublicPackageUpload] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId.showValueOrNone),
        param("sourceDescription", _.sourceDescription.map(_.doubleQuoted).showValueOrNone),
        paramWithoutValue("archives"),
      )

    override def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.PublicPackageUpload])
  }

  final case class PublicPackageUploadRejected(
      submissionId: LedgerSubmissionId,
      recordTime: LfTimestamp,
      rejectionReason: String,
  ) extends LedgerSyncEvent {
    override def description: String =
      s"Public package upload rejected, correlationId=$submissionId reason='$rejectionReason'"

    override def pretty: Pretty[PublicPackageUploadRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("submissionId", _.submissionId),
        param("rejectionReason", _.rejectionReason.doubleQuoted),
      )

    override def toDamlUpdate: Option[Update] = Some(
      this.transformInto[Update.PublicPackageUploadRejected]
    )
  }

  final case class TransactionAccepted(
      completionInfoO: Option[CompletionInfo],
      transactionMeta: TransactionMeta,
      transaction: CommittedTransaction,
      transactionId: LedgerTransactionId,
      recordTime: LfTimestamp,
      divulgedContracts: List[DivulgedContract],
      blindingInfoO: Option[BlindingInfo],
      hostedWitnesses: List[LfPartyId],
      contractMetadata: Map[LfContractId, Bytes],
  ) extends LedgerSyncEvent.WithDomainId {
    override def description: String = s"Accept transaction $transactionId"

    override def pretty: Pretty[TransactionAccepted] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("transactionId", _.transactionId),
        paramIfDefined("completion", _.completionInfoO),
        param("transactionMeta", _.transactionMeta),
        indicateOmittedFields,
      )
    override def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.TransactionAccepted])

    override def domainId: Option[DomainId] = transactionMeta.optDomainId
  }

  private def mkTx(nodes: Iterable[LfNode]): LfCommittedTransaction = {
    val nodeIds = LazyList.from(0).map(LfNodeId)
    val txNodes = nodeIds.zip(nodes).toMap
    LfCommittedTransaction(
      CantonOnly.lfVersionedTransaction(
        nodes = txNodes,
        roots = ImmArray.from(nodeIds.take(txNodes.size)),
      )
    )
  }

  final case class ContractsAdded(
      transactionId: LedgerTransactionId,
      contracts: Seq[LfNodeCreate],
      domainId: DomainId,
      ledgerTime: LfTimestamp,
      recordTime: LfTimestamp,
      hostedWitnesses: Seq[LfPartyId],
      contractMetadata: Map[LfContractId, Bytes],
      workflowId: Option[LfWorkflowId],
  ) extends LedgerSyncEvent {
    override def description: String = s"Contracts added $transactionId"

    override def toDamlUpdate: Option[Update] = Some(
      Update.TransactionAccepted(
        completionInfoO = None,
        transactionMeta = TransactionMeta(
          ledgerEffectiveTime = ledgerTime,
          workflowId = workflowId,
          submissionTime = recordTime,
          submissionSeed = LedgerSyncEvent.noOpSeed,
          optUsedPackages = None,
          optNodeSeeds = None,
          optByKeyNodes = None,
          optDomainId = Option(domainId),
        ),
        transaction = mkTx(contracts),
        transactionId = transactionId,
        recordTime = recordTime,
        divulgedContracts = List.empty,
        blindingInfoO = None,
        hostedWitnesses = hostedWitnesses.toList,
        contractMetadata = contractMetadata,
      )
    )

    override def pretty: Pretty[ContractsAdded] =
      prettyOfClass(
        param("transactionId", _.transactionId),
        param("contracts", _.contracts.map(_.coid)),
        param("domainId", _.domainId),
        param("recordTime", _.recordTime),
        param("ledgerTime", _.ledgerTime),
        paramWithoutValue("hostedWitnesses"),
        paramWithoutValue("contractMetadata"),
      )
  }

  final case class ContractsPurged(
      transactionId: LedgerTransactionId,
      contracts: Seq[LfNodeExercises],
      domainId: DomainId,
      recordTime: LfTimestamp,
      hostedWitnesses: Seq[LfPartyId],
  ) extends LedgerSyncEvent {
    override def description: String = s"Contracts purged $transactionId"

    override def toDamlUpdate: Option[Update] = Some(
      Update.TransactionAccepted(
        completionInfoO = None,
        transactionMeta = TransactionMeta(
          ledgerEffectiveTime = recordTime,
          workflowId = None,
          submissionTime = recordTime,
          submissionSeed = LedgerSyncEvent.noOpSeed,
          optUsedPackages = None,
          optNodeSeeds = None,
          optByKeyNodes = None,
          optDomainId = Option(domainId),
        ),
        transaction = mkTx(contracts),
        transactionId = transactionId,
        recordTime = recordTime,
        divulgedContracts = List.empty,
        blindingInfoO = None,
        hostedWitnesses = hostedWitnesses.toList,
        contractMetadata = Map.empty,
      )
    )

    override def pretty: Pretty[ContractsPurged] =
      prettyOfClass(
        param("transactionId", _.transactionId),
        paramWithoutValue("contractIds"),
        param("domainId", _.domainId),
        param("recordTime", _.recordTime),
        paramWithoutValue("hostedWitnesses"),
        paramWithoutValue("contractMetadata"),
      )
  }

  final case class CommandRejected(
      recordTime: LfTimestamp,
      completionInfo: CompletionInfo,
      reasonTemplate: CommandRejected.FinalReason,
      kind: ProcessingSteps.RequestType.Values,
      domainId: Option[DomainId],
  ) extends LedgerSyncEvent.WithDomainId {
    override def description: String =
      s"Reject command ${completionInfo.commandId}${if (definiteAnswer)
          " (definite answer)"}: ${reasonTemplate.message}"

    def definiteAnswer: Boolean = reasonTemplate.definiteAnswer

    override def pretty: Pretty[CommandRejected] =
      prettyOfClass(
        param("recordTime", _.recordTime),
        param("completionInfo", _.completionInfo),
        param("reason", _.reasonTemplate),
        param("kind", _.kind),
        paramIfDefined("domainId", _.domainId),
      )

    override def toDamlUpdate: Option[Update] = Some(this.transformInto[Update.CommandRejected])
  }

  object CommandRejected {
    final case class FinalReason(status: RpcStatus) extends PrettyPrinting {
      def message: String = status.message
      def code: Int = status.code
      def definiteAnswer: Boolean = GrpcStatuses.isDefiniteAnswer(status)
      override def pretty: Pretty[FinalReason] = prettyOfClass(
        unnamedParam(_.status)
      )
    }
    object FinalReason {
      implicit val toDamlFinalReason
          : Transformer[FinalReason, Update.CommandRejected.RejectionReasonTemplate] =
        (finalReason: FinalReason) => Update.CommandRejected.FinalReason(finalReason.status)
    }
  }

  sealed trait TransferEvent extends LedgerSyncEvent.WithDomainId {
    def transferId: TransferId
    def sourceDomain: SourceDomainId = transferId.sourceDomain
    def targetDomain: TargetDomainId
    def kind: String
    def isTransferringParticipant: Boolean
    def workflowId: Option[LfWorkflowId]
    def contractId: LfContractId

    override def description: String =
      s"transferred-$kind $contractId from $sourceDomain to $targetDomain"
  }

  /** Signal the transfer-out of a contract from source to target domain.
    *
    * @param updateId              Uniquely identifies the update.
    * @param optCompletionInfo     Must be provided for the participant that submitted the transfer-out.
    * @param submitter             The partyId of the transfer submitter, unless the operation was performed offline.
    * @param transferId            Uniquely identifies the transfer. See [[com.digitalasset.canton.protocol.TransferId]].
    * @param contractId            The contract-id that's being transferred-out.
    * @param templateId            The template-id of the contract that's being transferred-out.
    * @param targetDomain          The target domain of the transfer.
    * @param transferInExclusivity The timestamp of the timeout before which only the submitter can initiate the
    *                              corresponding transfer-in. Must be provided for the participant that submitted the transfer-out.
    * @param workflowId            The workflowId specified by the submitter in the transfer command.
    * @param isTransferringParticipant True if the participant is transferring.
    *                                  Note: false if the data comes from an old serialized event
    * @param transferCounter        Long.MinValue
    */
  final case class TransferredOut(
      updateId: LedgerTransactionId,
      optCompletionInfo: Option[CompletionInfo],
      submitter: Option[LfPartyId],
      contractId: LfContractId,
      templateId: Option[LfTemplateId],
      contractStakeholders: Set[LfPartyId],
      transferId: TransferId,
      targetDomain: TargetDomainId,
      transferInExclusivity: Option[LfTimestamp],
      workflowId: Option[LfWorkflowId],
      isTransferringParticipant: Boolean,
      hostedStakeholders: List[LfPartyId],
  ) extends TransferEvent {

    override def recordTime: LfTimestamp = transferId.transferOutTimestamp.underlying

    override def domainId: Option[DomainId] = Option(sourceDomain.id)

    def updateRecordTime(newRecordTime: LfTimestamp): TransferredOut =
      this.focus(_.transferId.transferOutTimestamp).replace(CantonTimestamp(newRecordTime))

    override def kind: String = "out"

    override def pretty: Pretty[TransferredOut] = prettyOfClass(
      param("updateId", _.updateId),
      paramIfDefined("completionInfo", _.optCompletionInfo),
      param("submitter", _.submitter),
      param("transferId", _.transferId),
      param("contractId", _.contractId),
      paramIfDefined("templateId", _.templateId),
      param("target", _.targetDomain),
      paramIfDefined("transferInExclusivity", _.transferInExclusivity),
      paramIfDefined("workflowId", _.workflowId),
    )

    def toDamlUpdate: Option[Update] = Some(
      Update.ReassignmentAccepted(
        optCompletionInfo = optCompletionInfo,
        workflowId = workflowId,
        updateId = updateId,
        recordTime = recordTime,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = transferId.sourceDomain,
          targetDomain = targetDomain,
          submitter = submitter,
          reassignmentCounter = Long.MinValue,
          hostedStakeholders = hostedStakeholders,
          unassignId = transferId.transferOutTimestamp,
        ),
        reassignment = Reassignment.Unassign(
          contractId = contractId,
          templateId = templateId.getOrElse(
            throw new IllegalStateException(
              s"templateId should not be empty in transfer-id: $transferId"
            )
          ),
          stakeholders = contractStakeholders.toList,
          assignmentExclusivity = transferInExclusivity,
        ),
      )
    )
  }

  /**  Signal the transfer-in of a contract from the source domain to the target domain.
    *
    * @param updateId                  Uniquely identifies the update.
    * @param optCompletionInfo         Must be provided for the participant that submitted the transfer-in.
    * @param submitter                 The partyId of the transfer submitter, unless the operation is performed offline.
    * @param recordTime                The ledger-provided timestamp at which the contract was transferred in.
    * @param ledgerCreateTime          The ledger time of the transaction '''creating''' the contract
    * @param createNode                Denotes the creation of the contract being transferred-in.
    * @param contractMetadata          Contains contract metadata of the contract transferred assigned by the ledger implementation
    * @param transferId                Uniquely identifies the transfer. See [[com.digitalasset.canton.protocol.TransferId]].
    * @param targetDomain              The target domain of the transfer.
    * @param createTransactionAccepted We used to create a TransactionAccepted for the transferIn.
    *                                  This param is used to keep the same behavior.
    * @param workflowId                The workflowId specified by the submitter in the transfer command.
    * @param isTransferringParticipant True if the participant is transferring.
    *                                  Note: false if the data comes from an old serialized event
    * @param transferCounter           Long.MinValue
    */
  final case class TransferredIn(
      updateId: LedgerTransactionId,
      optCompletionInfo: Option[CompletionInfo],
      submitter: Option[LfPartyId],
      recordTime: LfTimestamp,
      ledgerCreateTime: LfTimestamp,
      createNode: LfNodeCreate,
      creatingTransactionId: LedgerTransactionId,
      contractMetadata: Bytes,
      transferId: TransferId,
      targetDomain: TargetDomainId,
      createTransactionAccepted: Boolean,
      workflowId: Option[LfWorkflowId],
      isTransferringParticipant: Boolean,
      hostedStakeholders: List[LfPartyId],
  ) extends TransferEvent {

    override def pretty: Pretty[TransferredIn] = prettyOfClass(
      param("updateId", _.updateId),
      param("ledgerCreateTime", _.ledgerCreateTime),
      paramIfDefined("optCompletionInfo", _.optCompletionInfo),
      param("submitter", _.submitter),
      param("recordTime", _.recordTime),
      param("transferId", _.transferId),
      param("target", _.targetDomain),
      paramWithoutValue("createNode"),
      paramWithoutValue("contractMetadata"),
      paramWithoutValue("createdEvent"),
      paramIfDefined("workflowId", _.workflowId),
    )

    override def kind: String = "in"

    override def contractId: LfContractId = createNode.coid

    override def domainId: Option[DomainId] = Option(targetDomain.id)

    /** Workaround to create an update for informing the ledger API server about a transferred-in contract.
      * Creates a TransactionAccepted event consisting of a single create action that creates the given contract.
      *
      * The transaction has the same ledger time and transaction id as the creation of the contract.
      */
    def asTransactionAccepted: Option[Update] =
      Option.when(createTransactionAccepted)(
        Update.TransactionAccepted(
          completionInfoO = optCompletionInfo,
          transactionMeta = TransactionMeta(
            ledgerEffectiveTime = ledgerCreateTime,
            workflowId = workflowId,
            submissionTime =
              recordTime, // TODO(M41): Upstream mismatch, replace with enter/leave view
            submissionSeed = LedgerSyncEvent.noOpSeed,
            optUsedPackages = None,
            optNodeSeeds = None,
            optByKeyNodes = None,
            optDomainId = Some(targetDomain.unwrap),
          ),
          transaction = LfCommittedTransaction(
            LfVersionedTransaction(
              version = createNode.version,
              nodes = HashMap((LfNodeId(0), createNode)),
              roots = ImmArray(LfNodeId(0)),
            )
          ),
          transactionId = updateId,
          recordTime = recordTime,
          divulgedContracts = Nil,
          blindingInfoO = None,
          hostedWitnesses = hostedStakeholders,
          contractMetadata = Map(createNode.coid -> contractMetadata),
        )
      )

    def toDamlUpdate: Option[Update] = Some(
      Update.ReassignmentAccepted(
        optCompletionInfo = optCompletionInfo,
        workflowId = workflowId,
        updateId = updateId,
        recordTime = recordTime,
        reassignmentInfo = ReassignmentInfo(
          sourceDomain = transferId.sourceDomain,
          targetDomain = targetDomain,
          submitter = submitter,
          reassignmentCounter = Long.MinValue,
          hostedStakeholders = hostedStakeholders,
          unassignId = transferId.transferOutTimestamp,
        ),
        reassignment = Reassignment.Assign(
          ledgerEffectiveTime = ledgerCreateTime,
          createNode = createNode,
          contractMetadata = contractMetadata,
        ),
      )
    )
  }
}
