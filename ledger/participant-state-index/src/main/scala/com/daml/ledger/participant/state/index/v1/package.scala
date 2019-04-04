// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref.Identifier
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.concurrent.Future

package object v1 {
  import com.daml.ledger.participant.state.v1._

  type TransactionAccepted = Update.TransactionAccepted
  type AsyncResult[T] = Future[Either[IndexService.Err, T]]

  /** The ledger offset.
    * The opaque update identifiers are mapped to ledger offsets in the index service.
    * This provides ordering and efficient range queries, while leaving the underlying
    * state implementation free to choose its own representation for update identifiers.
    */
  type Offset = Long

  /** ACS event identifier */
  type EventId = String

  /** The index identifier uniquely identifies the index. Since the index may include
    * events that are not committed to the ledger, an index that has been rebuilt from
    * the ledger may use offsets that are incompatible with an earlier index generation.
    * This is why all methods in the index service include the unique index id and reject
    * requests with mismatching index ids.
    *
    * FIXME(JM): Use "IndexId" as "LedgerId", and expose an endpoint here that gives
    * StateId from it?
    */
  type IndexId = String

  trait IndexService {
    def listPackages(indexId: IndexId): AsyncResult[List[PackageId]]
    def isPackageRegistered(indexId: IndexId, packageId: PackageId): AsyncResult[Boolean]
    def getPackage(indexId: IndexId, packageId: PackageId): AsyncResult[Option[Archive]]

    def getLedgerConfiguration(indexId: IndexId): AsyncResult[Configuration]

    def getCurrentIndexId(): Future[IndexId]
    def getCurrentStateId(): Future[StateId]

    def getLedgerBeginning(indexId: IndexId): AsyncResult[Offset]
    def getLedgerEnd(indexId: IndexId): AsyncResult[Offset]

    def lookupActiveContract(
        indexId: IndexId,
        contractId: AbsoluteContractId
    ): AsyncResult[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

    def getActiveContractSetSnapshot(
        indexId: IndexId,
        filter: TransactionFilter
    ): AsyncResult[ActiveContractSetSnapshot]

    def getActiveContractSetUpdates(
        indexId: IndexId,
        beginAfter: Option[Offset],
        endAt: Option[Offset],
        filter: TransactionFilter
    ): AsyncResult[Source[AcsUpdate, NotUsed]]

    // FIXME(JM): Cleaner name/types for this
    // FIXME(JM): Fold BlindingInfo into TransactionAccepted, or introduce
    // new type in IndexService?
    def getAcceptedTransactions(
        indexId: IndexId,
        beginAfter: Option[Offset],
        endAt: Option[Offset],
        filter: TransactionFilter
    ): AsyncResult[Source[(Offset, (TransactionAccepted, BlindingInfo)), NotUsed]]

    /*
    def getTransactionById(
        indexId: IndexId,
        transactionId: TransactionId,
        requestingParties: Set[Party]
    ): AsyncResult[Option[TransactionAccepted]]
     */

    def getCompletions(
        indexId: IndexId,
        beginAfter: Option[Offset],
        applicationId: String,
        parties: List[String]
    ): AsyncResult[Source[CompletionEvent, NotUsed]]
  }

  object IndexService {
    sealed trait Err
    object Err {

      final case class IndexIdMismatch(expected: IndexId, actual: IndexId) extends Err

    }
  }

  final case class AcsUpdate(
      optSubmitterInfo: Option[SubmitterInfo],
      offset: Offset,
      transactionMeta: TransactionMeta,
      transactionId: String,
      events: List[AcsUpdateEvent]
  )

  sealed trait AcsUpdateEvent extends Product with Serializable
  object AcsUpdateEvent {
    final case class Create(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Identifier,
        argument: Value.VersionedValue[Value.AbsoluteContractId],
        // TODO(JM,SM): understand witnessing parties
        stakeholders: List[Party],
    ) extends AcsUpdateEvent

    final case class Archive(
        eventId: EventId,
        contractId: Value.AbsoluteContractId,
        templateId: Identifier,
        // TODO(JM,SM): understand witnessing parties
        stakeholders: List[Party],
    ) extends AcsUpdateEvent
  }

  sealed trait CompletionEvent extends Product with Serializable {
    def offset: Offset
  }
  object CompletionEvent {
    final case class Checkpoint(offset: Offset, recordTime: Timestamp) extends CompletionEvent
    final case class CommandAccepted(offset: Offset, commandId: String) extends CompletionEvent
    final case class CommandRejected(offset: Offset, commandId: String, reason: RejectionReason)
        extends CompletionEvent
  }

  final case class ActiveContractSetSnapshot(
      takenAt: Offset,
      activeContracts: Source[(WorkflowId, AcsUpdateEvent.Create), NotUsed]
  )

}
