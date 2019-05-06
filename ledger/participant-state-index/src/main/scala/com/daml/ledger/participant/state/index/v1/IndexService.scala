// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.concurrent.Future

trait IndexService {
  def listPackages(): Future[List[PackageId]]
  def isPackageRegistered(packageId: PackageId): Future[Boolean]
  def getPackage(packageId: PackageId): Future[Option[Archive]]

  def getLedgerConfiguration(): Future[Configuration]

  def getLedgerId(): Future[LedgerId]

  def getLedgerBeginning(): Future[Offset]
  def getLedgerEnd(): Future[Offset]

  def getLedgerRecordTimeStream(): Source[Time.Timestamp, NotUsed]

  def lookupActiveContract(
      contractId: AbsoluteContractId
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  def getActiveContractSetSnapshot(
      filter: TransactionFilter
  ): Future[ActiveContractSetSnapshot]

  def getActiveContractSetUpdates(
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter
  ): Source[AcsUpdate, NotUsed]

  // FIXME(JM): Cleaner name/types for this
  // FIXME(JM): Fold BlindingInfo into TransactionAccepted, or introduce
  // new type in IndexService?
  def getAcceptedTransactions(
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter
  ): Source[(Offset, (TransactionAccepted, BlindingInfo)), NotUsed]

  /*
  def getTransactionById(
      ,
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[TransactionAccepted]]
   */

  def getCompletions(
      beginAfter: Option[Offset],
      applicationId: ApplicationId,
      parties: List[Party]
  ): Source[CompletionEvent, NotUsed]
}
