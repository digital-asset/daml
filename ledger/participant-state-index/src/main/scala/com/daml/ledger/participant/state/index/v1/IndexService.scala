// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf.DamlLf.Archive
import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.concurrent.Future

trait IndexService {
  def listPackages(ledgerId: LedgerId): AsyncResult[List[PackageId]]
  def isPackageRegistered(ledgerId: LedgerId, packageId: PackageId): AsyncResult[Boolean]
  def getPackage(ledgerId: LedgerId, packageId: PackageId): AsyncResult[Option[Archive]]

  def getLedgerConfiguration(ledgerId: LedgerId): AsyncResult[Configuration]

  def getLedgerId(): Future[LedgerId]

  def getLedgerBeginning(ledgerId: LedgerId): AsyncResult[Offset]
  def getLedgerEnd(ledgerId: LedgerId): AsyncResult[Offset]

  def lookupActiveContract(
      ledgerId: LedgerId,
      contractId: AbsoluteContractId
  ): AsyncResult[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  def getActiveContractSetSnapshot(
      ledgerId: LedgerId,
      filter: TransactionFilter
  ): AsyncResult[ActiveContractSetSnapshot]

  def getActiveContractSetUpdates(
      ledgerId: LedgerId,
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter
  ): AsyncResult[Source[AcsUpdate, NotUsed]]

  // FIXME(JM): Cleaner name/types for this
  // FIXME(JM): Fold BlindingInfo into TransactionAccepted, or introduce
  // new type in IndexService?
  def getAcceptedTransactions(
      ledgerId: LedgerId,
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter
  ): AsyncResult[Source[(Offset, (TransactionAccepted, BlindingInfo)), NotUsed]]

  /*
  def getTransactionById(
      ledgerId: LedgerId,
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): AsyncResult[Option[TransactionAccepted]]
   */

  def getCompletions(
      ledgerId: LedgerId,
      beginAfter: Option[Offset],
      applicationId: ApplicationId,
      parties: List[Party]
  ): AsyncResult[Source[CompletionEvent, NotUsed]]
}

object IndexService {
  sealed trait Err
  object Err {

    final case class LedgerIdMismatch(expected: LedgerId, actual: LedgerId) extends Err

  }
}
