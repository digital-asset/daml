// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.index.v1

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.ledger.api.domain.TransactionFilter

import scala.concurrent.Future

trait IndexService
    extends PackagesService
    with ConfigurationService
    with TransactionsService
    with IdentityService {

  //TODO: review these where are they used and what concerns they fulfill
  def getLedgerBeginning(): Future[Offset]

  def getLedgerEnd(): Future[Offset]

  //TODO gabor: why is this seperate from the transactions, isn't it a problem that they can be out of sync?
  def getLedgerRecordTimeStream(): Source[Time.Timestamp, NotUsed]

  def lookupActiveContract(
      submitter: Party,
      contractId: AbsoluteContractId
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  def getActiveContractSetSnapshot(
      filter: TransactionFilter
  ): Future[ActiveContractSetSnapshot]

  //TODO gabor: why the update, when is it used?
  def getActiveContractSetUpdates(
      beginAfter: Option[Offset],
      endAt: Option[Offset],
      filter: TransactionFilter
  ): Source[AcsUpdate, NotUsed]

  def getCompletions(
      beginAfter: Option[Offset],
      applicationId: ApplicationId,
      parties: List[Party]
  ): Source[CompletionEvent, NotUsed]
}
