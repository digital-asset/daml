// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import com.daml.auth.TokenHolder
import com.daml.ledger.api.domain
import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.TransactionTree
import com.daml.ledger.api.v1.transaction_filter.{Filters, TransactionFilter}
import com.daml.ledger.client.LedgerClient

import scala.concurrent.{ExecutionContext, Future}

object LedgerUtils {

  /** Fetch all known parties from the ledger if requested by the given PartyConfig.
    */
  def getAllParties(
      client: LedgerClient,
      token: Option[TokenHolder],
      config: PartyConfig,
  )(implicit ec: ExecutionContext): Future[Seq[Party]] = {
    if (config.allParties) {
      val tokenString = token.flatMap(_.token)
      def fromDetails(details: Seq[domain.PartyDetails]): Seq[Party] =
        Party.subst(details.map(_.party))
      client.partyManagementClient.listKnownParties(tokenString).map(fromDetails)
    } else {
      Future.successful(config.parties)
    }
  }

  /** Fetch the active contract set from the ledger.
    *
    * @param parties Fetch the ACS for these parties.
    * @param offset Fetch the ACS as of this ledger offset.
    */
  def getACS(
      client: LedgerClient,
      parties: Seq[Party],
      offset: LedgerOffset,
  )(implicit
      mat: Materializer
  ): Future[Map[ContractId, CreatedEvent]] = {
    val ledgerBegin = LedgerOffset(
      LedgerOffset.Value.Boundary(LedgerOffset.LedgerBoundary.LEDGER_BEGIN)
    )
    if (offset == ledgerBegin) {
      Future.successful(Map.empty)
    } else {
      client.transactionClient
        .getTransactions(ledgerBegin, Some(offset), filter(Party.unsubst(parties)), verbose = true)
        .runFold(Map.empty[ContractId, CreatedEvent]) { case (acs, tx) =>
          tx.events.foldLeft(acs) { case (acs, ev) =>
            ev.event match {
              case Event.Empty => acs
              case Event.Created(value) => acs + (ContractId(value.contractId) -> value)
              case Event.Archived(value) => acs - ContractId(value.contractId)
            }
          }
        }
    }
  }

  /** Fetch a range of transaction trees from the ledger.
    *
    * @param parties Fetch transactions for these parties.
    * @param start Fetch transactions starting after this offset.
    * @param end Fetch transactions up to and including this offset.
    */
  def getTransactionTrees(
      client: LedgerClient,
      parties: Seq[Party],
      start: LedgerOffset,
      end: LedgerOffset,
  )(implicit
      mat: Materializer
  ): Future[Seq[TransactionTree]] = {
    if (start == end) {
      Future.successful(Seq.empty)
    } else {
      client.transactionClient
        .getTransactionTrees(start, Some(end), filter(Party.unsubst(parties)), verbose = true)
        .runWith(Sink.seq)
    }
  }

  private def filter(parties: Seq[String]): TransactionFilter =
    TransactionFilter(parties.map(p => p -> Filters()).toMap)
}
