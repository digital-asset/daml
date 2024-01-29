// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.script.export

import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Sink
import com.daml.auth.TokenHolder
import com.digitalasset.canton.ledger.api.domain
import com.daml.ledger.api.refinements.ApiTypes.{ContractId, Party}
import com.daml.ledger.api.v2.update_service.{GetUpdatesResponse, GetUpdateTreesResponse}
import com.daml.ledger.api.v1.event.CreatedEvent
import com.daml.ledger.api.v1.event.Event.Event
import com.daml.ledger.api.v2.participant_offset.ParticipantOffset
import com.daml.ledger.api.v2.transaction.TransactionTree
import com.daml.ledger.api.v2.transaction_filter.TransactionFilter
import com.daml.ledger.api.v1.transaction_filter.Filters
import com.digitalasset.canton.ledger.client.LedgerClient

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

  // TODO: Is there some way to inline this into the pattern match? Can't do (_.update.transaction) inline
  private val getUpdatesResponseTransaction = (_: GetUpdatesResponse).update.transaction

  /** Fetch the active contract set from the ledger.
    * Warning: Ignores reassignments
    *
    * @param parties Fetch the ACS for these parties.
    * @param offset Fetch the ACS as of this ledger offset.
    */
  def getACS(
      client: LedgerClient,
      parties: Seq[Party],
      offset: ParticipantOffset,
  )(implicit
      mat: Materializer
  ): Future[Map[ContractId, CreatedEvent]] = {
    val participantBegin =
      ParticipantOffset().withBoundary(ParticipantOffset.ParticipantBoundary.PARTICIPANT_BEGIN)
    if (offset == participantBegin) {
      Future.successful(Map.empty)
    } else {
      client.v2.updateService
        .getUpdatesSource(
          begin = participantBegin,
          end = Some(offset),
          filter = filter(Party.unsubst(parties)),
          verbose = true,
        )
        .runFold(Map.empty[ContractId, CreatedEvent]) {
          case (acs, getUpdatesResponseTransaction.unlift(tx)) =>
            tx.events.foldLeft(acs) { case (acs, ev) =>
              ev.event match {
                case Event.Empty => acs
                case Event.Created(value) => acs + (ContractId(value.contractId) -> value)
                case Event.Archived(value) => acs - ContractId(value.contractId)
              }
            }
          case (acs, _) => acs
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
      start: ParticipantOffset,
      end: ParticipantOffset,
  )(implicit
      mat: Materializer,
      ec: ExecutionContext,
  ): Future[Seq[TransactionTree]] = {
    if (start == end) {
      Future.successful(Seq.empty)
    } else {
      client.v2.updateService
        .getUpdateTreesSource(
          begin = start,
          end = Some(end),
          filter = filter(Party.unsubst(parties)),
          verbose = true,
        )
        .runWith(Sink.seq)
        .map(
          _.map(_.update)
            .collect { case GetUpdateTreesResponse.Update.TransactionTree(tree) =>
              tree
            }
        )
    }
  }

  private def filter(parties: Seq[String]): TransactionFilter =
    TransactionFilter(parties.map(p => p -> Filters()).toMap)
}
