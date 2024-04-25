// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store.db

import cats.data.EitherT
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.sequencing.protocol.MessageId
import com.digitalasset.canton.store.SavePendingSendError.MessageIdAlreadyTracked
import com.digitalasset.canton.store.{SavePendingSendError, SendTrackerStore}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

class DbSendTrackerStore_Unused(
    override protected val storage: DbStorage,
    client: SequencerClientDiscriminator,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends SendTrackerStore
    with DbStore {

  import storage.api.*

  override def savePendingSend(messageId: MessageId, maxSequencingTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SavePendingSendError, Unit] = {
    for {
      rowsUpdated <- EitherT.right(
        storage.update(
          storage.profile match {
            case _: DbStorage.Profile.Oracle =>
              sqlu"""insert
                       /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( sequencer_client_pending_sends ( message_id, client ) ) */
                       into sequencer_client_pending_sends (client, message_id, max_sequencing_time)
                       values ($client, $messageId, $maxSequencingTime)"""
            case _ =>
              sqlu"""insert into sequencer_client_pending_sends (client, message_id, max_sequencing_time)
                       values ($client, $messageId, $maxSequencingTime)
                       on conflict do nothing"""
          },
          operationName = s"${this.getClass}: save pending send",
        )
      )
      _ <-
        if (rowsUpdated == 1) EitherT.rightT[Future, SavePendingSendError](())
        else {
          // No row has been inserted.
          // Check whether the existing row coincides with the row to be inserted and fail, if not.
          EitherT(for {
            existingMaxSequencingTimeO <- storage.query(
              sql"""select max_sequencing_time from sequencer_client_pending_sends
                    where client = $client and message_id = $messageId"""
                .as[CantonTimestamp]
                .headOption,
              functionFullName,
            )
          } yield {
            Either.cond(
              existingMaxSequencingTimeO.contains(maxSequencingTime),
              (),
              MessageIdAlreadyTracked: SavePendingSendError,
            )
          })
        }
    } yield ()
  }

  override def fetchPendingSends(implicit
      traceContext: TraceContext
  ): Future[Map[MessageId, CantonTimestamp]] = {
    for {
      items <- storage.query(
        sql"select message_id, max_sequencing_time from sequencer_client_pending_sends where client = $client"
          .as[(MessageId, CantonTimestamp)],
        functionFullName,
      )
    } yield items.toMap
  }

  override def removePendingSend(
      messageId: MessageId
  )(implicit traceContext: TraceContext): Future[Unit] = {
    storage.update_(
      sqlu"delete from sequencer_client_pending_sends where client = $client and message_id = $messageId",
      functionFullName,
    )
  }

}
