// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.checked
import com.digitalasset.canton.config.CantonRequireTypes.String36
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ParticipantPruningStore
import com.digitalasset.canton.participant.store.ParticipantPruningStore.ParticipantPruningStatus
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.GetResult

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class DbParticipantPruningStore(
    name: String36,
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ParticipantPruningStore
    with DbStore {

  import storage.api.*

  override def markPruningStarted(
      upToInclusive: Offset
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val upsertQuery = storage.profile match {
      case _: Postgres =>
        sqlu"""insert into par_pruning_operation as po (name, started_up_to_inclusive, completed_up_to_inclusive)
                 values ($name, $upToInclusive, null)
                 on conflict (name) do
                   update set started_up_to_inclusive = $upToInclusive
                   where po.started_up_to_inclusive is null or po.started_up_to_inclusive < $upToInclusive"""
      case _: H2 =>
        sqlu"""merge into par_pruning_operation using dual on (name = $name)
                 when matched and (started_up_to_inclusive is null or started_up_to_inclusive < $upToInclusive) then
                   update set started_up_to_inclusive = $upToInclusive
                 when not matched then
                   insert (name, started_up_to_inclusive, completed_up_to_inclusive)
                   values ($name, $upToInclusive, null)"""
    }

    storage.update_(upsertQuery, functionFullName)
  }

  override def markPruningDone(
      upToInclusive: Offset
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage.update_(
      sqlu"""update par_pruning_operation set completed_up_to_inclusive = $upToInclusive
                       where name = $name and (completed_up_to_inclusive is null or completed_up_to_inclusive < $upToInclusive)""",
      functionFullName,
    )

  private implicit val readParticipantPruningStatus: GetResult[ParticipantPruningStatus] =
    GetResult { r =>
      val started = r.<<[Option[Offset]]
      val completed = r.<<[Option[Offset]]
      ParticipantPruningStatus(started, completed)
    }

  override def pruningStatus()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ParticipantPruningStatus] =
    for {
      statusO <- storage.query(
        sql"""select started_up_to_inclusive, completed_up_to_inclusive from par_pruning_operation
               where name = $name""".as[ParticipantPruningStatus].headOption,
        functionFullName,
      )
    } yield statusO.getOrElse(ParticipantPruningStatus(None, None))
}

// Wrapper for DbParticipantPruningStore that contains a simple cache for fetching ParticipantPruningStatus
final class DbParticipantPruningStoreCached(
    underlying: DbParticipantPruningStore,
    initialStatus: ParticipantPruningStatus,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ParticipantPruningStore
    with NamedLogging {

  private val statusCache =
    new AtomicReference[Option[ParticipantPruningStatus]](Some(initialStatus))

  private def updateCache(
      f: ParticipantPruningStatus => ParticipantPruningStatus
  )(implicit traceContext: TraceContext): Try[Unit] => FutureUnlessShutdown[Unit] = {
    case Success(_) =>
      // attempt to recover status first
      pruningStatus()
        .map(_ => statusCache.updateAndGet(_.map(f)).discard)
        // failure to setting the status cache does not fail the result (this only happens if the status cache is reset,
        // and attempt to recover from persistence failed, therefore remains reset)
        .transformWith(_ => FutureUnlessShutdown.unit)

    case Failure(throwable) =>
      // on failure the cache is reset, we cannot be sure about what changed
      statusCache.set(None)
      // attempt to recover cache from persistence
      pruningStatus()
        .transformWithHandledAborted(
          // independently of the result, the original error is returned
          _ => FutureUnlessShutdown.failed(throwable)
        )

  }

  override def markPruningStarted(upToInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying
      .markPruningStarted(upToInclusive)
      .transformWithHandledAborted(updateCache {
        case ParticipantPruningStatus(startedO, completedO) if Option(upToInclusive) > startedO =>
          ParticipantPruningStatus(Some(upToInclusive), completedO)
        case current => current
      })

  override def markPruningDone(upToInclusive: Offset)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    underlying
      .markPruningDone(upToInclusive)
      .transformWithHandledAborted(updateCache {
        case ParticipantPruningStatus(startedO, completedO) if Option(upToInclusive) > completedO =>
          ParticipantPruningStatus(startedO, Some(upToInclusive))
        case current => current
      })

  override def pruningStatus()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ParticipantPruningStatus] = statusCache.get() match {
    case Some(status) => FutureUnlessShutdown.pure(status)
    case None =>
      underlying.pruningStatus().map { status =>
        checked(
          statusCache
            .updateAndGet {
              case Some(newStatus) =>
                Some(newStatus) // in case updated in the meantime we prefer that
              case None => Some(status) // if still unset, we take the one from persistence
            }
            .getOrElse(throw new IllegalStateException("should never happen"))
        )
      }
  }

  override def close(): Unit = underlying.close()

  @VisibleForTesting
  def pruningStatusCached(): Option[ParticipantPruningStatus] = statusCache.get()

}
