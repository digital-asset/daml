// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.ResourceLimits
import com.digitalasset.canton.participant.store.ParticipantSettingsStore
import com.digitalasset.canton.participant.store.ParticipantSettingsStore.Settings
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import slick.jdbc.{GetResult, SetParameter}
import slick.sql.SqlAction

import scala.concurrent.{ExecutionContext, Future}

class DbParticipantSettingsStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends ParticipantSettingsStore
    with DbStore {

  private val client = 0 // dummy field used to enforce at most one row in the db

  private val executionQueue = new SimpleExecutionQueue(
    "participant-setting-store-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  import storage.api.*
  import storage.converters.*

  private implicit val readSettings: GetResult[Settings] = GetResult { r =>
    val maxInflightValidationRequests = r.<<[Option[NonNegativeInt]]
    val maxSubmissionRate = r.<<[Option[NonNegativeInt]]
    val maxDedupDuration = r.<<[Option[NonNegativeFiniteDuration]]
    val maxSubmissionBurstFactor = r.<<[PositiveDouble]
    Settings(
      ResourceLimits(
        maxInflightValidationRequests = maxInflightValidationRequests,
        maxSubmissionRate = maxSubmissionRate,
        maxSubmissionBurstFactor = maxSubmissionBurstFactor,
      ),
      maxDedupDuration,
    )
  }

  override def refreshCache()(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    executionQueue.execute(
      {
        for {
          settingsO <- storage.query(
            sql"select max_infight_validation_requests, max_submission_rate, max_deduplication_duration, max_submission_burst_factor from par_settings"
              .as[Settings]
              .headOption,
            functionFullName,
          )
          settings = settingsO.getOrElse(Settings())

          // Configure default resource limits for any participant without persistent settings.
          // For participants with v2.3.0 or earlier, this will upgrade resource limits from "no limits" to the new default
          _ <- settingsO match {
            case None if storage.isActive =>
              val ResourceLimits(
                maxInflightValidationRequests,
                maxSubmissionRate,
                maxSubmissionBurstFactor,
              ) = ResourceLimits.default
              val query = storage.profile match {
                case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
                  sqlu"""insert into par_settings(client, max_infight_validation_requests, max_submission_rate, max_submission_burst_factor)
                           values($client, $maxInflightValidationRequests, $maxSubmissionRate, $maxSubmissionBurstFactor)
                           on conflict do nothing"""

                case _: DbStorage.Profile.Oracle =>
                  sqlu"""merge into par_settings using dual on (1 = 1)
                           when not matched then
                             insert(client, max_infight_validation_requests, max_submission_rate, max_submission_burst_factor)
                             values($client, $maxInflightValidationRequests, $maxSubmissionRate, $maxSubmissionBurstFactor)"""
              }
              storage.update_(query, functionFullName)

            case _ => Future.unit
          }
        } yield cache.set(Some(settings))
      },
      functionFullName,
    )

  override def writeResourceLimits(
      resourceLimits: ResourceLimits
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    // Put the new value into the cache right away so that changes become effective immediately.
    // This also ensures that value meets the object invariant of Settings.
    cache.updateAndGet(_.map(_.copy(resourceLimits = resourceLimits)))

    val ResourceLimits(maxInflightValidationRequests, maxSubmissionRate, maxSubmissionBurstFactor) =
      resourceLimits

    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_settings(max_infight_validation_requests, max_submission_rate, max_submission_burst_factor, client) values($maxInflightValidationRequests, $maxSubmissionRate, $maxSubmissionBurstFactor, $client)
                   on conflict(client) do update set max_infight_validation_requests = $maxInflightValidationRequests, max_submission_rate = $maxSubmissionRate, max_submission_burst_factor = $maxSubmissionBurstFactor"""

      case _: DbStorage.Profile.Oracle | _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_settings using dual on (1 = 1)
                 when matched then
                   update set max_infight_validation_requests = $maxInflightValidationRequests, max_submission_rate = $maxSubmissionRate, max_submission_burst_factor = $maxSubmissionBurstFactor
                 when not matched then
                   insert (max_infight_validation_requests, max_submission_rate, max_submission_burst_factor, client) values ($maxInflightValidationRequests, $maxSubmissionRate, $maxSubmissionBurstFactor, $client)"""
    }
    runQueryAndRefreshCache(query, functionFullName)
  }

  override def insertMaxDeduplicationDuration(maxDeduplicationDuration: NonNegativeFiniteDuration)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    insertOrUpdateIfNull("max_deduplication_duration", maxDeduplicationDuration)

  private def insertOrUpdateIfNull[A: SetParameter](columnName: String, newValue: A)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into par_settings(#$columnName, client) values ($newValue, $client)
               on conflict(client) do
                 update set #$columnName = $newValue where par_settings.#$columnName is null
              """
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into par_settings using dual on (1 = 1)
               when matched and #$columnName is null then
                 update set #$columnName = $newValue
               when not matched then
                 insert (#$columnName, client) values ($newValue, $client)"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into par_settings using dual on (1 = 1)
               when matched then
                 update set #$columnName = $newValue where #$columnName is null
               when not matched then
                 insert (#$columnName, client) values ($newValue, $client)"""
    }
    runQueryAndRefreshCache(query, functionFullName)
  }

  private def runQueryAndRefreshCache(
      query: SqlAction[Int, NoStream, Effect.Write],
      operationName: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingF(operationName)(storage.update_(query, operationName)).transformWith {
      res =>
        // Reload cache to make it consistent with the DB. Particularly important in case of concurrent writes.
        FutureUtil
          .logOnFailureUnlessShutdown(
            refreshCache(),
            s"An exception occurred while refreshing the cache. Keeping old value $settings.",
          )
          .transform(_ => res)
    }

  override protected def onClosed(): Unit = Lifecycle.close(executionQueue)(logger)
}
