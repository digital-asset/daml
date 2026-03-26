// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.Monad
import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.error.{CantonError, ParentCantonError}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.repair.RepairService
import com.digitalasset.canton.participant.domain.{
  DomainAliasManager,
  DomainConnectionConfig,
  DomainRegistryError,
  DomainRegistryHelpers,
}
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  MigrationErrors,
  SyncServiceUnknownDomain,
}
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.{DomainAlias, SequencerCounter, checked}

import scala.concurrent.duration.DurationLong
import scala.concurrent.{ExecutionContext, Future}
import scala.math.Ordering.Implicits.infixOrderingOps

sealed trait SyncDomainMigrationError extends Product with Serializable with CantonError

class SyncDomainMigration(
    aliasManager: DomainAliasManager,
    domainConnectionConfigStore: DomainConnectionConfigStore,
    inspection: SyncStateInspection,
    repair: RepairService,
    prepareDomainConnection: Traced[DomainAlias] => EitherT[
      FutureUnlessShutdown,
      SyncDomainMigrationError,
      Unit,
    ],
    sequencerInfoLoader: SequencerInfoLoader,
    connectedDomainsLookup: ConnectedDomainsLookup,
    readBatchSize: PositiveInt,
    writeBatchSize: PositiveInt,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  import com.digitalasset.canton.participant.sync.SyncDomainMigrationError.*

  private def getDomainId(
      sourceAlias: DomainAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainMigrationError, DomainId] =
    EitherT.fromEither[Future](
      aliasManager
        .domainIdForAlias(sourceAlias)
        .toRight(
          SyncDomainMigrationError.InvalidArgument.SourceDomainIdUnknown(sourceAlias)
        )
    )

  private def checkMigrationRequest(
      source: DomainAlias,
      target: DomainConnectionConfig,
      targetDomainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainMigrationError, Unit] = {
    logger.debug(s"Checking migration request from $source to ${target.domain}")
    for {
      // check that target alias differs from source
      _ <- EitherT.cond[Future](
        source != target.domain,
        (),
        InvalidArgument.SameDomainAlias(source),
      )
      // check that source domain exists and has not been deactivated
      sourceStatus <- EitherT
        .fromEither[Future](domainConnectionConfigStore.get(source))
        .leftMap(_ => InvalidArgument.UnknownSourceDomain(source))
        .map(_.status)
      _ <- EitherT.cond[Future](
        sourceStatus.canMigrateFrom,
        (),
        InvalidArgument.InvalidDomainConfigStatus(source, sourceStatus),
      )
      // check that domain-id (in config) matches observed domain id
      _ <- target.domainId.traverse_ { expectedDomainId =>
        EitherT.cond[Future](
          expectedDomainId == targetDomainId,
          (),
          SyncDomainMigrationError.InvalidArgument
            .ExpectedDomainIdsDiffer(target.domain, expectedDomainId, targetDomainId),
        )
      }
      sourceDomainId <- getDomainId(source)
      _ <- EitherT.cond[Future](
        sourceDomainId != targetDomainId,
        (),
        SyncDomainMigrationError.InvalidArgument.SourceAndTargetAreSame(
          sourceDomainId
        ): SyncDomainMigrationError,
      )
    } yield ()
  }

  private def registerNewDomain(target: DomainConnectionConfig)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainMigrationError, Unit] = {
    logger.debug(s"Registering new domain ${target.domain}")
    domainConnectionConfigStore
      .put(target, DomainConnectionConfigStore.MigratingTo)
      .leftMap[SyncDomainMigrationError](_ => InternalError.DuplicateConfig(target.domain))
  }

  /** Checks whether the migration is possible:
    * - Participant needs to be disconnected from both domains
    * - No in flight submission (except if `force = true`)
    * - No dirty request (except if `force = true`)
    */
  def isDomainMigrationPossible(
      source: DomainAlias,
      target: DomainConnectionConfig,
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SyncServiceError,
    SequencerInfoLoader.SequencerAggregatedInfo,
  ] = {
    def mustBeOffline(alias: DomainAlias, domainId: DomainId) = EitherT.cond[FutureUnlessShutdown](
      !connectedDomainsLookup.isConnected(domainId),
      (),
      SyncServiceError.SyncServiceDomainMustBeOffline.Error(alias): SyncServiceError,
    )

    for {
      targetDomainInfo <- performUnlessClosingEitherU(functionFullName)(
        sequencerInfoLoader
          .loadSequencerEndpoints(target.domain, target.sequencerConnections)(
            traceContext,
            CloseContext(this),
          )
          .leftMap(DomainRegistryError.fromSequencerInfoLoaderError)
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedDomainConnection(
              target.domain,
              DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(err.cause),
            )
          )
      )
      _ <- performUnlessClosingEitherU(functionFullName)(
        aliasManager
          .processHandshake(target.domain, targetDomainInfo.domainId)
          .leftMap(DomainRegistryHelpers.fromDomainAliasManagerError)
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedDomainConnection(
              target.domain,
              err,
            )
          )
      )

      sourceDomainId <- EitherT.fromEither[FutureUnlessShutdown](
        aliasManager
          .domainIdForAlias(source)
          .toRight(
            SyncServiceError.SyncServiceUnknownDomain.Error(source): SyncServiceError
          )
      )

      // Perform all checks whether we can do the migration
      _ <- mustBeOffline(source, sourceDomainId)
      _ <- mustBeOffline(target.domain, targetDomainInfo.domainId)

      inFlights <- performUnlessClosingEitherU(functionFullName)(
        inspection
          .countInFlight(source)
          .leftMap(_ => SyncServiceUnknownDomain.Error(source))
      )

      _ <-
        if (force) {
          if (inFlights.exists) {
            logger.info(
              s"Ignoring existing in-flight transactions on domain with alias ${source.unwrap} because of forced migration. This may lead to a ledger fork."
            )
          }
          EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())
        } else
          EitherT
            .cond[FutureUnlessShutdown](
              !inFlights.exists,
              (),
              SyncServiceError.SyncServiceDomainMustNotHaveInFlightTransactions.Error(source),
            )
            .leftWiden[SyncServiceError]
    } yield targetDomainInfo
  }

  /** Performs the domain migration.
    * Assumes that [[isDomainMigrationPossible]] was called before to check preconditions.
    */
  def migrateDomain(
      source: DomainAlias,
      target: DomainConnectionConfig,
      targetDomainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = {
    def prepare(): EitherT[Future, SyncDomainMigrationError, Unit] = {
      logger.debug(s"Preparing domain migration from $source to ${target.domain}")
      for {
        // check that the request makes sense
        _ <- checkMigrationRequest(source, target, targetDomainId)
        // check if the target alias already exists.
        targetStatusO = domainConnectionConfigStore.get(target.domain).toOption.map(_.status)
        // check if we are already active on the target domain
        _ <- targetStatusO.fold {
          // domain not yet configured, add the configuration
          registerNewDomain(target)
        } { targetStatus =>
          logger.debug(s"Checking status of target domain ${target.domain}")
          EitherT.fromEither[Future](
            for {
              // check target status
              _ <- Either.cond(
                targetStatus.canMigrateTo,
                (),
                InvalidArgument.InvalidDomainConfigStatus(target.domain, targetStatus),
              )
              // check stored alias if it exists
              _ <- aliasManager.domainIdForAlias(target.domain).traverse_ { storedDomainId =>
                Either.cond(
                  targetDomainId == storedDomainId,
                  (),
                  InvalidArgument.ExpectedDomainIdsDiffer(
                    target.domain,
                    storedDomainId,
                    targetDomainId,
                  ),
                )
              }
            } yield ()
          )
        }
        _ <- updateDomainStatus(target.domain, DomainConnectionConfigStore.MigratingTo)
        _ <- updateDomainStatus(source, DomainConnectionConfigStore.Vacating)
      } yield ()
    }

    for {
      _ <- performUnlessClosingEitherU(functionFullName)(prepare())
      sourceDomainId <- performUnlessClosingEitherU(functionFullName)(getDomainId(source))
      _ <- prepareDomainConnection(Traced(target.domain))
      _ <- moveContracts(source, sourceDomainId, targetDomainId)
      _ <- performUnlessClosingEitherU(functionFullName)(
        updateDomainStatus(target.domain, DomainConnectionConfigStore.Active)
      )
      _ <- performUnlessClosingEitherU(functionFullName)(
        updateDomainStatus(source, DomainConnectionConfigStore.Inactive)
      )
    } yield ()
  }

  /*
  This method lives here because it should be called only for a deactivated domain,
  which happens only as part of the hard domain migration.
  Moreover, we want this to be available in community, so it cannot be implemented in
  the pruning processor.
   */
  def pruneSelectedDeactivatedDomainStores(
      domainId: DomainId
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    performUnlessClosingF("pruneSelectedDeactivatedDomainStores") {
      logger.info(
        s"About to prune deactivated sync domain $domainId sequenced event store'"
      )

      repair.syncDomainPersistentStateManager
        .get(domainId)
        .fold(Future.unit)(_.sequencedEventStore.delete(SequencerCounter.Genesis))
    }

  private def updateDomainStatus(
      alias: DomainAlias,
      state: DomainConnectionConfigStore.Status,
  )(implicit traceContext: TraceContext): EitherT[Future, SyncDomainMigrationError, Unit] = {
    logger.info(s"Changing status of domain configuration $alias to $state")
    domainConnectionConfigStore
      .setStatus(alias, state)
      .leftMap(err => SyncDomainMigrationError.InternalError.Generic(err.toString))
  }

  /** Migrates contracts using a '''Read-Ahead Pipeline''' to maximize throughput.
    *
    * General Idea:
    * Unlike standard sequential migration (`Read -> Write`), this implementation
    * overlaps IO operations to eliminate idle time:
    * - Read-Ahead: While writing Batch N (heavy operation), it asynchronously fetches
    *   Batch N+1 in the background.
    * - Hope/Result: The network/DB latency of reading becomes effectively zero, as it is hidden
    *   behind the write latency.
    *
    * Configuration Strategy:
    * - Read Batch (Small): Optimized for efficient DB paging and index usage (e.g., 10k).
    * - Write Batch (Large): Accumulates multiple read pages to amortize the high cost of commits
    *   and sequencing (e.g., 250k).
    */
  private def moveContracts(
      sourceAlias: DomainAlias,
      source: DomainId,
      target: DomainId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = {

    // The payload of contracts IDs to migrate (the "Batch")
    type ContractBuffer = Vector[LfContractId]
    // The cursor position to resume reading from for the next batch
    type Cursor = Option[LfContractId]

    type PipelineState = (ContractBuffer, Cursor)

    // Load the heavy, static domain state required for the migration loop upfront
    val migrationContext = performUnlessClosingEitherU(functionFullName)(
      repair.prepareDomainMigrationOnce(source, target)
    ).leftMap(err => SyncDomainMigrationError.InternalError.Generic(err))

    migrationContext.flatMap { case (sourceData, targetData) =>
      // Recursively fetches pages until we reach the target buffer size or run out of data
      def fetchBatchUntilFullBuffer(
          startCursor: Cursor,
          initialBufferSize: Int,
      ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, PipelineState] = {
        val currentBuffer = Vector.newBuilder[LfContractId]
        currentBuffer.sizeHint(writeBatchSize.value)

        Monad[EitherT[FutureUnlessShutdown, SyncDomainMigrationError, *]].tailRecM(
          (initialBufferSize, startCursor)
        ) { case (currentBufferSize, currentCursor) =>
          // Stop if we have enough data for a write batch
          if (currentBufferSize >= writeBatchSize.value) {
            logger.info(s"Read $currentBufferSize entries")
            EitherT.pure(Right((currentBuffer.result(), currentCursor)))
          } else {
            val paginationSize = readBatchSize min
              // checked() because we know currentBufferSize < writeBatchSize.value
              checked(PositiveInt.tryCreate(writeBatchSize.value - currentBufferSize))
            logger.info(
              s"Reading up to $paginationSize entries (writeBuffer size = $writeBatchSize)"
            )

            performUnlessClosingEitherU(functionFullName)(
              inspection
                .findContractIds(
                  sourceAlias,
                  paginationSize = Some(paginationSize),
                  paginationCursor = currentCursor,
                )
                .leftMap(err =>
                  SyncDomainMigrationError.InternalError
                    .FailedReadingAcs(sourceAlias, err): SyncDomainMigrationError
                )
            ).map { fetchedIds =>
              if (fetchedIds.isEmpty) {
                // No more data in DB, return what we have
                logger.info(s"Read $currentBufferSize entries")
                Right((currentBuffer.result(), None))
              } else {
                // We got data, append and recurse
                // Note: fetchedIds are usually sorted DESC, so the cursor is the last element
                currentBuffer.addAll(fetchedIds)
                Left((currentBufferSize + fetchedIds.size, fetchedIds.lastOption))
              }
            }
          }
        }
      }

      // Prime the pipeline: Fill the first buffer before starting the loop
      fetchBatchUntilFullBuffer(Option.empty[LfContractId], 0).flatMap { initialPipelineState =>
        // Main Loop: Process current batch while fetching next
        // State passed to Left(...) is the result of the *next* read: (bufferToMigrate, NextCursor)
        Monad[EitherT[FutureUnlessShutdown, SyncDomainMigrationError, *]].tailRecM(
          initialPipelineState
        ) { case (bufferToMigrate, cursorForBackgroundFill) =>
          if (bufferToMigrate.isEmpty) {
            // Done: No contracts left to migrate
            EitherT.pure[FutureUnlessShutdown, SyncDomainMigrationError](Right(()))
          } else {

            // Optimization: Read-Ahead Pipelining
            // Trigger the next read immediately. FutureUnlessShutdown is eager, so this starts
            // executing in the background while we proceed to write the current batch.
            val backgroundFillStartTime = System.nanoTime()
            val backgroundFill =
              fetchBatchUntilFullBuffer(cursorForBackgroundFill, 0)
                .map(v => System.nanoTime() - backgroundFillStartTime -> v)
                .value

            logger.info(
              s"Migrating buffer of ${bufferToMigrate.size} contracts from $sourceAlias (Pipeline active)..."
            )
            for {

              // Write: Commit the current buffer (Heavy Operation)
              // This runs while the next read is happening in parallel (read-ahead)
              _ <- performUnlessClosingEitherU(functionFullName)(
                repair.changeDomainBatched(
                  bufferToMigrate,
                  sourceData,
                  targetData,
                  skipInactive = true,
                  Some(writeBatchSize),
                )
              ).leftMap(
                SyncDomainMigrationError.InternalError
                  .FailedMigratingContracts(sourceAlias, _): SyncDomainMigrationError
              )

              // Re-sync with Read-Ahead: Await the background fill to get the next state
              // Note: If writing was slow (normal), this result is likely already ready
              nextPipelineStateWithElapsed <- EitherT(backgroundFill)
              (elapsed, nextPipelineState) = nextPipelineStateWithElapsed
              _ = logger.info(
                s"Background fill took ${LoggerUtil.roundDurationForHumans(elapsed.nanos)}"
              )

            } yield Left(nextPipelineState)
          }
        }
      }
    }
  }

}

object SyncDomainMigrationError extends MigrationErrors() {

  @Explanation(
    "This error results when invalid arguments are passed to the migration command."
  )
  object InvalidArgument
      extends ErrorCode(
        "INVALID_DOMAIN_MIGRATION_REQUEST",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class SameDomainAlias(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "Source domain must differ from target domain.")
        with SyncDomainMigrationError
    final case class UnknownSourceDomain(domain: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Source domain $domain is unknown.")
        with SyncDomainMigrationError

    final case class SourceDomainIdUnknown(source: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Source domain $source has no domain-id stored: it's completely empty"
        )
        with SyncDomainMigrationError

    final case class InvalidDomainConfigStatus(
        domain: DomainAlias,
        status: DomainConnectionConfigStore.Status,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The domain configuration state of $domain is in an invalid state for the requested migration $status"
        )
        with SyncDomainMigrationError

    final case class ExpectedDomainIdsDiffer(
        alias: DomainAlias,
        expected: DomainId,
        remote: DomainId,
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"The domain id for $alias was expected to be $expected, but is $remote"
        )
        with SyncDomainMigrationError

    final case class SourceAndTargetAreSame(source: DomainId)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"The target domain id needs to be different from the source domain id"
        )
        with SyncDomainMigrationError
  }

  final case class MigrationParentError(domain: DomainAlias, parent: SyncServiceError)(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncDomainMigrationError
      with ParentCantonError[SyncServiceError] {

    override def logOnCreation: Boolean = false
    override def mixinContext: Map[String, String] = Map("domain" -> domain.unwrap)

  }

  object InternalError
      extends ErrorCode(
        "BROKEN_DOMAIN_MIGRATION",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class DuplicateConfig(alias: DomainAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"The domain alias $alias was already present, but shouldn't be"
        )
        with SyncDomainMigrationError

    final case class FailedReadingAcs(source: DomainAlias, err: AcsError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Failed reading the ACS"
        )
        with SyncDomainMigrationError

    final case class FailedMigratingContracts(source: DomainAlias, err: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Migrating the ACS to the new domain failed unexpectedly!"
        )
        with SyncDomainMigrationError

    final case class Generic(reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Failure during migration"
        )
        with SyncDomainMigrationError

  }

}
