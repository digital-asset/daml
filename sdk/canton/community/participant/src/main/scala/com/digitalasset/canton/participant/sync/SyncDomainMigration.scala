// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation}
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SynchronizerAlias
import com.digitalasset.canton.common.sequencer.grpc.SequencerInfoLoader
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.error.{CantonError, ParentCantonError}
import com.digitalasset.canton.lifecycle.{CloseContext, FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection
import com.digitalasset.canton.participant.admin.inspection.SyncStateInspection.SyncStateInspectionError
import com.digitalasset.canton.participant.admin.repair.RepairService
import com.digitalasset.canton.participant.store.SynchronizerConnectionConfigStore
import com.digitalasset.canton.participant.sync.SyncServiceError.{
  MigrationErrors,
  SyncServiceUnknownDomain,
}
import com.digitalasset.canton.participant.synchronizer.{
  DomainRegistryHelpers,
  SynchronizerAliasManager,
  SynchronizerConnectionConfig,
  SynchronizerRegistryError,
}
import com.digitalasset.canton.sequencing.SequencerConnectionValidation
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{ReassignmentTag, SameReassignmentType}

import scala.concurrent.{ExecutionContext, Future}

sealed trait SyncDomainMigrationError extends Product with Serializable with CantonError

class SyncDomainMigration(
    aliasManager: SynchronizerAliasManager,
    domainConnectionConfigStore: SynchronizerConnectionConfigStore,
    inspection: SyncStateInspection,
    repair: RepairService,
    prepareDomainConnection: Traced[SynchronizerAlias] => EitherT[
      FutureUnlessShutdown,
      SyncDomainMigrationError,
      Unit,
    ],
    sequencerInfoLoader: SequencerInfoLoader,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  import com.digitalasset.canton.participant.sync.SyncDomainMigrationError.*

  private def getSynchronizerId(
      sourceAlias: SynchronizerAlias
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainMigrationError, SynchronizerId] =
    EitherT.fromEither[Future](
      aliasManager
        .synchronizerIdForAlias(sourceAlias)
        .toRight(
          SyncDomainMigrationError.InvalidArgument.SourceSynchronizerIdUnknown(sourceAlias)
        )
    )

  private def checkMigrationRequest(
      source: Source[SynchronizerAlias],
      target: Target[SynchronizerConnectionConfig],
      targetSynchronizerId: Target[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainMigrationError, Unit] = {
    logger.debug(s"Checking migration request from $source to ${target.unwrap.synchronizerAlias}")
    for {
      // check that target alias differs from source
      _ <- EitherT.cond[Future](
        source.unwrap != target.unwrap.synchronizerAlias,
        (),
        InvalidArgument.SameSynchronizerAlias(source.unwrap),
      )
      // check that source domain exists and has not been deactivated
      sourceStatus <- EitherT
        .fromEither[Future](source.traverse(domainConnectionConfigStore.get))
        .leftMap(_ => InvalidArgument.UnknownSourceDomain(source))
        .map(_.map(_.status))
      _ <- EitherT.cond[Future](
        sourceStatus.unwrap.canMigrateFrom,
        (),
        InvalidArgument.InvalidDomainConfigStatus(source, sourceStatus),
      )
      // check that synchronizer id (in config) matches observed synchronizer id
      _ <- target.unwrap.synchronizerId.traverse_ { expectedSynchronizerId =>
        EitherT.cond[Future](
          expectedSynchronizerId == targetSynchronizerId.unwrap,
          (),
          SyncDomainMigrationError.InvalidArgument
            .ExpectedsynchronizerIdsDiffer(
              target.map(_.synchronizerAlias),
              expectedSynchronizerId,
              targetSynchronizerId,
            ),
        )
      }
      sourceSynchronizerId <- source.traverse(getSynchronizerId(_))
      _ <- EitherT.cond[Future](
        sourceSynchronizerId.unwrap != targetSynchronizerId.unwrap,
        (),
        SyncDomainMigrationError.InvalidArgument.SourceAndTargetAreSame(
          sourceSynchronizerId
        ): SyncDomainMigrationError,
      )
    } yield ()
  }

  private def registerNewDomain(target: Target[SynchronizerConnectionConfig])(implicit
      traceContext: TraceContext
  ): EitherT[Future, SyncDomainMigrationError, Unit] = {
    logger.debug(s"Registering new domain ${target.unwrap.synchronizerAlias}")
    domainConnectionConfigStore
      .put(target.unwrap, SynchronizerConnectionConfigStore.MigratingTo)
      .leftMap[SyncDomainMigrationError](_ =>
        InternalError.DuplicateConfig(target.unwrap.synchronizerAlias)
      )
  }

  /** Checks whether the migration is possible:
    * - Participant needs to be disconnected from both domains
    * - No in-flight submission (except if `force = true`)
    * - No dirty request (except if `force = true`)
    */
  def isDomainMigrationPossible(
      source: Source[SynchronizerAlias],
      target: Target[SynchronizerConnectionConfig],
      force: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    SyncServiceError,
    Target[SequencerInfoLoader.SequencerAggregatedInfo],
  ] =
    for {
      targetDomainInfo <- target.traverse(domainConnectionConfig =>
        performUnlessClosingEitherUSF(functionFullName)(
          sequencerInfoLoader
            .loadAndAggregateSequencerEndpoints(
              domainConnectionConfig.synchronizerAlias,
              domainConnectionConfig.synchronizerId,
              domainConnectionConfig.sequencerConnections,
              SequencerConnectionValidation.Active,
            )(traceContext, CloseContext(this))
            .leftMap[SyncServiceError] { err =>
              val error = SynchronizerRegistryError.ConnectionErrors.FailedToConnectToSequencer
                .Error(SynchronizerRegistryError.fromSequencerInfoLoaderError(err).cause)
              SyncServiceError
                .SyncServiceFailedDomainConnection(domainConnectionConfig.synchronizerAlias, error)
            }
        )
      )
      _ <- performUnlessClosingEitherU(functionFullName)(
        aliasManager
          .processHandshake(target.unwrap.synchronizerAlias, targetDomainInfo.unwrap.synchronizerId)
          .leftMap(DomainRegistryHelpers.fromSynchronizerAliasManagerError)
          .leftMap[SyncServiceError](err =>
            SyncServiceError.SyncServiceFailedDomainConnection(
              target.unwrap.synchronizerAlias,
              err,
            )
          )
      )

      inFlights <- performUnlessClosingEitherUSF(functionFullName)(
        inspection
          .countInFlight(source.unwrap)
          .leftMap(_ => SyncServiceUnknownDomain.Error(source.unwrap))
      )

      _ <-
        if (force) {
          if (inFlights.exists) {
            logger.info(
              s"Ignoring existing in-flight transactions on domain with alias ${source.unwrap.unwrap} because of forced migration. This may lead to a ledger fork."
            )
          }
          EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())
        } else
          EitherT
            .cond[FutureUnlessShutdown](
              !inFlights.exists,
              (),
              SyncServiceError.SyncServiceDomainMustNotHaveInFlightTransactions.Error(source.unwrap),
            )
            .leftWiden[SyncServiceError]
    } yield targetDomainInfo

  /** Performs the domain migration.
    * Assumes that [[isDomainMigrationPossible]] was called before to check preconditions.
    */
  def migrateDomain(
      source: Source[SynchronizerAlias],
      target: Target[SynchronizerConnectionConfig],
      targetSynchronizerId: Target[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = {
    def prepare(): EitherT[Future, SyncDomainMigrationError, Unit] = {
      logger.debug(s"Preparing domain migration from $source to ${target.unwrap.synchronizerAlias}")
      for {
        // check that the request makes sense
        _ <- checkMigrationRequest(source, target, targetSynchronizerId)
        // check if the target alias already exists.
        targetStatusO = target.traverse(config =>
          domainConnectionConfigStore.get(config.synchronizerAlias).toOption.map(_.status)
        )
        // check if we are already active on the target domain
        _ <- targetStatusO.fold {
          // domain not yet configured, add the configuration
          registerNewDomain(target)
        } { targetStatus =>
          logger.debug(s"Checking status of target domain ${target.unwrap.synchronizerAlias}")
          EitherT.fromEither[Future](
            for {
              // check target status
              _ <- Either.cond(
                targetStatus.unwrap.canMigrateTo,
                (),
                InvalidArgument.InvalidDomainConfigStatus(
                  target.map(_.synchronizerAlias),
                  targetStatus,
                ),
              )
              // check stored alias if it exists
              _ <- aliasManager.synchronizerIdForAlias(target.unwrap.synchronizerAlias).traverse_ {
                storedSynchronizerId =>
                  Either.cond(
                    targetSynchronizerId.unwrap == storedSynchronizerId,
                    (),
                    InvalidArgument.ExpectedsynchronizerIdsDiffer(
                      target.map(_.synchronizerAlias),
                      storedSynchronizerId,
                      targetSynchronizerId,
                    ),
                  )
              }
            } yield ()
          )
        }
        _ <- updateDomainStatus(
          target.unwrap.synchronizerAlias,
          SynchronizerConnectionConfigStore.MigratingTo,
        )
        _ <- updateDomainStatus(source.unwrap, SynchronizerConnectionConfigStore.Vacating)
      } yield ()
    }

    for {
      _ <- performUnlessClosingEitherU(functionFullName)(prepare())
      sourceSynchronizerId <- performUnlessClosingEitherU(functionFullName)(
        source.traverse(getSynchronizerId(_))
      )
      _ <- prepareDomainConnection(Traced(target.unwrap.synchronizerAlias))
      _ <- moveContracts(source, sourceSynchronizerId, targetSynchronizerId)
      _ <- performUnlessClosingEitherU(functionFullName)(
        updateDomainStatus(
          target.unwrap.synchronizerAlias,
          SynchronizerConnectionConfigStore.Active,
        )
      )
      _ <- performUnlessClosingEitherU(functionFullName)(
        updateDomainStatus(source.unwrap, SynchronizerConnectionConfigStore.Inactive)
      )
    } yield ()
  }

  private def updateDomainStatus(
      alias: SynchronizerAlias,
      state: SynchronizerConnectionConfigStore.Status,
  )(implicit traceContext: TraceContext): EitherT[Future, SyncDomainMigrationError, Unit] = {
    logger.info(s"Changing status of domain configuration $alias to $state")
    domainConnectionConfigStore
      .setStatus(alias, state)
      .leftMap(err => SyncDomainMigrationError.InternalError.Generic(err.toString))
  }

  private def moveContracts(
      sourceAlias: Source[SynchronizerAlias],
      source: Source[SynchronizerId],
      target: Target[SynchronizerId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = {
    // TODO(i9270) parameter should be configurable
    val batchSize = PositiveInt.tryCreate(100)
    for {
      // load all contracts on source domain
      acs <- performUnlessClosingEitherUSF(functionFullName)(
        inspection
          .findAcs(sourceAlias.unwrap)
          .leftMap[SyncDomainMigrationError](err =>
            SyncDomainMigrationError.InternalError.FailedReadingAcs(sourceAlias.unwrap, err)
          )
      )
      _ = logger.info(
        s"Found ${acs.size} contracts in the ACS of $sourceAlias that need to be migrated"
      )
      _ <- NonEmpty
        .from(acs.keys.toSeq.distinct) match {
        case None => EitherT.right[SyncDomainMigrationError](FutureUnlessShutdown.unit)
        case Some(contractIds) =>
          // move contracts from one domain to the other domain using repair service in batches of batchSize
          performUnlessClosingEitherUSF(functionFullName)(
            repair.changeAssignation(
              contractIds,
              source,
              target,
              skipInactive = true,
              batchSize,
            )
          )
            .leftMap[SyncDomainMigrationError](
              SyncDomainMigrationError.InternalError.FailedMigratingContracts(sourceAlias.unwrap, _)
            )
      }
    } yield ()
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
    final case class SameSynchronizerAlias(synchronizerAlias: SynchronizerAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = "Source domain must differ from target domain.")
        with SyncDomainMigrationError
    final case class UnknownSourceDomain(domain: Source[SynchronizerAlias])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = s"Source domain $domain is unknown.")
        with SyncDomainMigrationError

    final case class SourceSynchronizerIdUnknown(source: SynchronizerAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = s"Source domain $source has no synchronizer id stored: it's completely empty"
        )
        with SyncDomainMigrationError

    final case class InvalidDomainConfigStatus[T[X] <: ReassignmentTag[X]: SameReassignmentType](
        domain: T[SynchronizerAlias],
        status: T[SynchronizerConnectionConfigStore.Status],
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            s"The domain configuration state of $domain is in an invalid state for the requested migration $status"
        )
        with SyncDomainMigrationError

    final case class ExpectedsynchronizerIdsDiffer(
        alias: Target[SynchronizerAlias],
        expected: SynchronizerId,
        remote: Target[SynchronizerId],
    )(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"The synchronizer id for $alias was expected to be $expected, but is $remote"
        )
        with SyncDomainMigrationError

    final case class SourceAndTargetAreSame(source: Source[SynchronizerId])(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            show"The target synchronizer id needs to be different from the source synchronizer id"
        )
        with SyncDomainMigrationError
  }

  final case class MigrationParentError(
      synchronizerAlias: SynchronizerAlias,
      parent: SyncServiceError,
  )(implicit
      val loggingContext: ErrorLoggingContext
  ) extends SyncDomainMigrationError
      with ParentCantonError[SyncServiceError] {

    override def logOnCreation: Boolean = false
    override def mixinContext: Map[String, String] = Map("domain" -> synchronizerAlias.unwrap)

  }

  object InternalError
      extends ErrorCode(
        "BROKEN_DOMAIN_MIGRATION",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    final case class DuplicateConfig(alias: SynchronizerAlias)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"The synchronizer alias $alias was already present, but shouldn't be"
        )
        with SyncDomainMigrationError

    final case class FailedReadingAcs(source: SynchronizerAlias, err: SyncStateInspectionError)(
        implicit val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Failed reading the ACS"
        )
        with SyncDomainMigrationError

    final case class FailedMigratingContracts(source: SynchronizerAlias, err: String)(implicit
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
