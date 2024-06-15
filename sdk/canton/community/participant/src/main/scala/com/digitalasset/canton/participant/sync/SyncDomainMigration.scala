// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.foldable.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation}
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.DomainAlias
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
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*

import scala.concurrent.{ExecutionContext, Future}

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

      hasInFlightSubmissions <- performUnlessClosingEitherU(functionFullName)(
        inspection
          .hasInFlightSubmissions(source)
          .leftMap(_ => SyncServiceUnknownDomain.Error(source))
      )
      hasDirtyRequests <- performUnlessClosingEitherU(functionFullName)(
        inspection
          .hasDirtyRequests(source)
          .leftMap(_ => SyncServiceUnknownDomain.Error(source))
      )

      _ <-
        if (force) {
          if (hasInFlightSubmissions || hasDirtyRequests) {
            logger.info(
              s"Ignoring existing in-flight transactions on domain with alias ${source.unwrap} because of forced migration. This may lead to a ledger fork."
            )
          }
          EitherT.rightT[FutureUnlessShutdown, SyncServiceError](())
        } else
          EitherT
            .cond[FutureUnlessShutdown](
              !hasInFlightSubmissions,
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

  private def updateDomainStatus(
      alias: DomainAlias,
      state: DomainConnectionConfigStore.Status,
  )(implicit traceContext: TraceContext): EitherT[Future, SyncDomainMigrationError, Unit] = {
    logger.info(s"Changing status of domain configuration $alias to $state")
    domainConnectionConfigStore
      .setStatus(alias, state)
      .leftMap(err => SyncDomainMigrationError.InternalError.Generic(err.toString))
  }

  private def moveContracts(
      sourceAlias: DomainAlias,
      source: DomainId,
      target: DomainId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SyncDomainMigrationError, Unit] = {
    // TODO(i9270) parameter should be configurable
    val batchSize = PositiveInt.tryCreate(100)
    for {
      // load all contracts on source domain
      acs <- performUnlessClosingEitherU(functionFullName)(
        inspection
          .findAcs(sourceAlias)
          .leftMap[SyncDomainMigrationError](err =>
            SyncDomainMigrationError.InternalError.FailedReadingAcs(sourceAlias, err)
          )
      )
      _ = logger.info(
        s"Found ${acs.size} contracts in the ACS of $sourceAlias that need to be migrated"
      )
      // move contracts from one domain to the other domain using repair service in batches of 1000
      _ <- performUnlessClosingEitherU(functionFullName)(
        repair.changeDomain(
          acs.keys.toSeq,
          source,
          target,
          skipInactive = true,
          batchSize,
        )
      )
        .leftMap[SyncDomainMigrationError](
          SyncDomainMigrationError.InternalError.FailedMigratingContracts(sourceAlias, _)
        )
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

    final case class PurgeDeactivatedDomain(source: DomainId, err: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(cause = show"The domain cannot be purged")
        with SyncDomainMigrationError

    final case class Generic(reason: String)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = show"Failure during migration"
        )
        with SyncDomainMigrationError

  }

}
