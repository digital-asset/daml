// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse.ContractEntry
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.digitalasset.base.error.{ErrorCategory, ErrorCode, Explanation, Resolution, RpcError}
import com.digitalasset.canton.auth.CantonAdminTokenDispenser
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.AdminWorkflowServicesErrorGroup
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as A
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.ledger.client.{LedgerClient, ResilientLedgerSubscription}
import com.digitalasset.canton.lifecycle.{
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  UnlessShutdown,
  *,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.party.PartyReplicationAdminWorkflow
import com.digitalasset.canton.participant.config.ParticipantNodeConfig
import com.digitalasset.canton.participant.ledger.api.client.LedgerConnection
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.TopologyManagerError.{
  NoAppropriateSigningKeyInStore,
  SecretKeyNotInStore,
}
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{Spanning, TraceContext, Traced, TracerProvider}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.digitalasset.canton.util.{DamlPackageLoader, EitherTUtil, FutureUtil, MonadUtil}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.language.Ast
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Flow

import java.io.InputStream
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/** Manages our admin workflow applications (ping, party management). Currently, each is an
  * individual application with their own ledger connection and acting independently.
  */
class AdminWorkflowServices(
    config: ParticipantNodeConfig,
    parameters: ParticipantNodeParameters,
    packageService: PackageService,
    syncService: CantonSyncService,
    participantId: ParticipantId,
    adminTokenDispenser: CantonAdminTokenDispenser,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
    protected val clock: Clock,
    tracerProvider: TracerProvider,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    tracer: Tracer,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends FlagCloseableAsync
    with NamedLogging
    with Spanning {

  private[this] val adminWorkflowsLoaded: PromiseUnlessShutdown[Unit] =
    PromiseUnlessShutdown.unsupervised()

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  withNewTraceContext("load_admin_workflows_dar") { implicit traceContext =>
    if (syncService.isActive() && parameters.adminWorkflow.autoLoadDar) {
      logger.debug("Loading admin workflows DAR")
      // load the admin workflows daml archive before moving forward
      // We use the pre-packaged dar from the resources/dar folder instead of the compiled one.
      loadDamlArchiveUnlessRegistered()
    } else {
      // Check that admin workflow dars are loaded as we may be transitioning from a passive to an active participant
      adminWorkflowsAreLoaded() match {
        case UnlessShutdown.Outcome(true) =>
          adminWorkflowsLoaded.success(UnlessShutdown.unit)
        case UnlessShutdown.Outcome(false) =>
          logger.warn("Admin workflow services not started as DARs have not been loaded")
        case _ =>
      }
    }
  }

  val (pingSubscription, ping) = createService(
    "admin-ping",
    // we can resubscribe as the ping service is forgiving if we missed a few events
    resubscribeIfPruned = true,
  ) { connection =>
    new PingService(
      connection,
      participantId.adminParty,
      parameters.adminWorkflow.bongTestMaxLevel,
      parameters.adminWorkflow.retries,
      NonNegativeFiniteDuration.fromConfig(parameters.adminWorkflow.maxBongDuration),
      NonNegativeFiniteDuration.fromConfig(parameters.adminWorkflow.pingResponseTimeout),
      timeouts,
      syncService.maxDeduplicationDuration, // Set the deduplication duration for Ping command to the maximum allowed.
      tracer,
      new PingService.SyncServiceHandle {
        override def isActive: Boolean = syncService.isActive()
        override def subscribeToConnections(subscriber: Traced[SynchronizerId] => Unit): Unit =
          syncService.subscribeToConnections(subscriber)
      },
      futureSupervisor,
      loggerFactory,
      clock,
    )
  }

  val partyManagementO: Option[
    (FutureUnlessShutdown[ResilientLedgerSubscription[?, ?]], PartyReplicationAdminWorkflow)
  ] =
    parameters.unsafeOnlinePartyReplication.map(config =>
      createService(
        "party-management",
        // TODO(#20637): Don't resubscribe if the ledger api has been pruned as that would mean missing updates that
        //  the PartyReplicationAdminWorkflow cares about. Instead let the ledger subscription fail after logging an error.
        resubscribeIfPruned = false,
      ) { connection =>
        new PartyReplicationAdminWorkflow(
          connection,
          participantId,
          syncService,
          clock,
          config,
          futureSupervisor,
          parameters.exitOnFatalFailures,
          timeouts,
          loggerFactory,
        )
      }
    )

  protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    def adminServiceCloseables(
        name: String,
        subscription: FutureUnlessShutdown[ResilientLedgerSubscription[?, ?]],
        service: AdminWorkflowService,
    ) =
      Seq[AsyncOrSyncCloseable](
        AsyncCloseable(
          s"$name-subscription",
          subscription
            .map(sub => LifeCycle.close(sub)(logger))
            .recover { err =>
              logger.warn(
                s"Skipping closing of defunct $name subscription due to ${err.getMessage}"
              )
              UnlessShutdown.unit
            }
            .unwrap,
          timeouts.unbounded,
        ),
        SyncCloseable(s"$name-service", LifeCycle.close(service)(logger)),
      )

    adminServiceCloseables("ping", pingSubscription, ping) ++ partyManagementO
      .fold(Seq.empty[AsyncOrSyncCloseable]) {
        case (partyManagementSubscription, partyManagement) =>
          adminServiceCloseables("party-management", partyManagementSubscription, partyManagement)
      }
  }

  private def checkPackagesStatus(
      pkgs: Map[PackageId, Ast.Package],
      lc: LedgerClient,
  )(implicit traceContext: TraceContext): Future[Boolean] =
    for {
      pkgRes <- pkgs.keys.toList.parTraverse(lc.packageService.getPackageStatus(_))
    } yield pkgRes.forall(pkgResponse => pkgResponse.packageStatus.isPackageStatusRegistered)

  private def adminWorkflowsAreLoaded()(implicit
      traceContext: TraceContext
  ): UnlessShutdown[Boolean] =
    withResource(createLedgerClient("admin-checkLoadStatus")) { conn =>
      parameters.processingTimeouts.unbounded.awaitUS("Check Daml packages loaded") {
        def isLoaded(darName: String): FutureUnlessShutdown[Boolean] = {
          val packages = AdminWorkflowServices.getDarPackages(darName)
          FutureUnlessShutdown
            .outcomeF(checkPackagesStatus(packages, conn))
        }

        for {
          adminWorkflowLoaded <- isLoaded(AdminWorkflowServices.PingDarResourceFileName)
          partReplicationWorkflowLoaded <-
            if (config.parameters.unsafeOnlinePartyReplication.isDefined)
              isLoaded(AdminWorkflowServices.PartyReplicationDarResourceFileName)
            else FutureUnlessShutdown.pure(true)
        } yield adminWorkflowLoaded && partReplicationWorkflowLoaded
      }
    }

  /** Parses dar and checks if all contained packages are already loaded and recorded in the
    * indexer. If not, loads the dar. Does NOT vet the dar.
    * @throws java.lang.IllegalStateException
    *   if the daml archive cannot be found on the classpath
    */
  private def loadDamlArchiveUnlessRegistered()(implicit traceContext: TraceContext): Unit =
    withResource(createLedgerClient("admin-checkStatus")) { conn =>
      parameters.processingTimeouts.unbounded.awaitUS_("Load Daml packages") {
        def load(darName: String): FutureUnlessShutdown[Unit] = {
          logger.debug(s"Loading dar `$darName` if not already loaded")
          val packages = AdminWorkflowServices.getDarPackages(darName)
          FutureUnlessShutdown
            .outcomeF(checkPackagesStatus(packages, conn))
            .flatMap(isAlreadyLoaded =>
              MonadUtil.when(!isAlreadyLoaded)(
                EitherTUtil.toFutureUnlessShutdown(loadDamlArchiveResource(darName))
              )
            )
        }

        val resultUS = for {
          _ <- load(AdminWorkflowServices.PingDarResourceFileName)
          _ <-
            if (config.parameters.unsafeOnlinePartyReplication.isDefined)
              load(AdminWorkflowServices.PartyReplicationDarResourceFileName)
            else FutureUnlessShutdown.pure(())
        } yield ()

        resultUS.transform[Unit](
          (value: UnlessShutdown[Unit]) => {
            adminWorkflowsLoaded.success(value)
            value
          },
          (error: Throwable) => {
            adminWorkflowsLoaded.failure(error)
            error
          },
        )
      }
    }

  /** For the admin workflows to run inside the participant we require their daml packages to be
    * loaded. This assumes that the daml archive has been included on the classpath and is loaded or
    * can be loaded as a resource.
    * @return
    *   Future that contains an IllegalStateException or a Unit
    * @throws RuntimeException
    *   if the daml archive cannot be found on the classpath
    */
  private def loadDamlArchiveResource(darName: String)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, IllegalStateException, Unit] = {
    val bytes =
      withResource(AdminWorkflowServices.getDarInputStream(darName))(ByteString.readFrom)
    AdminWorkflowServices.handleDamlErrorDuringPackageLoading(darName)(
      packageService
        .upload(
          darBytes = bytes,
          description = Some("System package"),
          submissionIdO = None,
          vettingInfo = None,
          expectedMainPackageId = None,
        )
        .void
    )
  }

  private def createLedgerClient(
      userId: String
  ): LedgerClient = {
    val taggedUserId = A.UserId(userId)
    val ledgerApiConfig = config.ledgerApi
    LedgerConnection.createLedgerClient(
      taggedUserId,
      ledgerApiConfig.clientConfig,
      CommandClientConfiguration.default, // not used by admin workflows
      tracerProvider,
      loggerFactory,
      Some(adminTokenDispenser),
    )
  }

  private def createService[S <: AdminWorkflowService](
      userId: String,
      resubscribeIfPruned: Boolean,
  )(
      createService: LedgerClient => S
  ): (FutureUnlessShutdown[ResilientLedgerSubscription[?, ?]], S) = {
    import TraceContext.Implicits.Empty.*

    val client = createLedgerClient(userId)
    val service = createService(client)

    val startupUS =
      adminWorkflowsLoaded.futureUS.flatMap { _ =>
        FutureUnlessShutdown.outcomeF {
          client.stateService.getLedgerEndOffset().flatMap { offset =>
            client.stateService
              .getActiveContractsSource(
                eventFormat = service.eventFormat,
                validAtOffset = offset,
                token = None,
              )
              .map(_.contractEntry)
              .collect { case ContractEntry.ActiveContract(contract) =>
                contract
              }
              .runFoldAsync(()) { case (_, contract) =>
                Future(service.processAcs(Seq(contract)))
              }
              .map { _ =>
                new ResilientLedgerSubscription(
                  makeSource = subscribeOffset =>
                    client.updateService.getUpdatesSource(
                      begin = subscribeOffset,
                      eventFormat = service.eventFormat,
                    ),
                  consumingFlow = Flow[GetUpdatesResponse]
                    .map(_.update)
                    .map {
                      case GetUpdatesResponse.Update.Transaction(tx) =>
                        service.processTransaction(tx)
                      case GetUpdatesResponse.Update.Reassignment(reassignment) =>
                        service.processReassignment(reassignment)
                      case GetUpdatesResponse.Update.OffsetCheckpoint(_) => ()
                      case GetUpdatesResponse.Update.TopologyTransaction(_) =>
                        ()
                      case GetUpdatesResponse.Update.Empty => ()
                    },
                  subscriptionName = service.getClass.getSimpleName,
                  startOffset = offset,
                  extractOffset = ResilientLedgerSubscription.extractOffsetFromGetUpdateResponse,
                  timeouts = timeouts,
                  loggerFactory = loggerFactory,
                  resubscribeIfPruned = resubscribeIfPruned,
                )
              }
          }
        }
      }

    (FutureUtil.logOnFailureUS(startupUS, s"Failed to start $service"), service)
  }

}

object AdminWorkflowServices extends AdminWorkflowServicesErrorGroup {

  val PingDarResourceName: String = "canton-builtin-admin-workflow-ping"
  val PingDarResourceFileName: String = s"$PingDarResourceName.dar"
  val PartyReplicationDarResourceName: String =
    "canton-builtin-admin-workflow-party-replication-alpha"
  private val PartyReplicationDarResourceFileName: String =
    s"$PartyReplicationDarResourceName.dar"
  val AdminWorkflowNames: Set[String] = Set(PingDarResourceName, PartyReplicationDarResourceName)

  private def getDarInputStream(resourceName: String): InputStream =
    Option(
      PingService.getClass.getClassLoader.getResourceAsStream(resourceName)
    ) match {
      case Some(is) => is
      case None =>
        throw new IllegalStateException(
          s"Failed to load [$resourceName] from classpath"
        )
    }

  private[participant] def getDarPackages(darName: String): Map[PackageId, Ast.Package] =
    DamlPackageLoader
      .getPackagesFromInputStream(darName, getDarInputStream(darName))
      .valueOr(err =>
        throw new IllegalStateException(s"Unable to load admin workflow packages: $err")
      )

  private[participant] def handleDamlErrorDuringPackageLoading(adminWorkflow: String)(
      res: EitherT[FutureUnlessShutdown, RpcError, Unit]
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, IllegalStateException, Unit] =
    EitherTUtil
      .leftSubflatMap(res) {
        case CantonPackageServiceError.IdentityManagerParentError(
              ParticipantTopologyManagerError.IdentityManagerParentError(
                NoAppropriateSigningKeyInStore.Failure(_, _) | SecretKeyNotInStore.Failure(_)
              )
            ) =>
          // Log error by creating error object, but continue processing.
          AdminWorkflowServices.CanNotAutomaticallyVetAdminWorkflowPackage
            .Error(adminWorkflow)
            .discard
          Either.unit
        case err =>
          Left(new IllegalStateException(CantonError.stringFromContext(err)))
      }

  lazy val PingPackages: Map[PackageId, Ast.Package] = getDarPackages(PingDarResourceFileName)
  lazy val PartyReplicationPackages: Map[PackageId, Ast.Package] = getDarPackages(
    PartyReplicationDarResourceFileName
  )
  lazy val AllBuiltInPackages: Map[PackageId, Ast.Package] =
    PingPackages ++ PartyReplicationPackages

  @Explanation(
    """This error indicates that the admin workflow package could not be vetted. The admin workflows is
      |a set of packages that are pre-installed and can be used for administrative processes.
      |The error can happen if the participant is initialised manually but is missing the appropriate
      |signing keys or certificates in order to issue new topology transactions within the participants
      |namespace.
      |The admin workflows can not be used until the participant has vetted the package."""
  )
  @Resolution(
    """This error can be fixed by ensuring that an appropriate vetting transaction is issued in the
      |name of this participant and imported into this participant node.
      |If the corresponding certificates have been added after the participant startup, then
      |this error can be fixed by either restarting the participant node, issuing the vetting transaction manually
      |or re-uploading the Dar (leaving the vetAllPackages argument as true)"""
  )
  object CanNotAutomaticallyVetAdminWorkflowPackage
      extends ErrorCode(
        id = "CAN_NOT_AUTOMATICALLY_VET_ADMIN_WORKFLOW_PACKAGE",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {
    final case class Error(adminWorkflow: String)(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            s"Unable to vet `$adminWorkflow` automatically. Please ensure you vet this package before using one of the admin workflows."
        )

  }
}
