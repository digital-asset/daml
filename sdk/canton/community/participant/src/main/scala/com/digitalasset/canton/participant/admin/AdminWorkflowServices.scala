// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.error.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.AdminWorkflowServicesErrorGroup
import com.digitalasset.canton.ledger.api.refinements.ApiTypes as A
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.ledger.client.{LedgerClient, ResilientLedgerSubscription}
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.AdminWorkflowServices.AbortedDueToShutdownException
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.participant.ledger.api.CantonAdminToken
import com.digitalasset.canton.participant.ledger.api.client.LedgerConnection
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.TopologyManagerError.{
  NoAppropriateSigningKeyInStore,
  SecretKeyNotInStore,
}
import com.digitalasset.canton.topology.{DomainId, PartyId}
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, Spanning, TraceContext, Traced, TracerProvider}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.digitalasset.canton.util.{DamlPackageLoader, EitherTUtil}
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Flow

import java.io.InputStream
import scala.concurrent.{ExecutionContextExecutor, Future}

/** Manages our admin workflow applications (ping, dar distribution).
  * Currently each is individual application with their own ledger connection and acting independently.
  */
class AdminWorkflowServices(
    config: LocalParticipantConfig,
    parameters: ParticipantNodeParameters,
    packageService: PackageService,
    syncService: CantonSyncService,
    adminPartyId: PartyId,
    adminToken: CantonAdminToken,
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
    with Spanning
    with NoTracing {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  if (syncService.isActive() && parameters.adminWorkflow.autoLoadDar) {
    withNewTraceContext { implicit traceContext =>
      logger.debug("Loading admin workflows DAR")
      // load the admin workflows daml archive before moving forward
      // We use the pre-packaged dar from the resources/dar folder instead of the compiled one.
      loadDamlArchiveUnlessRegistered()
    }
  }

  val (pingSubscription, ping) = createService(
    "admin-ping",
    // we can resubscribe as the ping service is forgiving if we missed a few events
    resubscribeIfPruned = true,
  ) { connection =>
    new PingService(
      connection,
      adminPartyId,
      parameters.adminWorkflow.bongTestMaxLevel,
      parameters.adminWorkflow.retries,
      NonNegativeFiniteDuration.fromConfig(parameters.adminWorkflow.maxBongDuration),
      NonNegativeFiniteDuration.fromConfig(parameters.adminWorkflow.pingResponseTimeout),
      timeouts,
      syncService.maxDeduplicationDuration, // Set the deduplication duration for Ping command to the maximum allowed.
      tracer,
      new PingService.SyncServiceHandle {
        override def isActive: Boolean = syncService.isActive()
        override def subscribeToConnections(subscriber: Traced[DomainId] => Unit): Unit =
          syncService.subscribeToConnections(subscriber)
      },
      futureSupervisor,
      loggerFactory,
      clock,
    )
  }

  protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq[AsyncOrSyncCloseable](
    AsyncCloseable(
      "connection",
      pingSubscription.map(conn => Lifecycle.close(conn)(logger)).recover { err =>
        logger.warn(s"Skipping closing of defunct ping subscription due to ${err.getMessage}")
      },
      timeouts.unbounded,
    ),
    SyncCloseable(
      "services",
      Lifecycle.close(
        ping
      )(logger),
    ),
  )

  private def checkPackagesStatus(
      pkgs: Map[PackageId, Ast.Package],
      lc: LedgerClient,
  ): Future[Boolean] =
    for {
      pkgRes <- pkgs.keys.toList.parTraverse(lc.v2.packageService.getPackageStatus(_))
    } yield pkgRes.forall(pkgResponse => pkgResponse.packageStatus.isPackageStatusRegistered)

  private def handleDamlErrorDuringPackageLoading(
      res: EitherT[FutureUnlessShutdown, DamlError, Unit]
  ): EitherT[Future, IllegalStateException, Unit] = EitherT {
    EitherTUtil
      .leftSubflatMap(res) {
        case CantonPackageServiceError.IdentityManagerParentError(
              ParticipantTopologyManagerError.IdentityManagerParentError(
                NoAppropriateSigningKeyInStore.Failure(_) | SecretKeyNotInStore.Failure(_)
              )
            ) =>
          // Log error by creating error object, but continue processing.
          AdminWorkflowServices.CanNotAutomaticallyVetAdminWorkflowPackage.Error().discard
          Right(())
        case err =>
          Left(new IllegalStateException(CantonError.stringFromContext(err)))
      }
      .value
      .failOnShutdownTo(new AbortedDueToShutdownException())
  }

  /** Parses dar and checks if all contained packages are already loaded and recorded in the indexer. If not,
    * loads the dar.
    * @throws java.lang.IllegalStateException if the daml archive cannot be found on the classpath
    * @throws AbortedDueToShutdownException if the node is shutting down
    */
  private def loadDamlArchiveUnlessRegistered()(implicit traceContext: TraceContext): Unit =
    withResource(createLedgerClient("admin-checkStatus")) { conn =>
      parameters.processingTimeouts.unbounded.await_(s"Load Daml packages") {
        checkPackagesStatus(AdminWorkflowServices.AdminWorkflowPackages, conn).flatMap {
          isAlreadyLoaded =>
            if (!isAlreadyLoaded) EitherTUtil.toFuture(loadDamlArchiveResource())
            else {
              logger.debug("Admin workflow packages are already present. Skipping loading.")
              // vet any packages that have not yet been vetted
              EitherTUtil.toFuture(
                handleDamlErrorDuringPackageLoading(
                  packageService
                    .vetPackages(
                      AdminWorkflowServices.AdminWorkflowPackages.keys.toSeq,
                      syncVetting = false,
                    )
                )
              )
            }
        }
      }
    }

  /** For the admin workflows to run inside the participant we require their daml packages to be loaded.
    * This assumes that the daml archive has been included on the classpath and is loaded
    * or can be loaded as a resource.
    * @return Future that contains an IllegalStateException or a Unit
    * @throws RuntimeException if the daml archive cannot be found on the classpath
    * @throws AbortedDueToShutdownException if the node is shutting down
    */
  private def loadDamlArchiveResource()(implicit
      traceContext: TraceContext
  ): EitherT[Future, IllegalStateException, Unit] = {
    val bytes =
      withResource(AdminWorkflowServices.adminWorkflowDarInputStream())(ByteString.readFrom)
    handleDamlErrorDuringPackageLoading(
      packageService
        .upload(
          darBytes = bytes,
          fileNameO = Some(AdminWorkflowServices.AdminWorkflowDarResourceName),
          submissionIdO = None,
          vetAllPackages = true,
          synchronizeVetting = false,
        )
        .void
    )
  }

  private def createLedgerClient(
      applicationId: String
  ): LedgerClient = {
    val appId = A.ApplicationId(applicationId)
    val ledgerApiConfig = config.ledgerApi
    LedgerConnection.createLedgerClient(
      appId,
      ledgerApiConfig.clientConfig,
      CommandClientConfiguration.default, // not used by admin workflows
      tracerProvider,
      loggerFactory,
      Some(adminToken.secret),
    )
  }

  private def createService[S <: AdminWorkflowService](
      applicationId: String,
      resubscribeIfPruned: Boolean,
  )(createService: LedgerClient => S): (Future[ResilientLedgerSubscription[?, ?]], S) = {

    val client = createLedgerClient(applicationId)
    val service = createService(client)

    val startupF =
      client.v2.stateService.getActiveContracts(service.filters).map { case (acs, offset) =>
        logger.debug(s"Loading ${acs} $service")
        service.processAcs(acs)
        new ResilientLedgerSubscription(
          subscribeOffset =>
            client.v2.updateService.getUpdatesSource(subscribeOffset, service.filters),
          Flow[GetUpdatesResponse]
            .map(_.update)
            .map {
              case GetUpdatesResponse.Update.Transaction(tx) =>
                service.processTransaction(tx)
              case GetUpdatesResponse.Update.Reassignment(reassignment) =>
                service.processReassignment(reassignment)
              case GetUpdatesResponse.Update.Empty => ()
            },
          subscriptionName = service.getClass.getSimpleName,
          offset,
          ResilientLedgerSubscription.extractOffsetFromGetUpdateResponse,
          timeouts,
          loggerFactory,
          resubscribeIfPruned = resubscribeIfPruned,
        )
      }
    (startupF, service)
  }

}

object AdminWorkflowServices extends AdminWorkflowServicesErrorGroup {
  class AbortedDueToShutdownException
      extends RuntimeException("The request was aborted due to the node shutting down.")

  private val AdminWorkflowDarResourceName: String = "AdminWorkflows.dar"
  private def adminWorkflowDarInputStream(): InputStream = getDarInputStream(
    AdminWorkflowDarResourceName
  )

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

  val AdminWorkflowPackages: Map[PackageId, Ast.Package] =
    DamlPackageLoader
      .getPackagesFromInputStream("AdminWorkflows", adminWorkflowDarInputStream())
      .valueOr(err =>
        throw new IllegalStateException(s"Unable to load admin workflow packages: $err")
      )

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
    final case class Error()(implicit val loggingContext: ErrorLoggingContext)
        extends CantonError.Impl(
          cause =
            "Unable to vet `AdminWorkflows` automatically. Please ensure you vet this package before using one of the admin workflows."
        )

  }
}
