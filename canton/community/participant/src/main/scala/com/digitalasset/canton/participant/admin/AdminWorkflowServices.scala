// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.daml.error.*
import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.refinements.ApiTypes as A
import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.transaction.Transaction
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.language.Ast
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.AdminWorkflowServicesErrorGroup
import com.digitalasset.canton.error.{CantonError, DecodedRpcStatus}
import com.digitalasset.canton.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.canton.ledger.error.LedgerApiErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.AdminWorkflowServices.AbortedDueToShutdownException
import com.digitalasset.canton.participant.config.LocalParticipantConfig
import com.digitalasset.canton.participant.ledger.api.CantonAdminToken
import com.digitalasset.canton.participant.ledger.api.client.{LedgerConnection, LedgerSubscription}
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.SyncServiceInjectionError.PassiveReplica
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.TopologyManagerError.NoAppropriateSigningKeyInStore
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.tracing.{NoTracing, Spanning, TraceContext, TracerProvider}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ResourceUtil.withResource
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.{DamlPackageLoader, EitherTUtil, FutureUtil, retry}
import com.google.protobuf.ByteString
import com.google.rpc.status.Status
import io.grpc.StatusRuntimeException
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.actor.ActorSystem
import org.slf4j.event.Level

import java.io.InputStream
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success, Try}

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
    scheduledExecutorService: ScheduledExecutorService,
    actorSystem: ActorSystem,
    tracer: Tracer,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends FlagCloseableAsync
    with NamedLogging
    with Spanning
    with NoTracing {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  private val adminParty = adminPartyId.toParty

  if (syncService.isActive() && parameters.adminWorkflow.autoloadDar) {
    withNewTraceContext { implicit traceContext =>
      logger.debug("Loading admin workflows DAR")
      // load the admin workflows daml archive before moving forward
      // We use the pre-packaged dar from the resources/dar folder instead of the compiled one.
      loadDamlArchiveUnlessRegistered()
    }
  }

  val (pingSubscription, ping) = createService("admin-ping") { connection =>
    new PingService(
      connection,
      adminPartyId,
      parameters.adminWorkflow.bongTestMaxLevel,
      timeouts,
      syncService.maxDeduplicationDuration, // Set the deduplication duration for Ping command to the maximum allowed.
      syncService.isActive(),
      Some(syncService),
      futureSupervisor,
      loggerFactory,
      clock,
    )
  }

  protected def closeAsync(): Seq[AsyncOrSyncCloseable] = Seq[AsyncOrSyncCloseable](
    SyncCloseable(
      "services",
      Lifecycle.close(
        pingSubscription,
        ping,
      )(logger),
    )
  )

  private def checkPackagesStatus(
      pkgs: Map[PackageId, Ast.Package],
      lc: LedgerConnection,
  ): Future[Boolean] =
    for {
      pkgRes <- pkgs.keys.toList.parTraverse(lc.getPackageStatus)
    } yield pkgRes.forall(pkgResponse => pkgResponse.packageStatus.isRegistered)

  private def handleDamlErrorDuringPackageLoading(
      res: EitherT[FutureUnlessShutdown, DamlError, Unit]
  ): EitherT[Future, IllegalStateException, Unit] = EitherT {
    EitherTUtil
      .leftSubflatMap(res) {
        case CantonPackageServiceError.IdentityManagerParentError(
              ParticipantTopologyManagerError.IdentityManagerParentError(
                NoAppropriateSigningKeyInStore.Failure(_)
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
    withResource({
      val (_, conn) = createConnection("admin-checkStatus", "admin-checkStatus")
      conn
    }) { conn =>
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
        .appendDarFromByteString(
          bytes,
          AdminWorkflowServices.AdminWorkflowDarResourceName,
          vetAllPackages = true,
          synchronizeVetting = false,
        )
        .void
    )
  }

  /** The admin workflow services are connected directly to a participant replica so we do not need to retry if the replica is passive. */
  private def noRetryOnPassiveReplica: PartialFunction[Status, Boolean] = {
    case status: Status
        if DecodedRpcStatus.fromScalaStatus(status).exists(s => s.id == PassiveReplica.id) =>
      false
  }

  private def createConnection(
      applicationId: String,
      workflowId: String,
  ): (LedgerOffset, LedgerConnection) = {
    val appId = A.ApplicationId(applicationId)
    val ledgerApiConfig = config.ledgerApi
    val connection = LedgerConnection(
      ledgerApiConfig.clientConfig,
      appId,
      parameters.adminWorkflow.retries,
      adminParty,
      A.WorkflowId(workflowId),
      CommandClientConfiguration.default.copy(
        defaultDeduplicationTime = parameters.adminWorkflow.submissionTimeout.asJava
      ),
      Some(adminToken.secret),
      parameters.processingTimeouts,
      loggerFactory,
      tracerProvider,
      futureSupervisor,
      noRetryOnPassiveReplica,
    )
    (
      parameters.processingTimeouts.unbounded.await("querying the ledger end")(
        connection.ledgerEnd
      ),
      connection,
    )
  }

  private def createService[S <: AdminWorkflowService](
      applicationId: String
  )(createService: LedgerConnection => S): (ResilientTransactionsSubscription, S) = {
    val (offset, connection) = createConnection(applicationId, applicationId)
    val service = createService(connection)
    logger.debug(s"Created connection for service $service")

    val subscription = new ResilientTransactionsSubscription(
      connection = connection,
      serviceName = service.getClass.getSimpleName,
      initialOffset = offset,
      subscriptionName = applicationId,
      timeouts = timeouts,
      loggerFactory = loggerFactory,
    )(service.processTransaction(_)(_: TraceContext))

    (subscription, service)
  }
}

object AdminWorkflowServices extends AdminWorkflowServicesErrorGroup {
  class AbortedDueToShutdownException
      extends RuntimeException("The request was aborted due to the node shutting down.")

  private val AdminWorkflowDarResourceName: String = "AdminWorkflowsWithVacuuming.dar"
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

/** Resilient ledger transaction listener, which keeps continuously
  * re-subscribing (on failure) to the Ledger API transaction stream
  * and applies the received transactions to the `processTransaction` function.
  *
  * `processTransaction` must not throw. If it does, it must be idempotent
  * (i.e. allow re-processing the same transaction twice).
  */
private[admin] class ResilientTransactionsSubscription(
    connection: LedgerConnection,
    serviceName: String,
    initialOffset: LedgerOffset,
    subscriptionName: String,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(processTransaction: (Transaction, TraceContext) => Unit)(implicit
    ec: ExecutionContextExecutor,
    tracer: Tracer,
) extends FlagCloseableAsync
    with NamedLogging
    with Spanning
    with NoTracing {
  private val offsetRef = new AtomicReference[LedgerOffset](initialOffset)
  private implicit val policyRetry: retry.Success[Any] = retry.Success.always

  private val ledgerSubscriptionRef = new AtomicReference[Option[LedgerSubscription]](None)

  private[admin] val subscriptionF = retry
    .Backoff(
      logger = logger,
      flagCloseable = this,
      maxRetries = retry.Forever,
      initialDelay = 1.second,
      maxDelay = 5.seconds,
      operationName = s"restartable-$serviceName-$subscriptionName",
    )
    .apply(resilientSubscription(), AllExnRetryable)

  runOnShutdown_(new RunOnShutdown {
    override def name: String = s"$serviceName-$subscriptionName-shutdown"
    override def done: Boolean = {
      // Use isClosing to avoid task eviction at the beginning (see runOnShutdown)
      isClosing && ledgerSubscriptionRef.get().forall(_.completed.isCompleted)
    }

    override def run(): Unit =
      ledgerSubscriptionRef.getAndSet(None).foreach(Lifecycle.close(_)(logger))
  })

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    val name = s"wait-for-$serviceName-$subscriptionName-completed"
    Seq(
      AsyncCloseable(
        name,
        subscriptionF.recover { error =>
          logger.warn(s"$name finished with an error", error)
          ()
        },
        timeouts.closing.duration,
      )
    )
  }

  private def resilientSubscription(): Future[Unit] =
    FutureUtil.logOnFailure(
      future = {
        val newSubscription = createLedgerSubscription()
        ledgerSubscriptionRef.set(Some(newSubscription))
        // Check closing again to ensure closing of the new subscription
        // in case the shutdown happened before the after the first closing check
        // but before the previous reference update
        if (isClosing) {
          ledgerSubscriptionRef.getAndSet(None).foreach(closeSubscription)
          newSubscription.completed.map(_ => ())
        } else {
          newSubscription.completed
            .map(_ => ())
            .thereafter { result =>
              // This closing races with the one from runOnShutdown so use getAndSet
              // to ensure calling close only once on a subscription
              ledgerSubscriptionRef.getAndSet(None).foreach(closeSubscription)
              result.failed.foreach(handlePrunedDataAccessed)
            }
        }
      },
      failureMessage = s"${subject(capitalized = true)} failed with an error",
      level = Level.WARN,
    )

  private def closeSubscription(ledgerSubscription: LedgerSubscription): Unit =
    Try(ledgerSubscription.close()) match {
      case Failure(exception) =>
        logger.warn(
          s"${subject(capitalized = true)} [$ledgerSubscription] failed to close successfully",
          exception,
        )
      case Success(_) =>
        logger.info(
          s"Successfully closed ${subject(capitalized = false)} [$ledgerSubscription] closed successfully"
        )
    }

  private def subject(capitalized: Boolean) =
    s"${if (capitalized) "Ledger" else "ledger"} subscription $subscriptionName for $serviceName"

  private def createLedgerSubscription(): LedgerSubscription = {
    val currentOffset = offsetRef.get()
    logger.debug(
      s"Creating new transactions ${subject(capitalized = false)} starting at offset $currentOffset"
    )
    connection.subscribe(subscriptionName, currentOffset)(tx =>
      // TODO(i14763): This Span has no effect now
      withSpan(s"$subscriptionName.processTransaction") { traceContext => _ =>
        processTransaction(tx, traceContext)
        offsetRef.set(LedgerOffset(LedgerOffset.Value.Absolute(tx.offset)))
      }
    )
  }

  private def handlePrunedDataAccessed: Throwable => Unit = {
    case sre: StatusRuntimeException =>
      DecodedRpcStatus
        .fromStatusRuntimeException(sre)
        .filter(_.id == RequestValidationErrors.ParticipantPrunedDataAccessed.id)
        .flatMap(_.context.get(LedgerApiErrors.EarliestOffsetMetadataKey))
        .foreach { earliestOffset =>
          logger.warn(
            s"Setting the ${subject(capitalized = false)} offset to a later offset [$earliestOffset] due to pruning. Some commands might timeout or events might become stale."
          )
          offsetRef.set(LedgerOffset(LedgerOffset.Value.Absolute(earliestOffset)))
        }
    case _ =>
      // Do nothing for other errors
      ()
  }
}
