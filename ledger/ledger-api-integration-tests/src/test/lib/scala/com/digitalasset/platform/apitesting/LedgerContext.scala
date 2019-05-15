// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apitesting

import java.time.Duration

import akka.actor.ActorSystem
import akka.pattern
import akka.stream.Materializer
import com.digitalasset.api.util.TimeProvider
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.grpc.adapter.ExecutionSequencerFactory
import com.digitalasset.ledger.api.testing.utils.{MockMessages, Resource}
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc
import com.digitalasset.ledger.api.v1.command_completion_service.CommandCompletionServiceGrpc.CommandCompletionService
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc
import com.digitalasset.ledger.api.v1.command_service.CommandServiceGrpc.CommandService
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc
import com.digitalasset.ledger.api.v1.command_submission_service.CommandSubmissionServiceGrpc.CommandSubmissionService
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc
import com.digitalasset.ledger.api.v1.ledger_configuration_service.LedgerConfigurationServiceGrpc.LedgerConfigurationService
import com.digitalasset.ledger.api.v1.ledger_identity_service.LedgerIdentityServiceGrpc.LedgerIdentityService
import com.digitalasset.ledger.api.v1.ledger_identity_service.{
  GetLedgerIdentityRequest,
  LedgerIdentityServiceGrpc
}
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc
import com.digitalasset.ledger.api.v1.package_service.PackageServiceGrpc.PackageService
import com.digitalasset.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
import com.digitalasset.ledger.api.v1.testing.reset_service.ResetServiceGrpc.ResetService
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc
import com.digitalasset.ledger.api.v1.testing.time_service.TimeServiceGrpc.TimeService
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc
import com.digitalasset.ledger.api.v1.transaction_service.TransactionServiceGrpc.TransactionService
import com.digitalasset.ledger.client.configuration.CommandClientConfiguration
import com.digitalasset.ledger.client.services.acs.ActiveContractSetClient
import com.digitalasset.ledger.client.services.commands.CommandClient
import com.digitalasset.ledger.client.services.pkg.PackageClient
import com.digitalasset.ledger.client.services.testing.time.StaticTime
import com.digitalasset.ledger.client.services.transactions.TransactionClient
import com.digitalasset.platform.common.LedgerIdMode
import com.digitalasset.platform.testing.ResourceExtensions
import io.grpc.{Channel, StatusRuntimeException}
import io.grpc.reflection.v1alpha.ServerReflectionGrpc
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.Success
import scala.concurrent.duration._

trait LedgerContext {
  import LedgerContext._

  implicit protected def esf: ExecutionSequencerFactory
  private val logger = LoggerFactory.getLogger(this.getClass)

  def ledgerId: Ref.LedgerId
  def packageIds: Iterable[Ref.PackageId]
  def ledgerIdentityService: LedgerIdentityService
  def ledgerConfigurationService: LedgerConfigurationService
  def packageService: PackageService
  def commandSubmissionService: CommandSubmissionService
  def commandCompletionService: CommandCompletionService
  def commandService: CommandService
  def transactionService: TransactionService
  def timeService: TimeService
  def acsService: ActiveContractsService
  def transactionClient: TransactionClient
  def packageClient: PackageClient
  def acsClient: ActiveContractSetClient
  def reflectionService: ServerReflectionGrpc.ServerReflectionStub
  def resetService: ResetService

  /**
    *  Reset the ledger server and wait for it to start again.
    *  @return the new ledger id
    * */
  final def reset()(implicit system: ActorSystem, mat: Materializer): Future[String] = {
    implicit val ec: ExecutionContext = mat.executionContext
    def waitForNewLedger(retries: Int): Future[String] =
      if (retries <= 0)
        Future.failed(new RuntimeException("waitForNewLedger: out of retries"))
      else {
        ledgerIdentityService
          .getLedgerIdentity(GetLedgerIdentityRequest())
          .flatMap { resp =>
            // TODO(JM): Could check that ledger-id has changed. However,
            // the tests use a static ledger-id...
            Future.successful(resp.ledgerId)
          }
          .recoverWith {
            case _: StatusRuntimeException =>
              logger.debug(
                "waitForNewLedger: retrying identity request in 1 second. {} retries remain",
                retries - 1)
              pattern.after(1.seconds, system.scheduler)(waitForNewLedger(retries - 1))
            case t: Throwable =>
              logger.warn("waitForNewLedger: failed to reconnect!")
              throw t
          }
      }
    for {
      _ <- resetService.reset(ResetRequest(ledgerId))
      newLedgerId <- waitForNewLedger(10)
    } yield newLedgerId
  }

  /**
    * Get a new client with no time provider and the given ledger ID.
    */
  final def commandClientWithoutTime(
      ledgerId: String = this.ledgerId,
      applicationId: String = MockMessages.applicationId,
      configuration: CommandClientConfiguration = defaultCommandClientConfiguration)
    : CommandClient =
    new CommandClient(
      commandSubmissionService,
      commandCompletionService,
      ledgerId,
      applicationId,
      configuration,
      None
    )

  /**
    * Get a new client with time provided by the context's time service.
    * <p/>
    * Note that the time service will fail fast on a misconfigured ledger. For ledger misconfiguration tests,
    * use [[commandClientWithoutTime()]].
    */
  final def commandClient(
      ledgerId: String = this.ledgerId,
      applicationId: String = MockMessages.applicationId,
      configuration: CommandClientConfiguration = defaultCommandClientConfiguration)(
      implicit mat: Materializer): Future[CommandClient] =
    StaticTime
      .updatedVia(timeService, ledgerId)
      .transform { t =>
        // FIXME: we shouldn't silently default to local UTC on any exception. That is bad practice in several ways.
        Success(
          commandClientWithoutTime(ledgerId, applicationId, configuration)
            .withTimeProvider(Some(t.fold(_ => TimeProvider.UTC, identity))))
      }(mat.executionContext)
}

object LedgerContext {
  val defaultCommandClientConfiguration =
    CommandClientConfiguration(
      maxCommandsInFlight = 1,
      maxParallelSubmissions = 1,
      overrideTtl = true,
      ttl = Duration.ofSeconds(30))

  final case class SingleChannelContext(
      channel: Channel,
      configuredLedgerId: LedgerIdMode,
      packageIds: Iterable[PackageId])(
      implicit override protected val esf: ExecutionSequencerFactory)
      extends LedgerContext {

    require(esf != null, "ExecutionSequencerFactory must not be null.")

    def ledgerId: Ref.LedgerId =
      configuredLedgerId match {
        case LedgerIdMode.Static(id) =>
          id
        case LedgerIdMode.Dynamic() =>
          Ref.LedgerName.assertFromString(
            LedgerIdentityServiceGrpc
              .blockingStub(channel)
              .getLedgerIdentity(GetLedgerIdentityRequest())
              .ledgerId
          )
      }

    override def ledgerIdentityService: LedgerIdentityService =
      LedgerIdentityServiceGrpc.stub(channel)
    override def ledgerConfigurationService: LedgerConfigurationService =
      LedgerConfigurationServiceGrpc.stub(channel)
    override def packageService: PackageService = PackageServiceGrpc.stub(channel)
    override def commandSubmissionService: CommandSubmissionService =
      CommandSubmissionServiceGrpc.stub(channel)
    override def commandCompletionService: CommandCompletionService =
      CommandCompletionServiceGrpc.stub(channel)
    override def commandService: CommandService = CommandServiceGrpc.stub(channel)
    override def transactionService: TransactionService = TransactionServiceGrpc.stub(channel)
    override def timeService: TimeService = TimeServiceGrpc.stub(channel)

    override def acsService: ActiveContractsService =
      ActiveContractsServiceGrpc.stub(channel)

    override def transactionClient: TransactionClient =
      new TransactionClient(ledgerId, transactionService)
    override def packageClient: PackageClient = new PackageClient(ledgerId, packageService)

    override def acsClient: ActiveContractSetClient =
      new ActiveContractSetClient(ledgerId, acsService)

    override def resetService: ResetService = ResetServiceGrpc.stub(channel)

    override def reflectionService: ServerReflectionGrpc.ServerReflectionStub =
      ServerReflectionGrpc.newStub(channel)
  }

  object SingleChannelContext {

    @SuppressWarnings(Array("org.wartremover.warts.ExplicitImplicitTypes"))
    implicit def withResource(resource: Resource[SingleChannelContext]) =
      ResourceExtensions.MultiResource(resource)
  }
}
