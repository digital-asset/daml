// Copyright (c) 2019 The DAML Authors. All rights reserved.
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
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.testing.utils.MockMessages
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc
import com.digitalasset.ledger.api.v1.active_contracts_service.ActiveContractsServiceGrpc.ActiveContractsService
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc
import com.digitalasset.ledger.api.v1.admin.package_management_service.PackageManagementServiceGrpc.PackageManagementService
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc
import com.digitalasset.ledger.api.v1.admin.party_management_service.PartyManagementServiceGrpc.PartyManagementService
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
import com.digitalasset.ledger.api.v1.testing.reset_service.ResetServiceGrpc.ResetService
import com.digitalasset.ledger.api.v1.testing.reset_service.{ResetRequest, ResetServiceGrpc}
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
import com.digitalasset.platform.common.util.DirectExecutionContext
import io.grpc.reflection.v1alpha.ServerReflectionGrpc
import io.grpc.{Channel, StatusRuntimeException}
import org.slf4j.LoggerFactory
import scalaz.syntax.tag._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

trait LedgerContext {
  import LedgerContext._

  implicit protected def esf: ExecutionSequencerFactory

  /**
    * Convenience function to either use statically configured ledger id or fetch it from the server under test.
    * @return
    */
  def ledgerId: domain.LedgerId

  /**
    *  Reset the ledger server and return a new LedgerContext appropriate to the new state of the ledger API server under test.
    *  @return the new LedgerContext
    * */
  def reset()(implicit system: ActorSystem, mat: Materializer): Future[LedgerContext]

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
  def partyManagementService: PartyManagementService
  def packageManagementService: PackageManagementService

  /**
    * resetService is protected on purpose, to disallow moving an instance of LedgerContext into an invalid state,
    * where [[ledgerId]] - if cached by implementation - becomes out of sync with the backend.
    * @return
    */
  protected def resetService: ResetService

  /**
    * Get a new client with no time provider and the given ledger ID.
    */
  final def commandClientWithoutTime(
      ledgerId: domain.LedgerId = this.ledgerId,
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
      ledgerId: domain.LedgerId = this.ledgerId,
      applicationId: String = MockMessages.applicationId,
      configuration: CommandClientConfiguration = defaultCommandClientConfiguration)(
      implicit mat: Materializer): Future[CommandClient] =
    timeProvider(ledgerId)
      .map(
        tp =>
          commandClientWithoutTime(ledgerId, applicationId, configuration)
            .withTimeProvider(Some(tp)))(DirectExecutionContext)

  final def timeProvider(ledgerId: domain.LedgerId = this.ledgerId)(
      implicit mat: Materializer): Future[TimeProvider] = {
    StaticTime
      .updatedVia(timeService, ledgerId.unwrap)
      .recover { case NonFatal(_) => TimeProvider.UTC }(DirectExecutionContext)
  }
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

    private val logger = LoggerFactory.getLogger(this.getClass)

    val ledgerId: domain.LedgerId =
      configuredLedgerId match {
        case LedgerIdMode.Static(id) => id
        case LedgerIdMode.Dynamic() =>
          domain.LedgerId(
            LedgerIdentityServiceGrpc
              .blockingStub(channel)
              .getLedgerIdentity(GetLedgerIdentityRequest())
              .ledgerId)
      }

    final def reset()(implicit system: ActorSystem, mat: Materializer): Future[LedgerContext] = {
      implicit val ec: ExecutionContext = mat.executionContext
      def waitForNewLedger(retries: Int): Future[domain.LedgerId] =
        if (retries <= 0)
          Future.failed(new RuntimeException("waitForNewLedger: out of retries"))
        else {
          ledgerIdentityService
            .getLedgerIdentity(GetLedgerIdentityRequest())
            .flatMap { resp =>
              // TODO: compare with current Ledger ID and retry when not changed
              Future.successful(domain.LedgerId(resp.ledgerId))
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
        _ <- resetService.reset(ResetRequest(ledgerId.unwrap))
        newLedgerId <- waitForNewLedger(10)
      } yield SingleChannelContext(channel, LedgerIdMode.Static(newLedgerId), packageIds)
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
    override def packageClient: PackageClient =
      new PackageClient(ledgerId, packageService)

    override def acsClient: ActiveContractSetClient =
      new ActiveContractSetClient(ledgerId, acsService)

    override def resetService: ResetService = ResetServiceGrpc.stub(channel)

    override def reflectionService: ServerReflectionGrpc.ServerReflectionStub =
      ServerReflectionGrpc.newStub(channel)

    override def partyManagementService: PartyManagementService =
      PartyManagementServiceGrpc.stub(channel)

    override def packageManagementService: PackageManagementService =
      PackageManagementServiceGrpc.stub(channel)
  }

}
